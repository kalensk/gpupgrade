package services

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/greenplum-db/gpupgrade/helpers"
	"github.com/greenplum-db/gpupgrade/hub/configutils"
	pb "github.com/greenplum-db/gpupgrade/idl"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/reflection"
)

var DialTimeout = 30 * time.Second

type dialer func(ctx context.Context, target string, opts ...grpc.DialOption) (*grpc.ClientConn, error)

type reader interface {
	GetHostnames() ([]string, error)
	GetSegmentConfiguration() configutils.SegmentConfiguration
	OfOldClusterConfig(baseDir string)
	OfNewClusterConfig(baseDir string)
	GetMasterDataDir() string
	GetPortForSegment(segmentDbid int) int
	GetMaxSegmentPort() int
}
type pairOperator interface {
	StopEverything(string)
	GetPortsAndDataDirForReconfiguration() (int, int, string)
	EitherPostmasterRunning() bool
}

type RemoteExecutor interface {
	VerifySoftware(hosts []string)
	Start(hosts []string)
}

type Hub struct {
	conf *HubConfig

	agentConns     []*Connection
	clusterPair    pairOperator
	configreader   reader
	grpcDialer     dialer
	commandExecer  helpers.CommandExecer
	remoteExecutor RemoteExecutor

	mu      sync.Mutex
	server  *grpc.Server
	lis     net.Listener
	stopped chan struct{}
}

type Connection struct {
	PbAgentClient pb.AgentClient
	Conn          *grpc.ClientConn
	Hostname      string
	CancelContext func()
}

type HubConfig struct {
	CliToHubPort   int
	HubToAgentPort int
	StateDir       string
	LogDir         string
}

func NewHub(pair pairOperator, configReader reader, grpcDialer dialer, execer helpers.CommandExecer, conf *HubConfig, executor RemoteExecutor) *Hub {
	// refactor opportunity -- don't use this pattern,
	// use different types or separate functions for old/new or set the config path at reader initialization time
	configReader.OfOldClusterConfig(conf.StateDir)

	h := &Hub{
		stopped:       make(chan struct{}, 1),
		conf:          conf,
		clusterPair:   pair,
		configreader:  configReader,
		grpcDialer:    grpcDialer,
		commandExecer: execer,
		remoteExecutor: executor,
	}

	return h
}

func (h *Hub) Start() {
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(h.conf.CliToHubPort))
	if err != nil {
		gplog.Fatal(err, "failed to listen")
	}

	server := grpc.NewServer()
	h.mu.Lock()
	h.server = server
	h.lis = lis
	h.mu.Unlock()

	pb.RegisterCliToHubServer(server, h)
	reflection.Register(server)

	err = server.Serve(lis)
	if err != nil {
		gplog.Fatal(err, "failed to serve", err)
	}

	h.stopped <- struct{}{}
}

func (h *Hub) Stop() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.server != nil {
		h.closeConns()
		h.server.Stop()
		<-h.stopped
	}
}

func (h *Hub) AgentConns() ([]*Connection, error) {
	if h.agentConns != nil {
		err := h.ensureConnsAreReady()
		if err != nil {
			gplog.Error("ensureConnsAreReady failed: ", err)
			return nil, err
		}

		return h.agentConns, nil
	}

	hostnames, err := h.configreader.GetHostnames()
	if err != nil {
		gplog.Error("GetHostnames failed: ", err)
		return nil, err
	}

	for _, host := range hostnames {
		ctx, cancelFunc := context.WithTimeout(context.Background(), DialTimeout)
		// grpc.WithBlock() is potentially slowing down the tests. Leaving it in to keep tests green.
		conn, err := h.grpcDialer(ctx, host+":"+strconv.Itoa(h.conf.HubToAgentPort), grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			gplog.Error("grpcDialer failed: ", err)
			cancelFunc()
			return nil, err
		}
		h.agentConns = append(h.agentConns, &Connection{
			PbAgentClient: pb.NewAgentClient(conn),
			Conn:          conn,
			Hostname:      host,
			CancelContext: cancelFunc,
		})
	}

	return h.agentConns, nil
}

func (h *Hub) ensureConnsAreReady() error {
	var hostnames []string
	for i := 0; i < 3; i++ {
		ready := 0
		for _, conn := range h.agentConns {
			if conn.Conn.GetState() == connectivity.Ready {
				ready++
			} else {
				hostnames = append(hostnames, conn.Hostname)
			}
		}

		if ready == len(h.agentConns) {
			return nil
		}

		time.Sleep(500 * time.Millisecond)
	}

	return fmt.Errorf("the connections to the following hosts were not ready: %s", strings.Join(hostnames, ","))
}

func (h *Hub) closeConns() {
	for _, conn := range h.agentConns {
		defer conn.CancelContext()
		err := conn.Conn.Close()
		if err != nil {
			gplog.Info(fmt.Sprintf("Error closing hub to agent connection. host: %s, err: %s", conn.Hostname, err.Error()))
		}
	}
}

func (h *Hub) segmentsByHost() map[string]configutils.SegmentConfiguration {
	segments := h.configreader.GetSegmentConfiguration()

	segmentsByHost := make(map[string]configutils.SegmentConfiguration)
	for _, segment := range segments {
		host := segment.Hostname
		if len(segmentsByHost[host]) == 0 {
			segmentsByHost[host] = []configutils.Segment{segment}
		} else {
			segmentsByHost[host] = append(segmentsByHost[host], segment)
		}
	}

	return segmentsByHost
}

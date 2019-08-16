package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"syscall"

	"github.com/greenplum-db/gpupgrade/utils/daemon"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var listener net.Listener

func main() {
	var shouldDaemonize bool

	var RootCmd = &cobra.Command{
		Use:   "gpupgrade_watchdog ",
		Short: "Start the watchdog",
		Long:  `Start the watchdog`,
		Args:  cobra.MaximumNArgs(0), //no positional args allowed
		RunE: func(cmd *cobra.Command, args []string) error {
			/*
				stdOut := os.Stdout
				stdErr := os.Stderr

				if shouldDaemonize {
					var err error
					stdOut, err = os.Create("watchdog.out")
					if err != nil {
						log.Fatalf("failed to open watchdog.out due to: %v", err)
					}

					stdErr, err = os.Create("watchdog.err")
					if err != nil {
						log.Fatalf("failed to open watchdog.err due to: %v", err)
					}

					daemon.Daemonize()
				}
			*/

			//startWatchdog(stdOut, stdErr)
			return startServer()
		},
	}

	RootCmd.AddCommand(&cobra.Command{
		Use:   "attach",
		Short: "Attach to a watchdog",
		Long:  "Attach to a watchdog",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			sockPath := args[0]

			const clientSockPath = "/tmp/client.sock"
			uds, err := net.ListenPacket("unixgram", clientSockPath)
			if err != nil {
				return errors.Wrapf(err, "error listening on %s", clientSockPath)
			}
			defer uds.Close()

			watchdogAddr, err := net.ResolveUnixAddr("unixgram", sockPath)
			if err != nil {
				return errors.Wrapf(err, "error resolving %s", sockPath)
			}

			_, _, err = uds.(*net.UnixConn).WriteMsgUnix([]byte("hello"), nil, watchdogAddr)
			if err != nil {
				return errors.Wrap(err, "error writing")
			}

			buf := make([]byte, 64)
			oob := make([]byte, 64)

			_, oobn, flags, _, err := uds.(*net.UnixConn).ReadMsgUnix(buf, oob)
			if err != nil {
				return errors.Wrap(err, "failed to read UDS message")
			}

			log.Printf("buf: %v, oob: %v, flags: %v\n", buf, oob, flags)

			scms, err := syscall.ParseSocketControlMessage(oob[:oobn])
			if err != nil {
				return errors.Wrap(err, "failed to parse OOB data")
			}

			if len(scms) != 1 {
				log.Panic("OMG")
			}

			scm := scms[0]
			fds, err := syscall.ParseUnixRights(&scm)
			if err != nil {
				return errors.Wrap(err, "failed to parse file descriptors...")
			}

			for _, fd := range fds {
				file := os.NewFile(uintptr(fd), "my received file")
				defer file.Close()

				for {
					n, err := file.Read(buf)
					if err == io.EOF {
						break
					}
					if err != nil {
						return errors.Wrap(err, "failed to read from file")
					}

					log.Printf("read %v", string(buf[:n]))
				}
			}

			return err
		},
	})

	daemon.MakeDaemonizable(RootCmd, &shouldDaemonize)

	err := RootCmd.Execute()
	if err != nil && err != daemon.ErrSuccessfullyDaemonized {
		log.Fatalf("%v", err)
	}
}

func startServer() error {
	const sockPath = "/tmp/watchdog.sock"
	defer os.Remove(sockPath)

	/*
		listener, err := net.ListenUnix("unix", sockAddr)
		if err != nil {
			return errors.Wrap(err, "listen error")
		}
		defer listener.Close()

		conn, err := listener.AcceptUnix()
		if err != nil {
			return errors.Wrap(err, "accept error")
		}
	*/

	conn, err := net.ListenPacket("unixgram", sockPath)
	if err != nil {
		return errors.Wrap(err, "failed to dial")
	}
	defer conn.Close()

	buf := make([]byte, 64)

	uds := conn.(*net.UnixConn)
	_, remote, err := uds.ReadFromUnix(buf)
	if err != nil {
		return errors.Wrap(err, "error reading from client connection")
	}

	log.Printf("got %v from %v", buf, remote)

	reader, writer, err := os.Pipe()
	if err != nil {
		return errors.Wrap(err, "failed to create pipe")
	}
	defer writer.Close()

	_, _, err = uds.WriteMsgUnix([]byte("hello"), syscall.UnixRights(int(reader.Fd())), remote)
	if err != nil {
		return errors.Wrap(err, "failed to write to new UDS connection")
	}

	stderr, err := os.Create("watchdog.err")
	if err != nil {
		return errors.Wrap(err, "failed to create stderr spill file")
	}

	startWatchdog(writer, stderr)

	return nil
}

func startWatchdog(stdOut io.Writer, stdErr io.Writer) {

	cmd := exec.Command("mock_pg_upgrade")

	cmd.Stdout = stdOut
	cmd.Stderr = stdErr
	err := cmd.Run()
	if err != nil {
		fmt.Fprintf(cmd.Stderr, "ERROR: watchdog failed to run due to: +%v", err.Error())
	}
}

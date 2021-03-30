// Copyright (c) 2017-2021 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package hub

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"sync"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/pkg/errors"

	"github.com/greenplum-db/gpupgrade/greenplum"
	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/step"
	"github.com/greenplum-db/gpupgrade/upgrade"
	"github.com/greenplum-db/gpupgrade/utils/errorlist"
	"github.com/greenplum-db/gpupgrade/utils/rsync"
)

var RecoversegCmd = exec.Command

var Options = []string{"--archive", "--compress", "--stats"}

var Excludes = []string{
	"pg_hba.conf", "postmaster.opts", "postgresql.auto.conf", "internal.auto.conf",
	"gp_dbid", "postgresql.conf", "backup_label.old", "postmaster.pid", "recovery.conf",
}

func RsyncMasterAndPrimaries(stream step.OutStreams, agentConns []*Connection, source *greenplum.Cluster) error {
	if !source.HasAllMirrorsAndStandby() {
		return errors.New("Source cluster does not have mirrors and/or standby. Cannot restore source cluster. Please contact support.")
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2)

	wg.Add(1)
	go func() {
		defer wg.Done()
		errs <- RsyncMaster(stream, source.Standby(), source.Master())
	}()

	errs <- RsyncPrimaries(agentConns, source)

	wg.Wait()
	close(errs)

	var err error
	for e := range errs {
		err = errorlist.Append(err, e)
	}

	return err
}

func RsyncMasterAndPrimariesTablespaces(stream step.OutStreams, agentConns []*Connection, source *greenplum.Cluster, tablespaces greenplum.Tablespaces) error {
	if !source.HasAllMirrorsAndStandby() {
		return ErrMissingMirrorsAndStandby
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2)

	wg.Add(1)
	go func() {
		defer wg.Done()
		errs <- RsyncMasterTablespaces(stream, source.StandbyHostname(), tablespaces[source.Master().DbID], tablespaces[source.Standby().DbID])
	}()

	errs <- RsyncPrimariesTablespaces(agentConns, source, tablespaces)

	wg.Wait()
	close(errs)

	var err error
	for e := range errs {
		err = errorlist.Append(err, e)
	}

	return err
}

// TODO: Consider moving to a common location since it is used by both restore and finalize.
func Recoverseg(stream step.OutStreams, cluster *greenplum.Cluster, useHbaHostnames bool) error {
	hbaHostnames := ""
	if useHbaHostnames {
		hbaHostnames = "--hba-hostnames"
	}

	script := fmt.Sprintf("source %[1]s/greenplum_path.sh && MASTER_DATA_DIRECTORY=%[2]s PGPORT=%[3]d %[1]s/bin/gprecoverseg -a %[4]s",
		cluster.GPHome, cluster.MasterDataDir(), cluster.MasterPort(), hbaHostnames)
	cmd := RecoversegCmd("bash", "-c", script)

	cmd.Stdout = stream.Stdout()
	cmd.Stderr = stream.Stderr()

	gplog.Info("running command: %q", cmd)
	return cmd.Run()
}

func RsyncMaster(stream step.OutStreams, standby greenplum.SegConfig, master greenplum.SegConfig) error {
	opts := []rsync.Option{
		rsync.WithSources(standby.DataDir + string(os.PathSeparator)),
		rsync.WithSourceHost(standby.Hostname),
		rsync.WithDestination(master.DataDir),
		rsync.WithOptions(Options...),
		rsync.WithExcludedFiles(Excludes...),
		rsync.WithStream(stream),
	}

	return rsync.Rsync(opts...)
}

func RsyncMasterTablespaces(stream step.OutStreams, standbyHostname string, masterTablespaces greenplum.SegmentTablespaces, standbyTablespaces greenplum.SegmentTablespaces) error {
	for oid, masterTsInfo := range masterTablespaces {
		if !masterTsInfo.IsUserDefined() {
			continue
		}

		opts := []rsync.Option{
			rsync.WithSourceHost(standbyHostname),
			rsync.WithSources(standbyTablespaces[oid].Location + string(os.PathSeparator)),
			rsync.WithDestination(masterTsInfo.Location),
			rsync.WithOptions(Options...),
			rsync.WithStream(stream),
		}

		err := rsync.Rsync(opts...)
		if err != nil {
			return err
		}
	}

	return nil
}

func RsyncPrimaries(agentConns []*Connection, source *greenplum.Cluster) error {
	request := func(conn *Connection) error {
		mirrors := source.SelectSegments(func(seg *greenplum.SegConfig) bool {
			return seg.IsOnHost(conn.Hostname) && !seg.IsStandby() && seg.IsMirror()
		})

		if len(mirrors) == 0 {
			return nil
		}

		var pairs []*idl.RsyncPair
		for _, mirror := range mirrors {
			pair := &idl.RsyncPair{
				Source:          mirror.DataDir + string(os.PathSeparator),
				DestinationHost: source.Primaries[mirror.ContentID].Hostname,
				Destination:     source.Primaries[mirror.ContentID].DataDir,
			}
			pairs = append(pairs, pair)
		}

		req := &idl.RsyncRequest{
			Options:  Options,
			Excludes: Excludes,
			Pairs:    pairs,
		}

		_, err := conn.AgentClient.RsyncDataDirectories(context.Background(), req)
		return err
	}

	return ExecuteRPC(agentConns, request)
}

func RsyncPrimariesTablespaces(agentConns []*Connection, source *greenplum.Cluster, tablespaces greenplum.Tablespaces) error {
	request := func(conn *Connection) error {
		mirrors := source.SelectSegments(func(seg *greenplum.SegConfig) bool {
			return seg.IsOnHost(conn.Hostname) && !seg.IsStandby() && seg.IsMirror()
		})

		if len(mirrors) == 0 {
			return nil
		}

		var pairs []*idl.RsyncPair
		for _, mirror := range mirrors {
			primary := source.Primaries[mirror.ContentID]

			primaryTablespaces := tablespaces[primary.DbID]
			mirrorTablespaces := tablespaces[mirror.DbID]
			for oid, mirrorTsInfo := range mirrorTablespaces {
				if !mirrorTsInfo.IsUserDefined() {
					continue
				}

				pair := &idl.RsyncPair{
					Source:          mirrorTsInfo.Location + string(os.PathSeparator),
					DestinationHost: primary.Hostname,
					Destination:     primaryTablespaces[oid].Location,
				}
				pairs = append(pairs, pair)
			}
		}

		req := &idl.RsyncRequest{
			Options:  Options,
			Excludes: Excludes,
			Pairs:    pairs,
		}

		_, err := conn.AgentClient.RsyncTablespaceDirectories(context.Background(), req)
		return err
	}

	return ExecuteRPC(agentConns, request)
}

func RestoreMasterAndPrimariesPgControl(streams step.OutStreams, agentConns []*Connection, source *greenplum.Cluster) error {
	var wg sync.WaitGroup
	errs := make(chan error, 2)

	wg.Add(1)
	go func() {
		defer wg.Done()
		errs <- upgrade.RestorePgControl(source.MasterDataDir(), streams)
	}()

	errs <- restorePrimariesPgControl(agentConns, source)

	wg.Wait()
	close(errs)

	var err error
	for e := range errs {
		err = errorlist.Append(err, e)
	}

	return err
}

func restorePrimariesPgControl(agentConns []*Connection, source *greenplum.Cluster) error {
	request := func(conn *Connection) error {
		primaries := source.SelectSegments(func(seg *greenplum.SegConfig) bool {
			return seg.IsOnHost(conn.Hostname) && !seg.IsStandby() && seg.IsPrimary()
		})

		if len(primaries) == 0 {
			return nil
		}

		var dataDirs []string
		for _, primary := range primaries {
			dataDirs = append(dataDirs, primary.DataDir)
		}

		req := &idl.RestorePgControlRequest{
			Datadirs: dataDirs,
		}

		_, err := conn.AgentClient.RestorePrimariesPgControl(context.Background(), req)
		return err
	}

	return ExecuteRPC(agentConns, request)
}

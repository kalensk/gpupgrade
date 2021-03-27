// Copyright (c) 2017-2021 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package hub

import (
	"context"

	"golang.org/x/xerrors"

	"github.com/greenplum-db/gpupgrade/greenplum"
	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/upgrade"
)

var ArchiveSource = upgrade.ArchiveSource

type RenameMap = map[string][]*idl.RenameDirectories

func (s *Server) UpdateDataDirectories() error {
	return UpdateDataDirectories(s.Config, s.agentConns)
}

func UpdateDataDirectories(conf *Config, agentConns []*Connection) error {
	source := conf.Source.MasterDataDir()
	target := conf.TargetInitializeConfig.Master.DataDir
	if err := ArchiveSource(source, target); err != nil {
		return xerrors.Errorf("renaming master data directories: %w", err)
	}

	// TODO: Does this still hold when upgrading mirrors in place?
	// In link mode, remove the source mirror and standby data directories.
	// Otherwise we create a second copy of them for the target cluster which
	// might take too much disk space.
	if conf.UseLinkMode {
		if err := DeleteMirrorAndStandbyDataDirectories(agentConns, conf.Source); err != nil {
			return xerrors.Errorf("removing source cluster standby and mirror segment data directories: %w", err)
		}

		// FIXME: This is deleting the 6X mirror tablespaces and not just the 5X ones...BAD
		// Now that link mode can upgrade mirrors in-place, "only" delete target
		// mirror tablespaces for 5X clusters. This is because the tablespace
		// layout is different between 5X and 6X and is safe to delete when in link
		// mode. However, for 6->7 upgrades do not delete the target mirror
		// tablespaces in link mode as the directory format is the same.
		if conf.Source.Version.Is("5") {
			// TODO: Fix Delete5XSourceTablespacesOnMirrorsAndStandby() to
			//  correctly delete only 5X tablespaces and not 6X or 7X tablespaces.
			if err := Delete5XSourceTablespacesOnMirrorsAndStandby(agentConns, conf.Source, conf.Tablespaces); err != nil {
				return xerrors.Errorf("removing source cluster standby and mirror tablespace data directories: %w", err)
			}
		}
	}

	renameMap := getRenameMap(conf.Source, conf.TargetInitializeConfig, !conf.UseLinkMode)
	if err := RenameSegmentDataDirs(agentConns, renameMap); err != nil {
		return xerrors.Errorf("renaming segment data directories: %w", err)
	}

	return nil
}

// getRenameMap() returns a map of host to cluster data directories to be renamed.
// This includes renaming source to archive, and target to source. In link mode
// the mirrors have been deleted to save disk space, so exclude them from the map.
func getRenameMap(source *greenplum.Cluster, target InitializeConfig, copyMode bool) RenameMap {
	m := make(RenameMap)

	for _, seg := range target.Primaries {
		m[seg.Hostname] = append(m[seg.Hostname], &idl.RenameDirectories{
			Source: source.Primaries[seg.ContentID].DataDir,
			Target: seg.DataDir,
		})
	}

	var targetMirrors greenplum.SegConfigs
	targetMirrors = append(targetMirrors, target.Mirrors...)

	if copyMode {
		// In link mode the standby was just deleted so there is nothing to rename.
		// TODO: Consider renaming then deleting to reduce the branches and
		//  increase code simplicity.
		targetMirrors = append(targetMirrors, target.Standby)
	}

	for _, seg := range targetMirrors {
		m[seg.Hostname] = append(m[seg.Hostname], &idl.RenameDirectories{
			Source: source.Mirrors[seg.ContentID].DataDir,
			Target: seg.DataDir,
		})
	}

	return m
}

// e.g. for source /data/dbfast1/demoDataDir0 becomes /data/dbfast1/demoDataDir0_old
// e.g. for target /data/dbfast1/demoDataDir0_123ABC becomes /data/dbfast1/demoDataDir0
func RenameSegmentDataDirs(agentConns []*Connection, renames RenameMap) error {
	request := func(conn *Connection) error {
		if len(renames[conn.Hostname]) == 0 {
			return nil
		}

		req := &idl.RenameDirectoriesRequest{Dirs: renames[conn.Hostname]}
		_, err := conn.AgentClient.RenameDirectories(context.Background(), req)
		return err
	}

	return ExecuteRPC(agentConns, request)
}

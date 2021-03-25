//  Copyright (c) 2017-2021 VMware, Inc. or its affiliates
//  SPDX-License-Identifier: Apache-2.0

package hub

import (
	"context"
	"path/filepath"

	"github.com/greenplum-db/gpupgrade/greenplum"
	"github.com/greenplum-db/gpupgrade/idl"
)

// TODO: delete upgrade_mirrors.go and rename this file to upgrade_mirrors.go

func (s *Server) upgradeMirrorsInPlace() error {
	request := func(conn *Connection) error {
		targetMirrors := s.Target.SelectSegments(func(seg *greenplum.SegConfig) bool {
			return seg.IsOnHost(conn.Hostname) && !seg.IsStandby() && seg.IsMirror()
		})

		if len(targetMirrors) == 0 {
			return nil
		}

		var pgOpts []*idl.PgOptions
		for _, targetMirror := range targetMirrors {
			sourceMirror := s.Source.Mirrors[targetMirror.ContentID]

			pgOpt := &idl.PgOptions{
				PrimaryHost:                s.Target.Primaries[targetMirror.ContentID].Hostname,
				TargetVersion:              s.Target.Version.SemVer.String(),
				CheckOnly:                  false,
				UseLinkMode:                s.UseLinkMode,
				TablespacesMappingFilePath: s.TablespacesMappingFilePath,
				Tablespaces:                greenplum.GetProtoTablespaceMap(s.Tablespaces, targetMirror.DbID),
				SourceBinDir:               filepath.Join(s.Source.GPHome, "bin"),
				SourceDataDir:              sourceMirror.DataDir,
				SourcePort:                 int32(sourceMirror.Port),
				TargetBinDir:               filepath.Join(s.Target.GPHome, "bin"),
				TargetDataDir:              targetMirror.DataDir,
				TargetPort:                 int32(targetMirror.Port),
				Content:                    int32(targetMirror.ContentID),
				DBID:                       int32(targetMirror.DbID),
			}

			pgOpts = append(pgOpts, pgOpt)
		}

		req := &idl.UpgradeMirrorsRequest{
			PgOptions: pgOpts,
		}

		_, err := conn.AgentClient.UpgradeMirrors(context.Background(), req)
		return err
	}

	return ExecuteRPC(s.agentConns, request)
}

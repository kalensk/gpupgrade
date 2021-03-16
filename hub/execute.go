// Copyright (c) 2017-2021 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package hub

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"golang.org/x/xerrors"

	"github.com/greenplum-db/gpupgrade/db/connURI"
	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/step"
	"github.com/greenplum-db/gpupgrade/utils"
	"github.com/greenplum-db/gpupgrade/utils/errorlist"
)

const executeMasterBackupName = "upgraded-master.bak"

func (s *Server) Execute(request *idl.ExecuteRequest, stream idl.CliToHub_ExecuteServer) (err error) {
	upgradedMasterBackupDir := filepath.Join(s.StateDir, executeMasterBackupName) // TODO: This should be a helper function and under the "backups" directory.

	st, err := step.Begin(s.StateDir, idl.Step_EXECUTE, stream)
	if err != nil {
		return err
	}

	defer func() {
		if ferr := st.Finish(); ferr != nil {
			err = errorlist.Append(err, ferr)
		}

		if err != nil {
			gplog.Error(fmt.Sprintf("execute: %s", err))
		}
	}()

	st.Run(idl.Substep_SHUTDOWN_SOURCE_CLUSTER, func(streams step.OutStreams) error {
		err := s.Source.Stop(streams)

		if err != nil {
			return xerrors.Errorf("failed to stop source cluster: %w", err)
		}

		return nil
	})

	st.Run(idl.Substep_UPGRADE_MASTER, func(streams step.OutStreams) error {
		stateDir := s.StateDir
		return UpgradeMaster(UpgradeMasterArgs{
			Source:      s.Source,
			Target:      s.Target,
			StateDir:    stateDir,
			Stream:      streams,
			CheckOnly:   false,
			UseLinkMode: s.UseLinkMode,
		})
	})

	st.Run(idl.Substep_COPY_MASTER, func(streams step.OutStreams) error {
		err := s.CopyMasterDataDir(streams, upgradedMasterBackupDir)
		if err != nil {
			return err
		}

		// TODO: Create a helper for this path such as GetBackupTablespaceDirForCoordinator()
		err = s.CopyMasterTablespaces(streams, utils.GetTablespaceDir()+string(os.PathSeparator))
		if err != nil {
			return err
		}

		return nil
	})

	st.Run(idl.Substep_UPGRADE_PRIMARIES, func(_ step.OutStreams) error {
		agentConns, err := s.AgentConns()

		if err != nil {
			return xerrors.Errorf("connect to gpupgrade agent: %w", err)
		}

		dataDirPair, err := s.GetDataDirPairs()

		if err != nil {
			return xerrors.Errorf("get source and target primary data directories: %w", err)
		}

		return UpgradePrimaries(UpgradePrimaryArgs{
			CheckOnly:              false,
			MasterBackupDir:        upgradedMasterBackupDir,
			AgentConns:             agentConns,
			DataDirPairMap:         dataDirPair,
			Source:                 s.Source,
			Target:                 s.Target,
			UseLinkMode:            s.UseLinkMode,
			TablespacesMappingFile: s.TablespacesMappingFilePath,
		})
	})

	// In link mode we don't want customers to touch the mirrors, so mark them
	// down before starting the target cluster to prevent the mirror processes
	// from running. This is needed since in link mode we chose to initialize
	// the target cluster with mirrors rather than adding the mirrors "manually".
	if s.UseLinkMode {
		st.RunInternalSubstep(func() error {
			return s.Target.StartMasterOnly(step.DevNullStream)
		})

		st.RunInternalSubstep(func() (err error) {
			options := []connURI.Option{
				connURI.ToTarget(),
				connURI.Port(s.Target.MasterPort()),
				connURI.UtilityMode(),
				connURI.AllowSystemTableMods(),
			}

			uri := s.Connection.URI(options...)
			connection, err := sql.Open("pgx", uri)
			if err != nil {
				return xerrors.Errorf("connecting to master on port %d in utility mode with connection URI '%s': %w", s.Target.MasterPort(), uri, err)
			}

			defer func() {
				closeErr := connection.Close()
				if closeErr != nil {
					closeErr = xerrors.Errorf("closing connection to target master: %w", closeErr)
					err = errorlist.Append(err, closeErr)
				}
			}()

			// To ensure mirrors are properly recovered during finalize set the
			// mode to not synchronized for both the mirrors and primaries.
			_, err = connection.Exec(`UPDATE gp_segment_configuration SET status = 'd', mode = 'n' WHERE preferred_role = 'm' AND content <> '-1';`)
			if err != nil {
				return xerrors.Errorf("set mirror status to down: %w", err)
			}

			_, err = connection.Exec(`UPDATE gp_segment_configuration SET mode = 'n' WHERE preferred_role = 'p' AND content <> '-1';`)
			if err != nil {
				return xerrors.Errorf("set corresponding primary mode to change tracking: %w", err)
			}

			return nil
		})

		st.RunInternalSubstep(func() error {
			return s.Target.StopMasterOnly(step.DevNullStream)
		})
	}

	st.Run(idl.Substep_START_TARGET_CLUSTER, func(streams step.OutStreams) error {
		err := s.Target.Start(streams)

		if err != nil {
			return xerrors.Errorf("failed to start target cluster: %w", err)
		}

		return nil
	})

	message := &idl.Message{Contents: &idl.Message_Response{Response: &idl.Response{Contents: &idl.Response_ExecuteResponse{
		ExecuteResponse: &idl.ExecuteResponse{
			Target: &idl.Cluster{
				Port:                int32(s.Target.MasterPort()),
				MasterDataDirectory: s.Target.MasterDataDir(),
			}},
	}}}}

	if err = stream.Send(message); err != nil {
		return err
	}

	return st.Err()
}

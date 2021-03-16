// Copyright (c) 2017-2021 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package hub

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"golang.org/x/xerrors"

	"github.com/greenplum-db/gpupgrade/db/connURI"
	"github.com/greenplum-db/gpupgrade/greenplum"
	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/step"
	"github.com/greenplum-db/gpupgrade/upgrade"
	"github.com/greenplum-db/gpupgrade/utils/errorlist"
)

func (s *Server) Initialize(in *idl.InitializeRequest, stream idl.CliToHub_InitializeServer) (err error) {
	st, err := step.Begin(s.StateDir, idl.Step_INITIALIZE, stream)
	if err != nil {
		return err
	}

	defer func() {
		if ferr := st.Finish(); ferr != nil {
			err = errorlist.Append(err, ferr)
		}

		if err != nil {
			gplog.Error(fmt.Sprintf("initialize: %s", err))
		}
	}()

	st.RunInternalSubstep(func() error {
		sourceVersion, err := greenplum.LocalVersion(in.SourceGPHome)
		if err != nil {
			return err
		}

		targetVersion, err := greenplum.LocalVersion(in.TargetGPHome)
		if err != nil {
			return err
		}

		conn := connURI.Connection(sourceVersion, targetVersion)
		s.Connection = conn

		return nil
	})

	st.Run(idl.Substep_SAVING_SOURCE_CLUSTER_CONFIG, func(stream step.OutStreams) error {
		options := []connURI.Option{
			connURI.ToSource(),
			connURI.Port(int(in.SourcePort)),
			connURI.UtilityMode(),
		}

		conn, err := sql.Open("pgx", s.Connection.URI(options...))
		if err != nil {
			return err
		}
		defer func() {
			if cerr := conn.Close(); cerr != nil {
				err = errorlist.Append(err, cerr)
			}
		}()

		return FillConfiguration(s.Config, conn, stream, in, s.SaveConfig)
	})

	// we need the cluster information to determine what hosts to check, so we do this check
	// as early as possible after that information is available
	st.RunInternalSubstep(func() error {
		if err := EnsureVersionsMatch(AgentHosts(s.Source), upgrade.NewVersions()); err != nil {
			return err
		}

		return EnsureVersionsMatch(AgentHosts(s.Source), greenplum.NewVersions(s.TargetGPHome))
	})

	st.Run(idl.Substep_START_AGENTS, func(_ step.OutStreams) error {
		_, err := RestartAgents(context.Background(), nil, AgentHosts(s.Source), s.AgentPort, s.StateDir)
		return err
	})

	return st.Err()
}

func (s *Server) InitializeCreateCluster(in *idl.InitializeCreateClusterRequest, stream idl.CliToHub_InitializeCreateClusterServer) (err error) {
	st, err := step.Begin(s.StateDir, idl.Step_INITIALIZE, stream)
	if err != nil {
		return err
	}

	defer func() {
		if ferr := st.Finish(); ferr != nil {
			err = errorlist.Append(err, ferr)
		}

		if err != nil {
			gplog.Error(fmt.Sprintf("initialize: %s", err))
		}
	}()

	st.Run(idl.Substep_GENERATE_TARGET_CONFIG, func(_ step.OutStreams) error {
		return s.GenerateInitsystemConfig()
	})

	st.Run(idl.Substep_INIT_TARGET_CLUSTER, func(stream step.OutStreams) error {
		err := s.RemoveTargetCluster(stream)
		if err != nil {
			return err
		}

		err = s.CreateTargetCluster(stream)
		if err != nil {
			return err
		}

		// Persist target catalog version which is needed to revert tablespaces.
		// We do this right after target cluster creation since during revert the
		// state of the cluster is unknown.
		version, err := GetCatalogVersion(stream, s.Target.GPHome, s.Target.MasterDataDir())
		if err != nil {
			return err
		}

		s.TargetCatalogVersion = version
		return s.SaveConfig()
	})

	st.Run(idl.Substep_SHUTDOWN_TARGET_CLUSTER, func(stream step.OutStreams) error {
		err := s.Target.Stop(stream)

		if err != nil {
			return xerrors.Errorf("stop target cluster: %w", err)
		}

		return nil
	})

	st.AlwaysRun(idl.Substep_CHECK_UPGRADE, func(stream step.OutStreams) error {
		conns, err := s.AgentConns()

		if err != nil {
			return err
		}

		return s.CheckUpgrade(stream, conns)
	})

	// FIXME:
	/*
		After execute to allow customer to test their upgraded primaries. We have two choices:

		1) add mirrors "manually"
		[Needs to be confirmed/prototype]. This includes creating mirror directories, copying upgraded target master to mirror directory, and updating gp_segment_configuration.
		This way is the preferred method.

		1) init target cluster with mirrors
		gpupgrade currently does not do this.
		This would entail manually editing gp_segment_configuration and marking mirrors as down and then starting the target cluster.
	*/

	// The mirrors have not yet been upgraded which occurs in finalize. To
	// allow customers to test out the upgraded cluster on the primaries, we
	// mark the mirrors as down and start the target cluster.
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

		_, err = connection.Exec(`UPDATE gp_segment_configuration SET status = 'd' where preferred_role = 'm';`)
		if err != nil {
			return xerrors.Errorf("set mirror status to down: %w", err)
		}

		return nil
	})

	st.RunInternalSubstep(func() error {
		return s.Target.StopMasterOnly(step.DevNullStream)
	})

	// Other option is to mark mirrors as down during execute after upgrading the master....
	// Needs to be after marking the mirrors as down, so that when starting the
	// target cluster in execute does not bring up the mirrors.
	// that is upgrading the master will wipe the catalog changes from us marking the mirrors as down.
	st.Run(idl.Substep_BACKUP_TARGET_MASTER, func(stream step.OutStreams) error {
		sourceDir := s.Target.MasterDataDir()
		targetDir := filepath.Join(s.StateDir, originalMasterBackupName)
		return RsyncMasterDataDir(stream, sourceDir, targetDir)
	})

	message := &idl.Message{Contents: &idl.Message_Response{Response: &idl.Response{Contents: &idl.Response_InitializeResponse{
		InitializeResponse: &idl.InitializeResponse{
			HasMirrors: s.Config.Source.HasMirrors(),
			HasStandby: s.Config.Source.HasStandby(),
		},
	}}}}

	if err = stream.Send(message); err != nil {
		return err
	}

	return st.Err()
}

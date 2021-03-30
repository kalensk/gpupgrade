// Copyright (c) 2017-2021 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package hub

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/blang/semver/v4"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"golang.org/x/xerrors"

	"github.com/greenplum-db/gpupgrade/greenplum"
	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/step"
	"github.com/greenplum-db/gpupgrade/upgrade"
	"github.com/greenplum-db/gpupgrade/utils"
	"github.com/greenplum-db/gpupgrade/utils/errorlist"
)

func (s *Server) Finalize(_ *idl.FinalizeRequest, stream idl.CliToHub_FinalizeServer) (err error) {
	st, err := step.Begin(s.StateDir, idl.Step_FINALIZE, stream)
	if err != nil {
		return err
	}

	defer func() {
		if ferr := st.Finish(); ferr != nil {
			err = errorlist.Append(err, ferr)
		}

		if err != nil {
			gplog.Error(fmt.Sprintf("finalize: %s", err))
		}
	}()

	st.Run(idl.Substep_SHUTDOWN_TARGET_CLUSTER, func(streams step.OutStreams) error {
		err := s.Target.Stop(streams)

		if err != nil {
			return xerrors.Errorf("failed to stop target cluster: %w", err)
		}

		return nil
	})

	// Upgrade the mirrors in-place when in link mode before updating the catalog
	// and data directories. This way the catalog and mirror data directories
	// will also be updated accordingly. Note that the source cluster mirror
	// data directories are deleted in link mode to "save space". Thus, as a
	// conservative approach its best to upgrade the mirrors in-place before
	// that such that there is a copy of the old cluster as a backup.
	if s.Source.HasMirrors() && s.UseLinkMode {
		st.Run(idl.Substep_UPGRADE_MIRRORS, func(streams step.OutStreams) error {
			return s.upgradeMirrorsInPlace()
		})
	}

	// TODO: We update the ports and datadir. Does the pg_control file that we
	// restore on the mirrors no longer "work" since it has the "old" ports and
	// datadirs? If so, how can we get around that. During recoverseg later on
	// the mirror logs show the following error:
	//
	// seg0,,,,,"ERROR","XX000","could not connect to the primary server: could not connect to server: Connection refused
	// Is the server running on host ""kkrempely-a01.vmware.com"" (::1) and accepting
	// TCP/IP connections on port 50433?
	// could not connect to server: Connection refused
	// Is the server running on host ""kkrempely-a01.vmware.com"" (127.0.0.1) and accepting
	// TCP/IP connections on port 50433?
	st.Run(idl.Substep_UPDATE_TARGET_CATALOG_AND_CLUSTER_CONFIG, func(streams step.OutStreams) error {
		return s.UpdateCatalogAndClusterConfig(streams)
	})

	st.Run(idl.Substep_UPDATE_DATA_DIRECTORIES, func(_ step.OutStreams) error {
		return s.UpdateDataDirectories()
	})

	st.Run(idl.Substep_UPDATE_TARGET_CONF_FILES, func(streams step.OutStreams) error {
		err := UpdateConfFiles(streams,
			semver.MustParse(s.Target.Version.SemVer.String()),
			s.Target.MasterDataDir(),
			s.TargetInitializeConfig.Master.Port,
			s.Source.MasterPort(),
		)
		if err != nil {
			return err
		}

		if s.UseLinkMode && s.Target.HasMirrors() {
			return UpdateTargetMirrorConfFiles(streams, s.TargetInitializeConfig, *s.Source)
		}

		return nil
	})

	st.Run(idl.Substep_START_TARGET_CLUSTER, func(streams step.OutStreams) error {
		err := s.Target.Start(streams)

		if err != nil {
			return xerrors.Errorf("failed to start target cluster: %w", err)
		}

		return nil
	})

	// todo: we don't currently have a way to output nothing to the UI when there is no standby.
	// If we did, this check would actually be in `UpgradeStandby`
	if s.Source.HasStandby() {
		st.Run(idl.Substep_UPGRADE_STANDBY, func(streams step.OutStreams) error {
			// TODO: once the temporary standby upgrade is fixed, switch to
			// using the TargetInitializeConfig's temporary assignments, and
			// move this upgrade step back to before the target shutdown.
			standby := s.Source.Mirrors[-1]
			return UpgradeStandby(greenplum.NewRunner(s.Target, streams), StandbyConfig{
				Port:            standby.Port,
				Hostname:        standby.Hostname,
				DataDirectory:   standby.DataDir,
				UseHbaHostnames: s.UseHbaHostnames,
			})
		})
	}

	// todo: we don't currently have a way to output nothing to the UI when there are no mirrors.
	// If we did, this check would actually be in `UpgradeMirrors`
	if s.Source.HasMirrors() && !s.UseLinkMode {
		st.Run(idl.Substep_UPGRADE_MIRRORS, func(streams step.OutStreams) error {
			// TODO: once the temporary mirror upgrade is fixed, switch to using
			// the TargetInitializeConfig's temporary assignments, and move this
			// upgrade step back to before the target shutdown.
			mirrors := func(seg *greenplum.SegConfig) bool {
				return seg.IsMirror()
			}

			return UpgradeMirrors(s.StateDir, s.Connection, s.Target.MasterPort(),
				s.Source.SelectSegments(mirrors), greenplum.NewRunner(s.Target, streams), s.UseHbaHostnames)
		})
	}

	// Since in link mode we initialize the target cluster with mirrors rather
	// than "manually" adding them we mark the mirrors down to prevent drift
	// between the primaries. Thus, run gprecoverseg to bring them up and replay
	// any potential changes that occurred on the primary between execute and
	// finalize which should be nothing.

	// Note: Need to run gprecoverseg after adding the standby otherwise it
	// fails with "Invalid GpArray ... Standby: Not Configured".
	if s.UseLinkMode {
		st.Run(idl.Substep_RECOVERSEG_TARGET_CLUSTER, func(streams step.OutStreams) error {
			return Recoverseg(streams, s.Target, s.UseHbaHostnames)
		})
	}

	// FIXME: archiveDir is not set unless we actually run this substep; it must be persisted.
	var archiveDir string
	st.Run(idl.Substep_ARCHIVE_LOG_DIRECTORIES, func(_ step.OutStreams) error {
		// Archive log directory on master
		oldDir, err := utils.GetLogDir()
		if err != nil {
			return err
		}
		archiveDir = filepath.Join(filepath.Dir(oldDir), upgrade.GetArchiveDirectoryName(s.UpgradeID, time.Now()))

		gplog.Debug("moving directory %q to %q", oldDir, archiveDir)
		if err = utils.Move(oldDir, archiveDir); err != nil {
			return err
		}

		return ArchiveSegmentLogDirectories(s.agentConns, s.Config.Target.MasterHostname(), archiveDir)
	})

	st.Run(idl.Substep_DELETE_SEGMENT_STATEDIRS, func(_ step.OutStreams) error {
		return DeleteStateDirectories(s.agentConns, s.Source.MasterHostname())
	})

	message := &idl.Message{Contents: &idl.Message_Response{Response: &idl.Response{Contents: &idl.Response_FinalizeResponse{
		FinalizeResponse: &idl.FinalizeResponse{
			TargetVersion:                     s.Target.Version.VersionString,
			LogArchiveDirectory:               archiveDir,
			ArchivedSourceMasterDataDirectory: s.Config.TargetInitializeConfig.Master.DataDir + upgrade.OldSuffix,
			UpgradeID:                         s.Config.UpgradeID.String(),
			Target: &idl.Cluster{
				Port:                int32(s.Target.MasterPort()),
				MasterDataDirectory: s.Target.MasterDataDir(),
			},
		},
	}}}}

	if err = stream.Send(message); err != nil {
		return err
	}

	return st.Err()
}

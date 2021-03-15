// Copyright (c) 2017-2021 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"

	"github.com/blang/semver/v4"
	"golang.org/x/xerrors"

	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/upgrade"
	"github.com/greenplum-db/gpupgrade/utils"
	"github.com/greenplum-db/gpupgrade/utils/rsync"
)

func upgradeSegment(segment Segment, request *idl.UpgradePrimariesRequest, host string) error {
	if request.CheckOnly {
		if err := upgrade.PerformUpgrade(segment.DataDirPair, request, segment.WorkDir); err != nil {
			return xerrors.Errorf("check primary on host %s with content %d: %w", host, segment.Content, err)
		}

		return nil
	}

	if !request.UseLinkMode {
		err := restoreBackup(request, segment)
		if err != nil {
			return xerrors.Errorf("restore master data directory backup on host %s for content id %d: %w", host, segment.Content, err)
		}

		err = RestoreMasterTablespaces(request, segment)
		if err != nil {
			return xerrors.Errorf("restore tablespace on host %s for content id %d: %w", host, segment.Content, err)
		}

		err = upgrade.PerformUpgrade(segment.DataDirPair, request, segment.WorkDir)
		if err != nil {
			return xerrors.Errorf("upgrade primary on host %s with content %d: %w", host, segment.Content, err)
		}

		return nil
	}

	// TODO: Combine with upgrade/segment.go where many of the functions can be shared between upgrading mirrors and primaries.
	if request.UseLinkMode {
		err := restoreBackup(request, segment)
		if err != nil {
			return xerrors.Errorf("restore master data directory backup on host %s for content id %d: %w", host, segment.Content, err)
		}

		err = RestoreMasterTablespaces(request, segment)
		if err != nil {
			return xerrors.Errorf("restore tablespace on host %s for content id %d: %w", host, segment.Content, err)
		}

		err = createTemplate(segment, request)
		if err != nil {
			return xerrors.Errorf("create primary template on host %s with content %d: %w", host, segment.Content, err)
		}

		err = backupTemplate(segment)
		if err != nil {
			return xerrors.Errorf("backup primary template to state dir on host %s with content %d: %w", host, segment.Content, err)
		}

		err = backupTemplateToPrimaryWorkingDir(segment)
		if err != nil {
			return xerrors.Errorf("backup primary template to working dir on host %s with content %d: %w", host, segment.Content, err)
		}

		err = backupPrimaryTablespaces(segment)
		if err != nil {
			return xerrors.Errorf("backup primary tablespaces on host %s with content %d: %w", host, segment.Content, err)
		}

		err = upgrade.LinkTablespacesToTemplate(segment.DataDirPair)
		if err != nil {
			return xerrors.Errorf("link target primary tablespaces to template on host %s with content %d: %w", host, segment.Content, err)
		}

		err = upgrade.PerformUpgrade(segment.DataDirPair, request, segment.WorkDir)
		if err != nil {
			return xerrors.Errorf("upgrade primary on host %s with content %d: %w", host, segment.Content, err)
		}

		err = upgrade.LinkTablespacesToPrimary(segment.DataDirPair)
		if err != nil {
			return xerrors.Errorf("link target primary tablespaces to template on host %s with content %d: %w", host, segment.Content, err)
		}

		if err := backupPrimaryPgControl(segment.GetTargetDataDir(), segment.GetContent()); err != nil {
			return err
		}

		return nil
	}

	return nil
}

// TODO: combine this with performUpgrade()
func createTemplate(segment Segment, request *idl.UpgradePrimariesRequest) error {
	dbid := int(segment.DBID)
	segmentPair := upgrade.SegmentPair{
		Source: &upgrade.Segment{BinDir: request.SourceBinDir, DataDir: segment.SourceDataDir, DBID: dbid, Port: int(segment.SourcePort)},
		Target: &upgrade.Segment{BinDir: request.TargetBinDir, DataDir: segment.TargetDataDir, DBID: dbid, Port: int(segment.TargetPort)},
	}

	options := []upgrade.Option{
		upgrade.WithExecCommand(execCommand),
		upgrade.WithWorkDir(segment.WorkDir),
		upgrade.WithSegmentMode(),
	}

	// During gpupgrade execute, tablepace mapping file is copied after
	// the master has been upgraded. So, don't pass this option during
	// --check mode. There is no test in pg_upgrade which depends on the
	// existence of this file.
	options = append(options, upgrade.WithTablespaceFile(request.TablespacesMappingFilePath))

	options = append(options, upgrade.CreateTemplate())
	options = append(options, upgrade.WithLinkMode())

	// TODO: remove SegmentPair and targetVersion parameters in favor of idl.pgOptions
	return upgrade.Run(segmentPair, semver.MustParse(request.TargetVersion), options...)
}

func backupTemplate(segment Segment) error {
	if err := os.MkdirAll(utils.GetTemplateBackupDir(segment.GetContent()), 0700); err != nil {
		return err
	}

	// Note: No need to exclude any files as we are backing everything up to
	// the state directory. When copying it from the sate directory to the mirror,
	// or elsewhere we can exclude files then.
	options := []rsync.Option{
		rsync.WithSources(segment.GetTargetDataDir() + string(os.PathSeparator)),
		rsync.WithDestination(utils.GetTemplateBackupDir(segment.GetContent())),
		rsync.WithOptions("--archive", "--delete"),
	}

	return rsync.Rsync(options...)
}

func backupTemplateToPrimaryWorkingDir(segment Segment) error {
	if err := os.MkdirAll(utils.GetTemplateWorkingDir(segment.GetContent()), 0700); err != nil {
		return err
	}

	// Note: No need to exclude any files as we are backing everything up to
	// the state directory. When copying it from the sate directory to the mirror,
	// or elsewhere we can exclude files then.
	options := []rsync.Option{
		rsync.WithSources(segment.GetTargetDataDir() + string(os.PathSeparator)),
		rsync.WithDestination(utils.GetTemplateWorkingDir(segment.GetContent())),
		rsync.WithOptions("--archive", "--delete"),
	}

	return rsync.Rsync(options...)
}

func backupPrimaryTablespaces(segment Segment) error {
	if segment.GetTablespaces() == nil {
		return nil
	}

	var sources []string
	for _, tablespace := range segment.GetTablespaces() {
		if !tablespace.GetUserDefined() {
			continue
		}

		// Note: The source directory includes the segment dbID, rather than just the root tablespace location.
		// TODO: This could be a helper called Get6XTablespace or similar.
		//  ~/.gpupgrade/tablespaces/p0/<tablespaceOID>/<dbID>/
		//  See diagram in directories.go. Specifically:
		//   GPDB 5X:  DIR/<fsname>/<datadir>/<tablespaceOID>/<dbOID>/<relfilenode>
		//   GPDB 6X:  DIR/<fsname>/<datadir>/<tablespaceOID>/<dbID>/GPDB_6_<catalogVersion>/<dbOID>/<relfilenode>
		sources = append(sources, filepath.Join(tablespace.Location, strconv.Itoa(int(segment.GetDBID())))+string(os.PathSeparator))
	}

	// TODO: Change initial if statement to if there are no user defined tablespaces then return early.
	if sources == nil {
		return nil
	}

	options := []rsync.Option{
		rsync.WithSources(sources...),
		rsync.WithDestination(utils.GetBackupTablespaceDirForPrimary(segment.GetContent())),
		rsync.WithOptions("--archive", "--delete"),
	}

	return rsync.Rsync(options...)
}

func restoreBackup(request *idl.UpgradePrimariesRequest, segment Segment) error {
	options := []rsync.Option{
		rsync.WithSources(request.MasterBackupDir + string(os.PathSeparator)),
		rsync.WithDestination(segment.TargetDataDir),
		rsync.WithOptions("--archive", "--delete"),
		rsync.WithExcludedFiles(
			"internal.auto.conf",
			"postgresql.conf",
			"pg_hba.conf",
			"postmaster.opts",
			"gp_dbid",
			"gpssh.conf",
			"gpperfmon",
			"pg_replslot", // NOTE: need to exclude pg_replslot, postgresql.auto.conf and recovery.conf
			"postgresql.auto.conf",
			"recovery.conf"),
	}

	return rsync.Rsync(options...)
}

func RestoreMasterTablespaces(request *idl.UpgradePrimariesRequest, segment Segment) error {
	for oid, tablespace := range segment.Tablespaces {
		if !tablespace.GetUserDefined() {
			continue
		}

		targetDir := utils.GetTablespaceLocationForDbId(tablespace, int(segment.DBID))
		sourceDir := utils.GetMasterTablespaceLocation(filepath.Dir(request.TablespacesMappingFilePath), int(oid))

		options := []rsync.Option{
			rsync.WithSources(sourceDir + string(os.PathSeparator)),
			rsync.WithDestination(targetDir),
			rsync.WithOptions("--archive", "--delete"),
		}

		if err := rsync.Rsync(options...); err != nil {
			return xerrors.Errorf("rsync master tablespace directory to segment tablespace directory: %w", err)
		}

		symLinkName := fmt.Sprintf("%s/pg_tblspc/%s", segment.TargetDataDir, strconv.Itoa(int(oid)))
		if err := ReCreateSymLink(targetDir, symLinkName); err != nil {
			return xerrors.Errorf("recreate symbolic link: %w", err)
		}
	}

	return nil
}

var ReCreateSymLink = func(sourceDir, symLinkName string) error {
	return upgrade.ReCreateSymLink(sourceDir, symLinkName)
}

func backupPrimaryPgControl(targetDataDir string, content int32) error {
	data, err := ioutil.ReadFile(filepath.Join(targetDataDir, "global", "pg_control"))
	if err != nil {
		return xerrors.Errorf("read target mirror pg_control file: %w", err)
	}

	if err := os.MkdirAll(filepath.Join(utils.GetBackupPrimaryDir(content), "global"), 0700); err != nil {
		return err
	}

	path := filepath.Join(utils.GetBackupPrimaryDir(content), "global", "pg_control")
	return utils.AtomicallyWrite(path, data)
}

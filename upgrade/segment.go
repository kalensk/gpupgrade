//  Copyright (c) 2017-2021 VMware, Inc. or its affiliates
//  SPDX-License-Identifier: Apache-2.0

package upgrade

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"golang.org/x/xerrors"

	"github.com/blang/semver/v4"

	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/utils"
	"github.com/greenplum-db/gpupgrade/utils/rsync"
)

func UpgradeMirror(opt idl.PgOptions) error {
	if err := rsyncTemplateToMirror(opt); err != nil {
		return err
	}

	err := syncPrimaryTablespacesToMirrorTablespaces(opt)
	if err != nil {
		return err
	}

	if err := linkTablespacesToTemplate(opt); err != nil {
		return err
	}

	if err := createSegmentWorkingDirectory(opt.DBID); err != nil {
		return err
	}

	if err := pgUpgrade(opt); err != nil {
		return err
	}

	if err := linkTablespacesToTargetSegment(opt); err != nil {
		return err
	}

	if err := restorePrimaryPgControlToMirror(opt.GetPrimaryHost(), opt.GetTargetDataDir(), opt.GetContent()); err != nil {
		return err
	}

	return nil
}

// TODO RENAME THIS FUCNTION
func syncPrimaryTablespacesToMirrorTablespaces(opt idl.PgOptions) error {
	for _, tablespace := range opt.Tablespaces {
		if !tablespace.GetUserDefined() {
			continue
		}

		// cp /tmp/primarybackup/* /tmp/gpdb5tablespace/m1/demoDataDir0/16386/3/GPDB_6_301908232/16404/
		options := []rsync.Option{
			rsync.WithSourceHost(opt.GetPrimaryHost()),
			rsync.WithSources(utils.GetBackupTablespaceDirForPrimary(opt.GetContent()) + string(os.PathSeparator)), // ~/.gpupgrade/tablespaces/p0/<tablespaceOID>/<dbID>/GPDB_6_301908232 (which contains 16388/12094 and 16388/2
			rsync.WithDestination(utils.GetTablespaceLocationForDbId(tablespace, int(opt.DBID))),                   // /tmp/gpdb5tablespace/m1/demoDataDir0/16386/3
			rsync.WithOptions("--archive", "--delete"),
		}

		err := rsync.Rsync(options...)
		if err != nil {
			return err
		}
	}

	return nil
}

func rsyncTemplateToMirror(opt idl.PgOptions) error {
	// rsync clean template to mirror data dir
	options := []rsync.Option{
		rsync.WithSourceHost(opt.GetPrimaryHost()),
		rsync.WithSources(utils.GetTemplateBackupDir(opt.GetContent()) + string(os.PathSeparator)),
		rsync.WithDestination(opt.GetTargetDataDir()),
		rsync.WithOptions("--archive", "--delete"),
		// TODO: No need to exclude these if the backup does not contain them...
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

	err := rsync.Rsync(options...)
	if err != nil {
		return err
	}

	// rsync working template to mirror host
	options = []rsync.Option{
		rsync.WithSourceHost(opt.GetPrimaryHost()),
		rsync.WithSources(utils.GetTemplateWorkingDir(opt.GetContent()) + string(os.PathSeparator)),
		rsync.WithDestination(utils.GetTemplateWorkingDir(opt.GetContent())),
		rsync.WithOptions("--archive", "--delete"),
		// TODO: No need to exclude these if the backup does not contain them...
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

	err = rsync.Rsync(options...)
	if err != nil {
		return err
	}

	return nil
}

func linkTablespacesToTemplate(opt idl.PgOptions) error {
	for oid, tablespace := range opt.Tablespaces {
		if !tablespace.GetUserDefined() {
			continue
		}

		// technically not needed for mirrors...But leaving it makes this function symetrical with upgrading primaries
		targetTablespace := fmt.Sprintf("%s/pg_tblspc/%s", opt.GetTargetDataDir(), strconv.Itoa(int(oid)))
		if err := os.Remove(targetTablespace); err != nil {
			return err
		}

		// This step appears to not be needed since we rysnc with --archive which preserves the symlinks...?
		// Probably best to leave it so that it is symmetrical to the linkTablespacesToPrimary function.
		targetDir := utils.GetTablespaceLocationForDbId(tablespace, int(opt.DBID))
		symLinkName := fmt.Sprintf("%s/pg_tblspc/%s", utils.GetTemplateWorkingDir(opt.GetContent()), strconv.Itoa(int(oid)))
		if err := link(targetDir, symLinkName); err != nil {
			return xerrors.Errorf("recreate symbolic link: %w", err)
		}
	}

	return nil
}

func createSegmentWorkingDirectory(dbid int32) error {
	workdir := GetSegmentWorkingDir(dbid)
	err := utils.System.MkdirAll(workdir, 0700)
	if err != nil {
		return xerrors.Errorf("creating pg_upgrade work directory %q: %w", workdir, err)
	}

	return nil
}

func pgUpgrade(opt idl.PgOptions) error {
	options := []Option{
		WithExecCommand(execCommand),
		WithWorkDir(GetSegmentWorkingDir(opt.DBID)),
		WithSegmentMode(),
		WithTablespaceFile(opt.TablespacesMappingFilePath),
		TemplateDataDir(utils.GetTemplateWorkingDir(opt.GetContent())),
		TemplatePort(int(opt.GetTargetPort())),
	}

	if opt.UseLinkMode {
		options = append(options, WithLinkMode())
	}

	segmentPair := SegmentPair{
		Source: &Segment{BinDir: opt.SourceBinDir, DataDir: opt.SourceDataDir, DBID: int(opt.DBID), Port: int(opt.SourcePort)},
		Target: &Segment{BinDir: opt.TargetBinDir, DataDir: opt.TargetDataDir, DBID: int(opt.DBID), Port: int(opt.TargetPort)},
	}

	// TODO: remove SegmentPair and targetVersion parameters in favor of idl.pgOptions
	return Run(segmentPair, semver.MustParse(opt.TargetVersion), options...)
}

func linkTablespacesToTargetSegment(opt idl.PgOptions) error {
	for oid, tablespace := range opt.Tablespaces {
		if !tablespace.GetUserDefined() {
			continue
		}

		templateTablespace := fmt.Sprintf("%s/pg_tblspc/%s", utils.GetTemplateWorkingDir(opt.GetContent()), strconv.Itoa(int(oid)))
		if err := os.Remove(templateTablespace); err != nil {
			return err
		}

		targetDir := utils.GetTablespaceLocationForDbId(tablespace, int(opt.DBID))
		symLinkName := fmt.Sprintf("%s/pg_tblspc/%s", opt.GetTargetDataDir(), strconv.Itoa(int(oid)))
		if err := link(targetDir, symLinkName); err != nil {
			return xerrors.Errorf("recreate symbolic link: %w", err)
		}
	}

	return nil
}

func link(source string, target string) error {
	// TODO: refactor to try symlink and check LinkError if link already exists...
	_, err := utils.System.Lstat(target)
	if err == nil {
		if err := utils.System.Remove(target); err != nil {
			return xerrors.Errorf("unlink %q: %w", target, err)
		}
	} else if !os.IsNotExist(err) {
		return xerrors.Errorf("stat symbolic link %q: %w", target, err)
	}

	if err := utils.System.Symlink(source, target); err != nil {
		return xerrors.Errorf("create symbolic link %q to directory %q: %w", target, source, err)
	}

	return nil
}

func restorePrimaryPgControlToMirror(primaryHost string, targetDataDir string, content int32) error {
	options := []rsync.Option{
		rsync.WithSourceHost(primaryHost),
		rsync.WithSources(filepath.Join(utils.GetBackupMirrorDir(content), "global", "pg_control")),
		rsync.WithDestination(filepath.Join(targetDataDir, "global", "pg_control")),
		rsync.WithOptions("--archive"),
	}

	return rsync.Rsync(options...)
}

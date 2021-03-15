//  Copyright (c) 2017-2021 VMware, Inc. or its affiliates
//  SPDX-License-Identifier: Apache-2.0

package upgrade

import (
	"fmt"
	"os"
	"strconv"

	"github.com/blang/semver/v4"
	"golang.org/x/xerrors"

	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/utils"
)

// TODO: Merge this with segment.go
func LinkTablespacesToTemplate(dataDirPair *idl.DataDirPair) error {
	for oid, tablespace := range dataDirPair.Tablespaces {
		if !tablespace.GetUserDefined() {
			continue
		}

		targetTablespace := fmt.Sprintf("%s/pg_tblspc/%s", dataDirPair.GetTargetDataDir(), strconv.Itoa(int(oid)))
		if err := os.Remove(targetTablespace); err != nil {
			return err
		}

		targetDir := utils.GetTablespaceLocationForDbId(tablespace, int(dataDirPair.DBID))
		symLinkName := fmt.Sprintf("%s/pg_tblspc/%s", utils.GetTemplateWorkingDir(dataDirPair.GetContent()), strconv.Itoa(int(oid)))
		if err := ReCreateSymLink(targetDir, symLinkName); err != nil {
			return xerrors.Errorf("recreate symbolic link: %w", err)
		}
	}

	return nil
}

func PerformUpgrade(dataDirPair *idl.DataDirPair, request *idl.UpgradePrimariesRequest, workDir string) error {
	dbid := int(dataDirPair.DBID)
	segmentPair := SegmentPair{
		Source: &Segment{BinDir: request.SourceBinDir, DataDir: dataDirPair.SourceDataDir, DBID: dbid, Port: int(dataDirPair.SourcePort)},
		Target: &Segment{BinDir: request.TargetBinDir, DataDir: dataDirPair.TargetDataDir, DBID: dbid, Port: int(dataDirPair.TargetPort)},
	}

	options := []Option{
		WithExecCommand(execCommand),
		WithWorkDir(workDir),
		WithSegmentMode(),
	}

	if request.CheckOnly {
		options = append(options, WithCheckOnly())
	} else {
		// During gpupgrade execute, tablepace mapping file is copied after
		// the master has been upgraded. So, don't pass this option during
		// --check mode. There is no test in pg_upgrade which depends on the
		// existence of this file.
		options = append(options, WithTablespaceFile(request.TablespacesMappingFilePath))
	}

	if request.UseLinkMode {
		options = append(options, WithLinkMode())
		if !request.CheckOnly {
			options = append(options, TemplateDataDir(utils.GetTemplateWorkingDir(dataDirPair.GetContent())))
			options = append(options, TemplatePort(int(dataDirPair.GetTargetPort())))
		}
	}

	// TODO: remove SegmentPair and targetVersion parameters in favor of idl.pgOptions
	return Run(segmentPair, semver.MustParse(request.TargetVersion), options...)
}

func LinkTablespacesToPrimary(dataDirPair *idl.DataDirPair) error {
	for oid, tablespace := range dataDirPair.Tablespaces {
		if !tablespace.GetUserDefined() {
			continue
		}

		templateTablespace := fmt.Sprintf("%s/pg_tblspc/%s", utils.GetTemplateWorkingDir(dataDirPair.GetContent()), strconv.Itoa(int(oid)))
		if err := os.Remove(templateTablespace); err != nil {
			return err
		}

		targetDir := utils.GetTablespaceLocationForDbId(tablespace, int(dataDirPair.DBID))
		symLinkName := fmt.Sprintf("%s/pg_tblspc/%s", dataDirPair.GetTargetDataDir(), strconv.Itoa(int(oid)))
		if err := ReCreateSymLink(targetDir, symLinkName); err != nil {
			return xerrors.Errorf("recreate symbolic link: %w", err)
		}
	}

	return nil
}

func ReCreateSymLink(sourceDir, symLinkName string) error {
	_, err := utils.System.Lstat(symLinkName)
	if err == nil {
		if err := utils.System.Remove(symLinkName); err != nil {
			return xerrors.Errorf("unlink %q: %w", symLinkName, err)
		}
	} else if !os.IsNotExist(err) {
		return xerrors.Errorf("stat symbolic link %q: %w", symLinkName, err)
	}

	if err := utils.System.Symlink(sourceDir, symLinkName); err != nil {
		return xerrors.Errorf("create symbolic link %q to directory %q: %w", symLinkName, sourceDir, err)
	}

	return nil
}

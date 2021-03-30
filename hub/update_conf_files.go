// Copyright (c) 2017-2021 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package hub

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/blang/semver/v4"

	"github.com/greenplum-db/gpupgrade/greenplum"
	"github.com/greenplum-db/gpupgrade/step"
	"github.com/greenplum-db/gpupgrade/utils/errorlist"
)

func UpdateConfFiles(streams step.OutStreams, version semver.Version, masterDataDir string, oldPort, newPort int) error {
	if version.Major < 7 {
		if err := UpdateGpperfmonConf(streams, masterDataDir); err != nil {
			return err
		}
	}

	if err := UpdatePostgresqlConf(streams, masterDataDir, oldPort, newPort); err != nil {
		return err
	}

	return nil
}

func UpdateTargetMirrorConfFiles(streams step.OutStreams, oldCluster InitializeConfig, newCluster greenplum.Cluster) error {
	errs := make(chan error, len(newCluster.Mirrors))
	var wg sync.WaitGroup

	for contentId, newMirror := range newCluster.Mirrors {
		if contentId == -1 {
			continue // skip the standby as it does not yet exist.
		}

		newMirror := newMirror

		newPrimary := newCluster.Primaries[contentId]
		oldPrimary := oldCluster.Primaries.Select(func(config *greenplum.SegConfig) bool {
			return config.ContentID == contentId
		})[0]

		wg.Add(1)
		go func() {
			defer wg.Done()

			// FIXME: Why is the postgresql.conf of the mirror contain the old
			//  primary port rather than old mirror port? Are we erroneously
			//  replacing the target mirror postgresql.conf?
			errs <- UpdatePostgresqlConf(streams, newMirror.DataDir, oldPrimary.Port, newMirror.Port)
			errs <- UpdateRecoveryConf(streams, newMirror.DataDir, oldPrimary.Port, newPrimary.Port)
		}()
	}

	wg.Wait()
	close(errs)

	var err error
	for e := range errs {
		err = errorlist.Append(err, e)
	}

	return err
}

func UpdateGpperfmonConf(streams step.OutStreams, masterDataDir string) error {
	logDir := filepath.Join(masterDataDir, "gpperfmon", "logs")

	pattern := `^log_location = .*$`
	replacement := fmt.Sprintf("log_location = %s", logDir)

	// TODO: allow arbitrary whitespace around the = sign?
	cmd := execCommand(
		"sed",
		"-i.bak", // in-place substitution with .bak backup extension
		fmt.Sprintf(`s|%s|%s|`, pattern, replacement),
		filepath.Join(masterDataDir, "gpperfmon", "conf", "gpperfmon.conf"),
	)

	cmd.Stdout, cmd.Stderr = streams.Stdout(), streams.Stderr()
	return cmd.Run()
}

func UpdatePostgresqlConf(streams step.OutStreams, dataDir string, oldPort, newPort int) error {
	// NOTE: any additions of forward slashes (/) here require an update to the
	// sed script below
	pattern := fmt.Sprintf(`(^port[ \t]*=[ \t]*)%d([^0-9]|$)`, oldPort)
	replacement := fmt.Sprintf(`\1%d\2`, newPort)

	path := filepath.Join(dataDir, "postgresql.conf")

	cmd := execCommand(
		"sed",
		"-E",     // use POSIX extended regexes
		"-i.bak", // in-place substitution with .bak backup extension
		fmt.Sprintf(`s/%s/%s/`, pattern, replacement),
		path,
	)

	cmd.Stdout, cmd.Stderr = streams.Stdout(), streams.Stderr()
	return cmd.Run()
}

// TODO: This is the exact same as the postgresql.conf function except the
//  pattern is not anchored to the beginning of the line...Consider refactoring.
func UpdateRecoveryConf(streams step.OutStreams, dataDir string, oldPort, newPort int) error {
	// NOTE: any additions of forward slashes (/) here require an update to the
	// sed script below
	pattern := fmt.Sprintf(`(port[ \t]*=[ \t]*)%d([^0-9]|$)`, oldPort)
	replacement := fmt.Sprintf(`\1%d\2`, newPort)

	path := filepath.Join(dataDir, "recovery.conf")

	cmd := execCommand(
		"sed",
		"-E",     // use POSIX extended regexes
		"-i.bak", // in-place substitution with .bak backup extension
		fmt.Sprintf(`s/%s/%s/`, pattern, replacement),
		path,
	)

	cmd.Stdout, cmd.Stderr = streams.Stdout(), streams.Stderr()
	return cmd.Run()
}

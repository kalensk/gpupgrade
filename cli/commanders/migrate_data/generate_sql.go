//  Copyright (c) 2017-2020 VMware, Inc. or its affiliates
//  SPDX-License-Identifier: Apache-2.0

package migrate_data

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"golang.org/x/xerrors"

	"github.com/greenplum-db/gpupgrade/upgrade"
)

var execCommand = exec.Command

var ApplyOnceFiles = []string{"gen_alter_gphdfs_roles.sql"}

// inputDir: /usr/local/greenplum-upgrade/.data-migration-scripts/<phase>
// outputDir: /var/greenplum/upgrade/data-migration-scripts/<phase>/<datname>
func GenerateSql(inputDir string, outputDir string, gphome string, port int) error {
	dbConns, err := getDbConnections(port)
	if err != nil {
		return xerrors.Errorf("getting database connections: %w", err)
	}
	defer dbConns.Close()

	err = os.RemoveAll(outputDir)
	if err != nil {
		return xerrors.Errorf("removing %q: %w", outputDir, err)
	}

	err = os.Mkdir(outputDir, 0700)
	if err != nil {
		return xerrors.Errorf("creating %q: %w", outputDir, err)
	}

	for _, phase := range Phases {
		outputPhasePath := filepath.Join(outputDir, phase)
		err = os.Mkdir(outputPhasePath, 0700)
		if err != nil {
			return xerrors.Errorf("creating %q: %w", outputPhasePath, err)
		}

		inputMigrationPath := filepath.Join(inputDir, phase)
		files, err := ioutil.ReadDir(inputMigrationPath)
		if err != nil {
			return xerrors.Errorf("reading %q: %w", inputMigrationPath, err)
		}

		fmt.Println(fmt.Sprintf("generating %q scripts...", phase))

		// TODO: add go funcs
		for datname, dbConn := range dbConns.Conns {
			for _, f := range files {
				phase := New(outputPhasePath, inputMigrationPath, f.Name(), datname)

				if strings.HasSuffix(f.Name(), ".sh") || strings.HasSuffix(f.Name(), ".bash") {
					err := executeBashScript(phase, gphome, port)
					if err != nil {
						return xerrors.Errorf("executing BASH script: %w", err)
					}

					continue
				}

				if runScriptOnce(f.Name()) || datname == "postgres" {
					continue
				}

				if strings.HasSuffix(f.Name(), ".sql") {
					err = executeSqlScript(phase, dbConn)
					if err != nil {
						return xerrors.Errorf("executing SQL script %q: %w", phase.SQLPath, err)
					}
				}
			}
		}
	}

	return nil
}

func runScriptOnce(name string) bool {
	for _, f := range ApplyOnceFiles {
		return name == f
	}

	return false
}

// TODO: Consider porting BASH scripts to Golang
func executeBashScript(phase *Phase, gphome string, port int) error {
	cmd := execCommand(phase.SQLPath, gphome, strconv.Itoa(port), phase.Datname)
	gplog.Debug("running cmd %q", cmd.String())
	output, err := cmd.CombinedOutput()
	if err != nil {
		return xerrors.Errorf("%q failed with %q: %w", cmd.String(), string(output), err)
	}

	err = writeOutputFile(phase, output)
	if err != nil {
		return xerrors.Errorf("writing output file %q: %w", phase.OutputPath, err)
	}

	return nil
}

func executeSqlScript(phase *Phase, dbConn *sql.DB) error {
	script, err := ioutil.ReadFile(phase.SQLPath)
	if err != nil {
		return err
	}

	rows, err := dbConn.Query(string(script))
	if err != nil {
		return xerrors.Errorf("querying database %q with %q: %w", phase.Datname, script, err)
	}

	var output string // TODO: Should this be []byte?
	for rows.Next() {
		err := rows.Scan(&output)
		if err != nil {
			return xerrors.Errorf("scanning row for output: %w", err)
		}
	}

	err = rows.Err()
	if err != nil {
		return xerrors.Errorf("reading rows from querying database %q with %q: %w", phase.Datname, script, err)
	}

	if output == "" {
		return nil
	}

	err = writeOutputFile(phase, []byte(output))
	if err != nil {
		return xerrors.Errorf("writing output file %q: %w", phase.OutputPath, err)
	}

	return nil
}

func writeOutputFile(phase *Phase, output []byte) error {
	var err error

	// create output datname directory
	exist, err := upgrade.PathExist(phase.OutputDatnameDir)
	if err != nil {
		return err
	}

	if !exist {
		err := os.Mkdir(phase.OutputDatnameDir, 0700)
		if err != nil {
			return err
		}
	}

	// write header if one exists
	outputFile, err := os.OpenFile(phase.OutputPath, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer func() {
		if cErr := outputFile.Close(); cErr != nil {
			err = cErr
		}
	}()

	exists, err := upgrade.PathExist(phase.HeaderPath)
	if err != nil {
		return err
	}

	if exists {
		header, err := ioutil.ReadFile(phase.HeaderPath)
		if err != nil {
			return err
		}

		if _, err = outputFile.Write(header); err != nil {
			return err
		}
	}

	// write results
	if _, err = outputFile.Write(output); err != nil {
		return err
	}

	return nil
}

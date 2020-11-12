//  Copyright (c) 2017-2020 VMware, Inc. or its affiliates
//  SPDX-License-Identifier: Apache-2.0

package migrate_data

import (
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"path/filepath"

	"golang.org/x/xerrors"
)

// inputDir: /var/greenplum/upgrade/data-migration-scripts/<phase>/<datname>
func ExecuteSQL(inputDir string, phase string, port int) (err error) {
	dbConns, err := getDbConnections(port)
	if err != nil {
		return xerrors.Errorf("getting database connections: %w", err)
	}
	defer dbConns.Close()

	PhasePath := filepath.Join(inputDir, phase)
	datnameDirs, err := ioutil.ReadDir(PhasePath)
	if err != nil {
		return errors.New(fmt.Sprintf(`Ensure "gpupgrade migrate-data generate" has been run. %v`, err))
	}

	if len(datnameDirs) == 0 {
		return xerrors.Errorf("No SQL files found in %q for the %s phase. Exiting.", PhasePath, phase)
	}

	for _, datnameDir := range datnameDirs {
		datnamePath := filepath.Join(PhasePath, datnameDir.Name())
		files, err := ioutil.ReadDir(datnamePath)
		if err != nil {
			return xerrors.Errorf("reading %q: %w", datnamePath, err)
		}

		// TODO: add go funcs
		for _, f := range files {
			sqlPath := filepath.Join(datnamePath, f.Name())

			err := executeSql(sqlPath, dbConns.Conns[datnameDir.Name()])
			if err != nil {
				return xerrors.Errorf("executing SQL script %q: %w", sqlPath, err)
			}
		}
	}

	return nil
}

// TODO: Fix the following
// 1. No output or echo for users for log file
// 2. Need to find a different way of connecting to the database instead of "\c test"
// 2. No setting of parameters such as:
//    \c test
//    \set VERBOSITY terse
//    \unset ECHO
func executeSql(sqlPath string, dbConn *sql.DB) error {
	script, err := ioutil.ReadFile(sqlPath)
	if err != nil {
		return err
	}

	_, err = dbConn.Exec(string(script))
	if err != nil {
		return xerrors.Errorf("executing query %q: %w", script, err)
	}

	return nil
}

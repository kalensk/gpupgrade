//  Copyright (c) 2017-2020 VMware, Inc. or its affiliates
//  SPDX-License-Identifier: Apache-2.0

package migrate_data

import (
	"database/sql"
	"fmt"
	"strings"

	"golang.org/x/xerrors"

	"github.com/greenplum-db/gpupgrade/utils"
)

type dbConns struct {
	port  int
	Conns map[string]*sql.DB
}

func getDbConnections(port int) (*dbConns, error) {
	connURI := fmt.Sprintf("postgresql://localhost:%d/template1?gp_session_role=utility&search_path=", port)
	template1, err := utils.System.SqlOpen("pgx", connURI)
	if err != nil {
		return nil, xerrors.Errorf("opening %q database connection %q: %w", "template1", connURI, err)
	}

	rows, err := template1.Query("SELECT datname FROM pg_database WHERE datname != 'template0'")
	if err != nil {
		return nil, xerrors.Errorf("querying for all databases: %w", err)
	}

	conns := make(map[string]*sql.DB)
	var datnames []string
	var datname string

	for rows.Next() {
		if err := rows.Scan(&datname); err != nil {
			return nil, xerrors.Errorf("scanning datname: %w", err)
		}

		connURI := fmt.Sprintf("postgresql://localhost:%d/%s?search_path=", port, datname)
		conn, err := utils.System.SqlOpen("pgx", connURI)
		if err != nil {
			return nil, xerrors.Errorf("opening %q database connection %q: %w", datname, connURI, err)
		}

		datnames = append(datnames, datname)
		conns[datname] = conn
	}

	err = rows.Err()
	if err != nil {
		return nil, xerrors.Errorf("reading rows from querying for all database: %w", err)
	}

	fmt.Println(fmt.Sprintf("for each database: %s", strings.Join(datnames, ", ")))
	fmt.Println()

	return &dbConns{
		port:  port,
		Conns: conns,
	}, nil
}

func (d *dbConns) Close() {
	for datname, dbConn := range d.Conns {
		err := dbConn.Close()
		if err != nil {
			err = xerrors.Errorf("closing %q database connection: %w", datname, err)
		}
	}
}

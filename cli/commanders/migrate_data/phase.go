//  Copyright (c) 2017-2020 VMware, Inc. or its affiliates
//  SPDX-License-Identifier: Apache-2.0

package migrate_data

import (
	"fmt"
	"path/filepath"
)

var Phases = []string{"start", "complete", "revert", "stats"}

// Phase is a struct that holds various paths for ease of use.
// For example: /data-migration-scripts/<phase>/<datname>/migrate_<basename>.sql
type Phase struct {
	Datname          string
	SQLPath          string
	HeaderPath       string
	OutputDatnameDir string
	OutputPath       string
}

func New(outputPhasePath string, inputMigrationPath string, sqlFile string, datname string) *Phase {
	sqlPath := filepath.Join(inputMigrationPath, sqlFile)
	extension := filepath.Ext(sqlFile)
	BaseName := sqlFile[0 : len(sqlFile)-len(filepath.Ext(sqlFile))]
	outputFileName := fmt.Sprintf("migrate_%s%s", BaseName, extension)

	headerPath := filepath.Join(inputMigrationPath, BaseName+".header")

	outputDatnameDir := filepath.Join(outputPhasePath, datname)
	outputPath := filepath.Join(outputPhasePath, datname, outputFileName)

	return &Phase{
		Datname:          datname,
		SQLPath:          sqlPath,
		HeaderPath:       headerPath,
		OutputDatnameDir: outputDatnameDir,
		OutputPath:       outputPath,
	}
}

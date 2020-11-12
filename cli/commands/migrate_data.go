//  Copyright (c) 2017-2020 VMware, Inc. or its affiliates
//  SPDX-License-Identifier: Apache-2.0

package commands

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"golang.org/x/xerrors"

	"github.com/greenplum-db/gpupgrade/cli"
	"github.com/greenplum-db/gpupgrade/cli/commanders"
	"github.com/greenplum-db/gpupgrade/cli/commanders/migrate_data"
	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/utils"
)

var migrateData = &cobra.Command{
	Use:   "migrate-data",
	Short: "migrate and execute subcommands",
	Long:  "migrate and execute subcommands",
}

func migrateDataGenerate() *cobra.Command {
	var sourceGPHome string
	var sourcePort int
	var inputDir, outputDir string

	cmd := &cobra.Command{
		Use:   "migrate-data generate",
		Short: "generate data migration SQL scripts",
		Long:  migrateDataGenerateHelp,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			cmd.SilenceUsage = true

			err = commanders.CreateStateDir()
			if err != nil {
				nextActions := fmt.Sprintf(`Please address the above issue and run "gpupgrade %s" again.`, strings.ToLower(idl.Step_INITIALIZE.String()))
				return cli.NewNextActions(err, nextActions)
			}

			inputDir, err = filepath.Abs(inputDir)
			if err != nil {
				return err
			}

			outputDir, err = filepath.Abs(outputDir)
			if err != nil {
				return err
			}

			fmt.Println()
			fmt.Print("Generating data migration scripts ")

			err = migrate_data.GenerateSql(inputDir, outputDir, sourceGPHome, sourcePort)
			if err != nil {
				return err
			}

			fmt.Printf(`
Successfuly generated SQL data migration scripts.
Please manually inspect the scripts in %s
`, outputDir)

			return nil
		},
	}

	cmd.Flags().StringVar(&sourceGPHome, "source-gphome", "", "path for the source Greenplum installation")
	cmd.Flags().IntVar(&sourcePort, "source-master-port", 5432, "master port for source gpdb cluster")
	cmd.Flags().StringVar(&inputDir, "input-dir", utils.GetDataMigrationInputDir(), "path for the internal data migration scripts")
	cmd.Flags().MarkHidden("input-dir") //nolint
	cmd.Flags().StringVar(&outputDir, "output-dir", utils.GetDataMigrationOutputDir(), "path to place the generated data migrations scripts")
	cmd.Flags().MarkHidden("output-dir") //nolint

	return addHelpToCommand(cmd, fmt.Sprintf(migrateDataGenerateHelp, outputDir))
}

func migrateDataExecute() *cobra.Command {
	var verbose bool
	var sourcePort int
	var inputDir string
	var phase string

	cmd := &cobra.Command{
		Use:   "migrate-data execute",
		Short: "execute data migration SQL scripts",
		Long:  migrateDataExecuteHelp,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			cmd.SilenceUsage = true

			phase, err := parsePhase(phase)
			if err != nil {
				return err
			}

			fmt.Println()
			fmt.Printf("Executing data migration scripts for %q phase.\n", phase)
			fmt.Println()

			err = migrate_data.ExecuteSQL(inputDir, phase, sourcePort)
			if err != nil {
				return err
			}

			fmt.Printf(`
Successfuly executed SQL data migration scripts.
`)
			return nil
		},
	}

	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "print the output stream from all substeps")
	cmd.Flags().StringVar(&phase, "phase", "", "what phase to run the data migration scripts for. Either start, complete, revert, stats")
	cmd.Flags().IntVar(&sourcePort, "source-master-port", 5432, "master port for source gpdb cluster")
	cmd.Flags().StringVar(&inputDir, "input-dir", utils.GetDataMigrationInputDir(), "path for the internal data migration scripts")
	cmd.Flags().MarkHidden("input-dir") //nolint

	logdir, err := utils.GetLogDir()
	if err != nil {
		panic(fmt.Sprintf("getting log directory: %v", err))
	}

	return addHelpToCommand(cmd, fmt.Sprintf(migrateDataExecuteHelp, logdir))
}

func parsePhase(input string) (string, error) {
	input = strings.ToLower(input)

	for _, phase := range migrate_data.Phases {
		if input == phase {
			return input, nil
		}
	}
	return "", xerrors.Errorf("Phase %q not found. Expected either %s", input, strings.Join(migrate_data.Phases, ", "))
}

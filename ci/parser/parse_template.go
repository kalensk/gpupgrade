/*
	This command is used to parse a template file using the text/template package.
	Given a list of source versions and target versions, it will render these
	versions into the places specified by the template.

	Usage:
	parse_template template.yml output.yml

	Note: This will overwrite the contents of output.yml (if the file already
	exists) with the parsed output.
*/
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"text/template"

	"github.com/blang/semver"
)

var sourceVersions = []string{"6", "5"}
var targetVersions = []string{"6"}

type UpgradeJob struct {
	Source, Target string
	PrimariesOnly  bool
	NoStandby      bool
	UseLinkMode    bool
	RetailDemo     bool
}

type Data struct {
	SourceVersions, TargetVersions []string
	AllVersions                    []string // combination of Source/Target
	UpgradeJobs                    []*UpgradeJob
	LastTargetVersion              string
	PrimariesOnly                  []bool
	ProdTarget                     bool
}

var data Data

func init() {
	var upgradeJobs []*UpgradeJob
	for _, sourceVersion := range sourceVersions {
		for _, targetVersion := range targetVersions {
			upgradeJobs = append(upgradeJobs, &UpgradeJob{
				Source: sourceVersion,
				Target: targetVersion,
			})
		}
	}

	// Special cases for 5->6. (These are special-cased to avoid exploding the
	// test matrix too much.)
	special := []*UpgradeJob{
		{UseLinkMode: true},
		{PrimariesOnly: true},
		{NoStandby: true},
		{RetailDemo: true},
	}

	for _, job := range special {
		job.Source = "5"
		job.Target = "6"

		upgradeJobs = append(upgradeJobs, job)
	}

	// Duplicate version data here in order to simplify template logic
	data = Data{
		SourceVersions:    sourceVersions,
		TargetVersions:    targetVersions,
		AllVersions:       deduplicate(sourceVersions, targetVersions),
		UpgradeJobs:       upgradeJobs,
		LastTargetVersion: targetVersions[len(targetVersions)-1],
	}
}

// deduplicate combines, sorts, and deduplicates two string slices.
func deduplicate(a, b []string) []string {
	var all []string

	all = append(all, a...)
	all = append(all, b...)
	sort.Strings(all)

	// Deduplicate by compacting runs of identical strings.
	cur := 0
	for next := 1; next < len(all); next++ {
		if all[cur] == all[next] {
			continue
		}

		cur++
		all[cur] = all[next]
	}

	return all[:cur+1]
}

func main() {
	flag.BoolVar(&data.ProdTarget, "prod", false, "generate a production pipeline")
	flag.Parse()

	templateFilepath, pipelineFilepath := flag.Arg(0), flag.Arg(1)

	templateFuncs := template.FuncMap{
		// The escapeVersion function is used to ensure that the gcs-resource
		// concourse plugin regex matches the version correctly. As an example
		// if we didn't do this, 60100 would match version 6.1.0
		"escapeVersion": func(version string) string {
			return regexp.QuoteMeta(version)
		},

		// majorVersion parses its string as a semver and returns the major
		// component. E.g. "4.15.3" -> "4"
		"majorVersion": func(version string) string {
			v, err := semver.ParseTolerant(version)
			if err != nil {
				panic(err) // the template engine deals with panics nicely
			}

			return fmt.Sprintf("%d", v.Major)
		},
	}

	yamlTemplate, err := template.New("Pipeline Template").Funcs(templateFuncs).ParseFiles(templateFilepath)
	if err != nil {
		log.Fatalf("error parsing %s: %+v", templateFilepath, err)
	}
	// Duplicate version data here in order to simplify template logic

	templateFilename := filepath.Base(templateFilepath)
	// Create truncates the file if it already exists, and opens it for writing
	pipelineFile, err := os.Create(path.Join(pipelineFilepath))
	if err != nil {
		log.Fatalf("error opening %s: %+v", pipelineFilepath, err)
	}
	_, err = pipelineFile.WriteString("## Code generated by ci/generate.go - DO NOT EDIT\n")
	if err != nil {
		log.Fatalf("error writing %s: %+v", pipelineFilepath, err)
	}

	err = yamlTemplate.ExecuteTemplate(pipelineFile, templateFilename, data)
	closeErr := pipelineFile.Close()
	if err != nil {
		log.Fatalf("error executing template: %+v", err)
	}
	if closeErr != nil {
		log.Fatalf("error closing %s: %+v", pipelineFilepath, closeErr)
	}
}

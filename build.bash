#!/usr/bin/env bash
#
# Copyright (c) 2017-2020 VMware, Inc. or its affiliates
# SPDX-License-Identifier: Apache-2.0

set -e -o pipefail

install() {
    echo install
}

binary() {
    version=$(git describe --tags --abbrev=0)
	commit=$(git rev-parse --short --verify HEAD)

    version_ld_str="
    -X 'github.com/greenplum-db/gpupgrade/cli/commands.Version=${version}'
    -X 'github.com/greenplum-db/gpupgrade/cli/commands.Commit=${commit}'
    -X 'github.com/greenplum-db/gpupgrade/cli/commands.Release=${RELEASE}'"

    gcflags='-gcflags="all=-N -l"'

	env ${BUILD_ENV} go build -o gpupgrade -ldflags "${version_ld_str}" "${gcflags}" github.com/greenplum-db/gpupgrade/cmd/gpupgrade
	go generate ./cli/bash
}

tarball() {
    echo tarball
}

rpm() {
    eacho rpm
}

_main() {
    # For tagging a RELEASE see the "Upgrade Release Checklist" document.
    local RELEASE
    RELEASE="${2:-Dev Release}"

    if [ "$1" == "build" ]; then
        binary "$RELEASE"
    fi

    if [ "$1" == "install" ]; then
        binary "$RELEASE"
    fi

    if [ "$1" == "tarball" ]; then
        binary "$RELEASE"
        tarball
    fi

    if [ "$1" == "rpm" ]; then
        local name="$3"
        local license="$4"

        binary "$RELEASE"
        tarball
        rpm "$name" "$license"
    fi
}

_main "$@"

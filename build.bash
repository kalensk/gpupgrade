#!/usr/bin/env bash
#
# Copyright (c) 2017-2020 VMware, Inc. or its affiliates
# SPDX-License-Identifier: Apache-2.0

set -e -u -o pipefail -x
#set -e -o pipefail -x


install() {
    echo install
}

# go build -o gpupgrade -gcflags="all=-N -l"
#   -ldflags "-X 'github.com/greenplum-db/gpupgrade/cli/commands.Version=1.0.0'
#             -X 'github.com/greenplum-db/gpupgrade/cli/commands.Commit=cdc26d83'
#             -X 'github.com/greenplum-db/gpupgrade/cli/commands.Release=Dev Release'"
#   github.com/greenplum-db/gpupgrade/cmd/gpupgrade

binary() {
    VERSION=$(git describe --tags --abbrev=0)
	COMMIT=$(git rev-parse --short --verify HEAD)

    local version_ld_str
	version_ld_str="-X 'github.com/greenplum-db/gpupgrade/cli/commands.Version=${VERSION}'"
	version_ld_str+=" -X 'github.com/greenplum-db/gpupgrade/cli/commands.Commit=${COMMIT}'"
	version_ld_str+=" -X 'github.com/greenplum-db/gpupgrade/cli/commands.Release=${RELEASE}'"

    local build_flags
    build_flags="-gcflags=\"all=-N -l\""
    build_flags+=" -ldflags "${version_ld_str}""

    # "${BUILD_ENV}"
	go build -o gpupgrade "${build_flags}" github.com/greenplum-db/gpupgrade/cmd/gpupgrade
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
    RELEASE=${2:-Dev Release}

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

#!/usr/bin/env bash

set -e -u -o pipefail -x

# For tagging a RELEASE see the "Upgrade Release Checklist" document.

binary() {
    VERSION=$(git describe --tags --abbrev=0)
    COMMIT=$(git rev-parse --short --verify HEAD)

    local version_ld_str
    version_ld_str="-X github.com/greenplum-db/gpupgrade/cli/commands.Version=${VERSION}"
    version_ld_str+=" -X github.com/greenplum-db/gpupgrade/cli/commands.Commit=${COMMIT}"
    version_ld_str+=" -X github.com/greenplum-db/gpupgrade/cli/commands.Release=${RELEASE}"

    local build_flags
    build_flags=-gcflags="all=-N -l"
    build_flags+=" -ldflags ${version_ld_str}"

    go build -o gpupgrade "${build_flags}" github.com/greenplum-db/gpupgrade/cmd/gpupgrade
    go generate ./cli/bash
}

tarball() {
    [ ! -d tarball ] && mkdir tarball
    # gather files
    cp gpupgrade tarball
    cp cli/bash/gpupgrade.bash tarball
    cp gpupgrade_config tarball
    cp open_source_licenses.txt tarball
    cp -r data-migration-scripts/ tarball/data-migration-scripts/
    # remove test files
    rm -r tarball/data-migration-scripts/test
    # create tarball
    (
        cd tarball
        tar czf ../"${TARBALL_NAME}" .
    )
    sha256sum "${TARBALL_NAME}" > CHECKSUM
    rm -r tarball
}

rpm() {
    local license name
    license="$1"
    name="$2"

    [ ! -d rpm ] && mkdir rpm
    mkdir -p rpm/rpmbuild/{BUILD,RPMS,SOURCES,SPECS}
    cp "${TARBALL_NAME}" rpm/rpmbuild/SOURCES
    cp gpupgrade.spec rpm/rpmbuild/SPECS/
    rpmbuild \
        --define "_topdir $${PWD}/rpm/rpmbuild" \
        --define "gpupgrade_version ${VERSION}" \
        --define "gpupgrade_rpm_RELEASE 1" \
        --define "RELEASE_type ${RELEASE}" \
        --define "license ${license}" \
        --define "summary ${name}" \
        -bb "$(PWD)/rpm/rpmbuild/SPECS/gpupgrade.spec"
    cp rpm/rpmbuild/RPMS/x86_64/gpupgrade-"${VERSION}"*.rpm .
    rm -r rpm
}

_main() {
    local RELEASE
    RELEASE=${2:-Dev Release}

    if [ "$1" == "build" ]; then
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

#!/bin/bash
#
# Copyright (c) 2017-2021 VMware, Inc. or its affiliates
# SPDX-License-Identifier: Apache-2.0

set -eux -o pipefail

source gpupgrade_src/ci/scripts/ci-helpers.bash

#
# MAIN
#

USE_LINK_MODE=${USE_LINK_MODE:-0}
FILTER_DIFF=${FILTER_DIFF:-0}
DIFF_FILE=${DIFF_FILE:-"icw.diff"}

export GPHOME_SOURCE=/usr/local/greenplum-db-source
export GPHOME_TARGET=/usr/local/greenplum-db-target
export PGPORT=5432

./ccp_src/scripts/setup_ssh_to_cluster.sh

# On GPDB version other than 5, set the gucs before taking dumps
if ! is_GPDB5 ${GPHOME_SOURCE}; then
    configure_gpdb_gucs ${GPHOME_SOURCE}
fi

# Dump the old cluster for later comparison.
dump_sql $PGPORT /tmp/source.sql

# Now do the upgrade.
LINK_MODE=""
if [ "${USE_LINK_MODE}" = "1" ]; then
    LINK_MODE="--mode=link"
fi

time ssh mdw bash <<EOF
    set -eux -o pipefail

    gpupgrade initialize \
              $LINK_MODE \
              --automatic \
              --target-gphome ${GPHOME_TARGET} \
              --source-gphome ${GPHOME_SOURCE} \
              --source-master-port $PGPORT \
              --temp-port-range 6020-6040
    # TODO: rather than setting a temp port range, consider carving out an
    # ip_local_reserved_ports range during/after CCP provisioning.

    gpupgrade execute --non-interactive
    gpupgrade finalize --non-interactive
EOF

# On GPDB version other than 5, set the gucs before taking dumps
# and reindex all the databases to enable bitmap indexes which were
# marked invalid during upgrade
if ! is_GPDB5 ${GPHOME_TARGET}; then
    configure_gpdb_gucs ${GPHOME_TARGET}
    reindex_all_dbs ${GPHOME_TARGET}
fi

# Dump the target cluster and compare.
# TODO: Are there additional checks to ensure the cluster was actually upgraded
# since after finalize the source and target cluster appear identical such as
# data directories getting renmaed and PGPORT. Perhaps fields in pg_controldata?
dump_sql ${PGPORT} /tmp/target.sql
if ! compare_dumps /tmp/source.sql /tmp/target.sql; then
    echo 'error: before and after dumps differ'
    exit 1
fi

echo 'Upgrade successful.'

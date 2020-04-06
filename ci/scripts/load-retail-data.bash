#!/bin/bash

set -eux -o pipefail

./ccp_src/scripts/setup_ssh_to_cluster.sh

# Cache our list of hosts to loop over below.
mapfile -t hosts < cluster_env_files/hostfile_all

# Retrieves the installed GPHOME for a given GPDB RPM.
rpm_gphome() {
    local package_name=$1

    local version=$(ssh -n mdw rpm -q --qf '%{version}' "$package_name")
    echo /usr/local/greenplum-db-$version
}

export GPHOME_OLD=$(rpm_gphome ${OLD_PACKAGE})
export GPHOME_NEW=$(rpm_gphome ${NEW_PACKAGE})

export SOURCE_MASTER_PORT=5432

# Copy binaries to test runner container to help compile bm.so
scp -qr mdw:${GPHOME_OLD} ${GPHOME_OLD}
scp -qr mdw:${GPHOME_NEW} ${GPHOME_NEW}

pushd retail_demo_src/box_muller/
  # make bm.so for source cluster
  make PG_CONFIG=${GPHOME_OLD}/bin/pg_config clean all

  # Install bm.so onto the segments
  for host in "${hosts[@]}"; do
      scp bm.so $host:/tmp
      ssh centos@$host "sudo mv /tmp/bm.so ${GPHOME_OLD}/lib/postgresql/bm.so"
  done

  # make bm.so for target cluster
  make PG_CONFIG=${GPHOME_NEW}/bin/pg_config clean all

  # Install bm.so onto the segments for target cluster
  for host in "${hosts[@]}"; do
      scp bm.so $host:/tmp
      ssh centos@$host "sudo mv /tmp/bm.so ${GPHOME_NEW}/lib/postgresql/bm.so"
  done
popd

# extract demo_data for both mdw and segments
pushd retail_demo_src
    tar xf demo_data.tar.xz

    pushd demo_data
        # decimate key data files to speed things up
        for f in male_first_names.txt female_first_names.txt products_full.dat surnames.dat; do
            awk 'NR % 10 == 0' "$f" > tmp.txt
            mv tmp.txt "$f"
        done
    popd
popd

# copy extracted demo_data and retail_demo_src to mdw
scp -qr retail_demo_src mdw:/home/gpadmin/industry_demo/

# create database and tables
ssh mdw <<EOF
    set -x

    source ${GPHOME_OLD}/greenplum_path.sh
    cd /home/gpadmin/industry_demo
    psql -d template1 -e -f data_generation/prep_database.sql
    psql -d gpdb_demo -e -f data_generation/prep_external_tables.sql
EOF

# copy extracted demo_data to segments and start gpfdist
for host in "${hosts[@]}"; do
    scp -qr retail_demo_src/demo_data/ $host:/data/

    ssh -n $host "
        set -x

        source ${GPHOME_OLD}/greenplum_path.sh
        gpfdist -d /data/demo_data -p 8081 -l /data/demo_data/gpfdist.8081.log &
        gpfdist -d /data/demo_data -p 8082 -l /data/demo_data/gpfdist.8082.log &
    "
done

# prepare and generate data
time ssh mdw <<EOF
    set -x

    source ${GPHOME_OLD}/greenplum_path.sh

    # Why do we need to restart in order to have the bm.so extension take affect?
    # todo: why does the -d from gpstop not get passed down to gpstart?
    export MASTER_DATA_DIRECTORY=/data/gpdata/master/gpseg-1
    PGPORT=${SOURCE_MASTER_PORT} gpstop -ar

    cd /home/gpadmin/industry_demo
    psql -d gpdb_demo -e -f data_generation/prep_UDFs.sql

    data_generation/prep_GUCs.sh

    # preparing data
    psql -d gpdb_demo -e -f data_generation/prep_retail_xts_tables.sql
    psql -d gpdb_demo -e -f data_generation/prep_dimensions.sql
    psql -d gpdb_demo -e -f data_generation/prep_facts.sql
    psql -d gpdb_demo -e -f data_generation/prep_tablespace.sql

    # generating data
    psql -d gpdb_demo -e -f data_generation/gen_order_base.sql
    psql -d gpdb_demo -e -f data_generation/gen_facts.sql
    psql -d gpdb_demo -e -f data_generation/gen_load_files.sql
    psql -d gpdb_demo -e -f data_generation/load_RFMT_Scores.sql

    # verifying data
    # TODO: assert on the output of verification script
    psql -d gpdb_demo -e -f data_generation/verify_data.sql
EOF

# perform upgrade fixups:
# - remove gphdfs from the source 5X cluster
# - drop partition indices
# - match root/child partition schemas
ssh mdw "
    set -x

    source ${GPHOME_OLD}/greenplum_path.sh
    psql -d gpdb_demo <<SQL_EOF
        CREATE OR REPLACE FUNCTION drop_gphdfs() RETURNS VOID AS \\\$\\\$
        DECLARE
          rolerow RECORD;
        BEGIN
          RAISE NOTICE 'Dropping gphdfs users...';
          FOR rolerow IN SELECT * FROM pg_catalog.pg_roles LOOP
            EXECUTE 'alter role '
              || quote_ident(rolerow.rolname) || ' '
              || 'NOCREATEEXTTABLE(protocol=''gphdfs'',type=''readable'')';
            EXECUTE 'alter role '
              || quote_ident(rolerow.rolname) || ' '
              || 'NOCREATEEXTTABLE(protocol=''gphdfs'',type=''writable'')';
            RAISE NOTICE 'dropping gphdfs from role % ...', quote_ident(rolerow.rolname);
          END LOOP;
        END;
        \\\$\\\$ LANGUAGE plpgsql;

        SELECT drop_gphdfs();

        DROP FUNCTION drop_gphdfs();

        DROP INDEX retail_demo.order_lineitems_cust_id;
        DROP INDEX retail_parts.order_lineitems_cust_id_1_prt_default_part;
        DROP INDEX retail_parts.order_lineitems_cust_id_1_prt_today;
        DROP INDEX retail_parts.order_lineitems_cust_id_1_prt_2;
        DROP INDEX retail_parts.order_lineitems_cust_id_1_prt_3;
        DROP INDEX retail_parts.order_lineitems_cust_id_1_prt_4;
        DROP INDEX retail_parts.order_lineitems_cust_id_1_prt_5;
        DROP INDEX retail_parts.order_lineitems_cust_id_1_prt_6;
        DROP INDEX retail_parts.order_lineitems_cust_id_1_prt_7;
        DROP INDEX retail_parts.order_lineitems_cust_id_1_prt_8;
        DROP INDEX retail_parts.order_lineitems_cust_id_1_prt_9;
        DROP INDEX retail_parts.order_lineitems_cust_id_1_prt_10;
        DROP INDEX retail_parts.order_lineitems_cust_id_1_prt_11;

        ALTER TABLE retail_demo.order_lineitems SET SCHEMA retail_parts;
        ALTER TABLE retail_demo.shipment_lineitems SET SCHEMA retail_parts;
        ALTER TABLE retail_demo.orders SET SCHEMA retail_parts;
SQL_EOF
"

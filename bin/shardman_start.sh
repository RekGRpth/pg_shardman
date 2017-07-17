#!/bin/bash

script_dir=`dirname "$(readlink -f "$0")"`
source "${script_dir}/common.sh"

cd "${script_dir}/.."
make clean
make install

restart_nodes
for port in $master_port "${worker_ports[@]}"; do
    psql -p $port -c "drop extension if exists pg_shardman;"
    psql -p $port -c "create extension pg_shardman cascade;"
done
# to restart master bgw
restart_nodes

psql -p 5433 -c "drop table if exists partitioned_table cascade;"
psql -p 5433 -c "CREATE TABLE partitioned_table(id INT NOT NULL, payload REAL);"
psql -p 5433 -c "INSERT INTO partitioned_table SELECT generate_series(1, 1000), random();"
psql -c "select shardman.add_node('port=5433');"
psql -c "select shardman.add_node('port=5434');"

psql

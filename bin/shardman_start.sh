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

psql

#!/bin/bash

script_dir=`dirname "$(readlink -f "$0")"`
source "${script_dir}/common.sh"

cd "${script_dir}/.."

> $logfile

restart_nodes # make sure nodes run
# first workers, then lord
for port in "${worker_ports[@]}" $lord_port; do
    psql -p $port -c "drop extension if exists pg_shardman cascade;"
done

make clean
make install

restart_nodes
for port in $lord_port "${worker_ports[@]}"; do
    psql -p $port -c "create extension pg_shardman cascade;"
done

# to restart lord bgw
restart_nodes

run_demo

# psql

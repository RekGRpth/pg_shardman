#!/bin/bash

script_dir=`dirname "$(readlink -f "$0")"`
source "${script_dir}/common.sh"

cd "${script_dir}/.."

if [ -n "$logfile" ]; then
    > $logfile
fi

restart_nodes # make sure nodes run
# first workers, then lord
for port in "${worker_ports[@]}" $lord_port; do
    psql -p $port -c "set synchronous_commit to local; drop extension if exists pg_shardman cascade;"
done

make clean
make install

restart_nodes
for port in $lord_port "${worker_ports[@]}"; do
    psql -p $port -c "set synchronous_commit to local; create extension pg_shardman cascade;"
done

run_demo

#!/bin/bash

script_dir=`dirname "$(readlink -f "$0")"`
source "${script_dir}/common.sh"

if $install_pathman; then
    cd $pathmanpath
    USE_PGXS=1 make clean
    USE_PGXS=1 make install
fi

cd "${script_dir}/.."
make clean
make install

if [ -n "$logfile" ]; then
    > $logfile
fi

stop_nodes
pkill -9 postgres || true
for datadir in $lord_datadir "${worker_datadirs[@]}"; do
    rm -rf "$datadir"
    mkdir -p "$datadir"
    initdb -D "$datadir"
done

send_configs

start_nodes
for port in $lord_port "${worker_ports[@]}"; do
    createdb -p $port `whoami`
    echo "creating pg_shardman extension..."
    psql -p $port -c "create extension pg_shardman cascade;"
done

restart_nodes

run_demo

# psql

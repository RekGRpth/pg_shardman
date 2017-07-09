#!/bin/bash

script_dir=`dirname "$(readlink -f "$0")"`
source "${script_dir}/common.sh"

echo $PATH
cd $pathmanpath
USE_PGXS=1 make install

cd "${script_dir}/.."
make clean
make install

stop_nodes
for datadir in $master_datadir "${worker_datadirs[@]}"; do
    rm -rf "$datadir"
    mkdir -p "$datadir"
    initdb -D "$datadir"
done

cat postgresql.conf.master.template >> ${master_datadir}/postgresql.conf
for worker_datadir in "${worker_datadirs[@]}"; do
    cat postgresql.conf.worker.template >> ${worker_datadir}/postgresql.conf
done

start_nodes
for port in $master_port "${worker_ports[@]}"; do
    createdb -p $port `whoami`
    psql -p $port -c "create extension pg_shardman cascade;"
done

restart_nodes

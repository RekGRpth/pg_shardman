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

stop_nodes
for datadir in $master_datadir "${worker_datadirs[@]}"; do
    rm -rf "$datadir"
    mkdir -p "$datadir"
    initdb -D "$datadir"
done

cat postgresql.conf.common.template >> ${master_datadir}/postgresql.conf
cat postgresql.conf.master.template >> ${master_datadir}/postgresql.conf
for worker_datadir in "${worker_datadirs[@]}"; do
    cat postgresql.conf.common.template >> ${worker_datadir}/postgresql.conf
    cat postgresql.conf.worker.template >> ${worker_datadir}/postgresql.conf
done

start_nodes
for port in $master_port "${worker_ports[@]}"; do
    createdb -p $port `whoami`
    psql -p $port -c "create extension pg_shardman cascade;"
done

restart_nodes

psql -p 5433 -c "drop table if exists partitioned_table cascade;"
psql -p 5433 -c "CREATE TABLE partitioned_table(id INT NOT NULL, payload REAL);"
psql -p 5433 -c "INSERT INTO partitioned_table SELECT generate_series(1, 1000), random();"
psql -c "select shardman.add_node('port=5433');"
psql -c "select shardman.add_node('port=5434');"

psql

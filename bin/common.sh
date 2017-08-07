#!/bin/bash
set -e

#------------------------------------------------------------
# Params

pgpath=~/postgres/install/vanilla/
pathmanpath=~/postgres/pg_pathman
install_pathman=false
logfile=$HOME/tmp/tmp/tmp.log

master_datadir=~/postgres/data1
master_port=5432

# declare -a worker_datadirs=()
# declare -a worker_ports=()

# declare -a worker_datadirs=("${HOME}/postgres/data2")
# declare -a worker_ports=("5433")

# declare -a worker_datadirs=("${HOME}/postgres/data2" "${HOME}/postgres/data3")
# declare -a worker_ports=("5433" "5434")

declare -a worker_datadirs=("${HOME}/postgres/data2" "${HOME}/postgres/data3" "${HOME}/postgres/data4")
declare -a worker_ports=("5433" "5434" "5435")

#------------------------------------------------------------
PATH="$PATH:${pgpath}bin/"
function start_nodes()
{
    echo "Starting nodes"
    for ((i=0; i<${#worker_datadirs[@]}; ++i)); do
	datadir="${worker_datadirs[i]}"
	port="${worker_ports[i]}"
	pg_ctl -o "-p $port" -D $datadir  -l $logfile start
    done
    pg_ctl -o "-p $master_port" -D $master_datadir -l $logfile start
}

function stop_nodes()
{
    echo "Stopping nodes"
    for datadir in $master_datadir "${worker_datadirs[@]}"; do
	pg_ctl -D $datadir stop || true
    done
}

function restart_nodes()
{
    echo "Restarting nodes"
    for ((i=0; i<${#worker_datadirs[@]}; ++i)); do
	datadir="${worker_datadirs[i]}"
	port="${worker_ports[i]}"
	pg_ctl -o "-p $port" -D $datadir -l $logfile restart
    done
    pg_ctl -o "-p $master_port" -D $master_datadir -l $logfile restart
}

function run_demo()
{
    :
    psql -p 5433 -c "drop table if exists pt cascade;"
    psql -p 5433 -c "CREATE TABLE pt(id INT NOT NULL, payload REAL);"
    psql -p 5433 -c "INSERT INTO pt SELECT generate_series(1, 10), random();"
    psql -c "select shardman.add_node('port=5433');"
    psql -c "select shardman.add_node('port=5434');"
    psql -p 5433 -c "drop table if exists pt_0;" # drop replica
    psql -c "select shardman.create_hash_partitions(2, 'pt', 'id', 2);"

    psql -c "select shardman.add_node('port=5435');"
    # psql -c "select shardman.move_primary('pt_0', 4);"
    # psql -c "select shardman.create_replica('pt_0', 2);"
}

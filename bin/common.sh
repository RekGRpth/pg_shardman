#!/bin/bash
set -e

#------------------------------------------------------------
# Params

pgpath=~/postgres/install/vanilla/
pathmanpath=~/postgres/pg_pathman
install_pathman=false

master_datadir=~/postgres/data1
master_port=5432

# declare -a worker_datadirs=()
# declare -a worker_ports=()
# declare -a worker_datadirs=("${HOME}/postgres/data2")
# declare -a worker_ports=("5433")
declare -a worker_datadirs=("${HOME}/postgres/data2" "${HOME}/postgres/data3")
declare -a worker_ports=("5433" "5434")

#------------------------------------------------------------
PATH="$PATH:${pgpath}bin/"
function start_nodes()
{
    echo "Starting nodes"
    for ((i=0; i<${#worker_datadirs[@]}; ++i)); do
	datadir="${worker_datadirs[i]}"
	port="${worker_ports[i]}"
	pg_ctl -o "-p $port" -D $datadir start
    done
    pg_ctl -o "-p $master_port" -D $master_datadir start
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
	pg_ctl -o "-p $port" -D $datadir restart
    done
    pg_ctl -o "-p $master_port" -D $master_datadir restart
}

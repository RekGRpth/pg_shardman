#!/bin/bash
set -e

script_dir=`dirname "$(readlink -f "$0")"`
source "${script_dir}/setup.sh"

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

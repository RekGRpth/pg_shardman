#!/bin/bash
set -e

script_dir=`dirname "$(readlink -f "$0")"`
source "${script_dir}/setup.sh"

logopts=""
if [ -n "$logfile" ]; then
    logopts = "-l $logfile"
fi

PATH="$PATH:${pgpath}bin/"
function start_nodes()
{
    echo "Starting nodes"
    for ((i=0; i<${#worker_datadirs[@]}; ++i)); do
	datadir="${worker_datadirs[i]}"
	port="${worker_ports[i]}"
	pg_ctl -o "-p $port" -D $datadir $logopts start
    done
    pg_ctl -o "-p $lord_port" -D $lord_datadir $logopts start
}

function stop_nodes()
{
    echo "Stopping nodes"
    for datadir in $lord_datadir "${worker_datadirs[@]}"; do
	pg_ctl -D $datadir stop || true
    done
}

function restart_nodes()
{
    echo "Restarting nodes"
    for ((i=0; i<${#worker_datadirs[@]}; ++i)); do
	datadir="${worker_datadirs[i]}"
	port="${worker_ports[i]}"
	pg_ctl -o "-p $port" -D $datadir $logopts restart
    done
    pg_ctl -o "-p $lord_port" -D $lord_datadir $logopts restart
}

function send_configs()
{
    echo "Sending configs"
    cat postgresql.conf.common >> ${lord_datadir}/postgresql.conf
    cat postgresql.conf.lord >> ${lord_datadir}/postgresql.conf
    for worker_datadir in "${worker_datadirs[@]}"; do
	cat postgresql.conf.common >> ${worker_datadir}/postgresql.conf
	cat postgresql.conf.worker >> ${worker_datadir}/postgresql.conf
    done

    # custom conf
    if [ -f bin/postgresql.conf.common ]; then
	cat bin/postgresql.conf.common >> ${lord_datadir}/postgresql.conf
	for worker_datadir in "${worker_datadirs[@]}"; do
	    cat bin/postgresql.conf.common >> ${worker_datadir}/postgresql.conf
	done
    fi

    if [ -f pg_hba.conf ]; then
	for datadir in $lord_datadir "${worker_datadirs[@]}"; do
	    cat pg_hba.conf > ${datadir}/pg_hba.conf
	done
    fi
}

#!/usr/bin/env bash
set -e

app_name=$(psql -qtA -c "select application_name from pg_stat_replication where application_name ~ '.*sub_[0-9]+_[0-9]+.*' limit 1;")

> wal_lag.txt
while true; do
    lag=$(psql -qtA -c "select pg_current_wal_lsn() - (select flush_lsn from pg_stat_replication where application_name = '${app_name}');")
    echo "${lag}" >> wal_lag.txt
    sleep 2s
done

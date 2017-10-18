#!/usr/bin/env python3

import time
import re
import csv
import os
import shutil
from subprocess import check_call, check_output

from inventory_ec2.ec2_elect_shardlord import ec2_elect_shardlord

scale = 10
duration = 30

workers = [3, 6, 9]
# replications = ['none', 'logical_sync', 'logical_async', 'trigger']
replications = ['logical_async']
async_config = '''
synchronous_commit = local
shardman.sync_replicas = off
'''
nparts_per_node = [3, 10]
clients = [1, 16, 32, 64, 128]

# debug
# workers = [3]
# replications = ['logical_async']
# nparts_per_node = [3]
# clients = [8]

create_instances = True
destroy_instances = True
provision = True
rebuild_shardman = True
prepare = True

resfile_path = "tester_res.csv"
resfile_writer = None

class TestRow:
    def __init__(self):
        self.test_id = None
        self.workers = None
        self.replication = None
        self.nparts_per_node = None
        self.clients = None
        self.tps_sum = None
        self.wal_lag = ''

def res_header():
    return ["test_id", "instance_type", "num of workers", "nparts", "replicas",
            "repmode", "sync_replicas", "sync_commit", "CFLAGS", "scale",
            "seconds", "test", "fdw 2pc", "active_workers", "clients", "tps sum",
            "avg latency", "end latency", "wal lag"]

def tester():
    if os.path.isfile(resfile_path):
        shutil.copy(resfile_path, "{}.old".format(resfile_path))
    resfile = open(resfile_path, 'w', newline='', buffering=1)
    global resfile_writer
    resfile_writer = csv.writer(resfile)
    resfile_writer.writerow(res_header())
    if create_instances:
        check_call('ansible-playbook -i inventory_ec2/ ec2.yml --tags "terminate"',
                   shell=True)
    for n in workers:
        try:
            if create_instances:
                check_call('ansible-playbook -i inventory_ec2/ ec2.yml --tags "c3.2xlarge" -e "count={}"'.format(n + 1),
                           shell=True)
                time.sleep(20) ## wait for system to start up
            test_row = TestRow()
            test_row.workers = n
            test_nodes(test_row)
        finally:
            if destroy_instances:
                check_call('ansible-playbook -i inventory_ec2/ ec2.yml --tags "terminate"',
                           shell=True)

def test_nodes(test_row):
    if provision:
        check_call(
            '''
            ansible-playbook -i inventory_ec2/ provision.yml --tags "ars" && \
            ansible-playbook -i inventory_ec2/ provision.yml
            ''', shell=True)
    for rep in replications:
        test_row.replication = rep
        test_repl(test_row)

def test_repl(test_row):
    if rebuild_shardman:
        if (test_row.replication == 'trigger'):
            print("Building tbr shardman")
            check_call(
                '''
                ansible-playbook -i inventory_ec2/ provision.yml -e "shardman_version_tag=tbr-2pc" --tags "build_shardman"
                ''', shell=True)
        else:
            print("Bulding master shardman")
            check_call(
                '''
                ansible-playbook -i inventory_ec2/ provision.yml --tags "build_shardman"
            ''', shell=True)

    check_call('cp postgresql.conf.common.example postgresql.conf.common', shell=True)
    if test_row.replication == 'logical_async':
        with open("postgresql.conf.common", "a") as pgconf:
            pgconf.write(async_config)

    for nparts in nparts_per_node:
        test_row.nparts_per_node = nparts
        test_nparts(test_row)

def test_nparts(test_row):
    if prepare:
        print("Preparing data for {}".format(test_row.__dict__))
        repfactor = 0 if test_row.replication == 'none' else 1
        check_call(
            '''
            ansible-playbook -i inventory_ec2/ pgbench_prepare.yml \
            -e "scale={} nparts={} repfactor={} rebalance=true tellers_branches=false"
            '''.format(scale, test_row.workers * test_row.nparts_per_node, repfactor), shell=True)
    for c in clients:
        test_row.clients = c
        run(test_row)

def run(test_row):
    print("Running {}".format(test_row.__dict__))

    # monitor wal lag if we test async logical replication
    if test_row.replication == 'logical_async':
        inventory = ec2_elect_shardlord()
        mon_node = "ubuntu@{}".format(inventory['ec2_workers'][0])
        print("mon_node is {}".format(mon_node))
        check_call('scp monitor_wal_lag.sh {}:'.format(mon_node), shell=True)
        check_call(
            '''
            ssh {} 'nohup /home/ubuntu/monitor_wal_lag.sh > monitor_wal_lag.out 2>&1 &'
            '''.format(mon_node), shell=True)

    run_output = check_output(
        '''
        ansible-playbook -i inventory_ec2/ pgbench_run.yml -e \
        'tmstmp=true tname=t pgbench_opts="-N -c {} -T {}"'
        '''.format(test_row.clients, duration), shell=True).decode("ascii")

    # stop wal lag monitoring and pull the results
    if test_row.replication == 'logical_async':
        check_call('ssh {} pkill -f -SIGTERM monitor_wal_lag.sh'.format(mon_node), shell=True)
        check_call('scp {}:wal_lag.txt .'.format(mon_node), shell=True)
        max_lag_bytes = int(check_output(
            "awk -v max=0 '{if ($1 > max) {max=$1} }END {print max}' wal_lag.txt",
            shell=True))
        test_row.wal_lag = size_pretty(max_lag_bytes)


    print('Here is run output:')
    print(run_output)
    test_id_re = re.compile('test_id is ([\w-]+)')
    test_id = test_id_re.search(run_output).group(1)
    print('test id is {}'.format(test_id))
    test_row.test_id = test_id
    tps_sum = int(float(check_output('tail -1 res/{}/tps.txt | xargs echo -n'.format(test_id),
                               shell=True).decode("ascii")))
    print('tps sum is {}'.format(tps_sum))
    test_row.tps_sum = tps_sum
    resfile_writer.writerow(form_csv_row(test_row))


def form_csv_row(test_row):
    if test_row.replication == 'none':
        replicas = 0
    else:
        replicas = 1
    repmode = ''
    if test_row.replication.startswith('logical'):
        repmode = 'logical'
    if test_row.replication == 'trigger':
        repmode = 'trigger'

    sync_replicas = ''
    if test_row.replication == 'logical_sync':
        sync_replicas = 'on'
    if test_row.replication == 'logical_async':
        sync_replicas = 'off'

    sync_commit = 'on'
    if test_row.replication == 'logical_async':
        sync_commit = 'local'

    return [test_row.test_id, 'c3.2xlarge', test_row.workers, test_row.workers * test_row.nparts_per_node,
            replicas, repmode, sync_replicas, sync_commit, "-O2", scale, duration,
            "pgbench -N", "on", test_row.workers, test_row.clients, test_row.tps_sum,
            '', '', test_row.wal_lag]

def size_pretty(size, precision=2):
    suffixes = ['B','KB','MB','GB','TB']
    suffixIndex = 0
    while size > 1024 and suffixIndex < 4:
        suffixIndex += 1
        size = size / 1024.0
    return "%.*f%s" % (precision, size, suffixes[suffixIndex])

if __name__ == '__main__':
    tester()

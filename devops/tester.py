#!/usr/bin/env python3

import time
import re
import csv
import os
import shutil
import subprocess
import json
import copy
from sys import argv
from itertools import product
from subprocess import check_call, check_output

from inventory_ec2.ec2_elect_shardlord import ec2_elect_shardlord

async_config = '''
synchronous_commit = local
shardman.sync_replication = off
'''

create_instances = True
destroy_instances = True
provision = True
prepare = True


# debug
# create_instances = False
# destroy_instances = False
# provision = False

resfile_path = "tester_res.csv"
resfile_writer = None

class TestRow:
    def __init__(self):
        self.instance_type = None
        self.nodes = None

        self.sync_replication = None
        self.global_snapshots_and_2pc = None
        self.nparts_per_node = None
        self.redundancy = None
        self.scale = None
        self.use_pgbouncer = None
        self.pgbouncer_pool_mode = ''
        self.pgbouncer_pool_size = ''

        self.duration = None
        self.test = None
        self.clients = None
        self.active_nodes = None

        self.nparts = None
        self.nodes_in_repgroup = None
        self.test_id = None
        self.tps_sum = None
        self.wal_lag = ''

def res_header():
    return ["instance_type", "nodes", "sync_replication",
            "sync_commit", "global_snapshots_and_2pc", "nparts", "nodes_in_repgroup",
            "redundancy", "scale", "use_pgbouncer", "pgbouncer_pool_mode",
            "pgbouncer_pool_size",
            "duration", "test", "active_nodes", "clients", "tps_sum",
            "avg_lat", "end_lat", "wal_lag", "test_id", "comment"]

def form_csv_row(test_row):
    sync_commit = 'on'
    if not test_row.sync_replication:
        sync_commit = 'local'

    return [test_row.instance_type, test_row.nodes, test_row.sync_replication,
            sync_commit, test_row.global_snapshots_and_2pc, test_row.nparts,
            test_row.nodes_in_repgroup, test_row.redundancy, test_row.scale,
            test_row.use_pgbouncer, test_row.pgbouncer_pool_mode,
            test_row.pgbouncer_pool_size,
            test_row.duration, test_row.test,
            test_row.active_nodes, test_row.clients,
            test_row.tps_sum, '', '', test_row.wal_lag, test_row.test_id, '']

def test_conf(conf):
    try:
        test_row = TestRow()
        for test_row.instance_type, test_row.nodes in product(conf['instance_type'],
                                                              conf['nodes']):
            # To have clean object on next iteration
            test_row_dup = copy.copy(test_row)
            if create_instances:
                check_call('ansible-playbook -i inventory_ec2/ ec2.yml --tags "terminate"',
                           shell=True)
                check_call('''
                ansible-playbook -i inventory_ec2/ ec2.yml --tags "launch" \
                -e "instance_type={} count={}"
                '''.format(test_row.instance_type, test_row.nodes + 1),
                           shell=True)
                time.sleep(20) ## wait for system to start up
            test_nodes(conf, test_row_dup)
    finally:
        if destroy_instances:
            check_call('ansible-playbook -i inventory_ec2/ ec2.yml --tags "terminate"',
                       shell=True)

def test_nodes(conf, test_row):
    if provision:
        check_call(
            '''
            ansible-playbook -i inventory_ec2/ provision.yml --tags "ars" && \
            ansible-playbook -i inventory_ec2/ provision.yml
            ''', shell=True)
    for test_row.sync_replication, test_row.global_snapshots_and_2pc, test_row.nparts_per_node, test_row.redundancy, test_row.scale, test_row.use_pgbouncer in product(conf['sync_replication'], conf['global_snapshots_and_2pc'], conf['nparts_per_node'], conf['redundancy'], conf['scale'], conf['use_pgbouncer']):
        test_row_dup = copy.copy(test_row)
        test_pgbench(conf, test_row_dup)

def test_pgbench(conf, test_row):
    check_call('cp postgresql.conf.common.example postgresql.conf.common', shell=True)
    if not test_row.sync_replication:
        with open("postgresql.conf.common", "a") as pgconf:
            pgconf.write(async_config)

    if not test_row.global_snapshots_and_2pc:
        with open("postgresql.conf.common", "a") as pgconf:
            pgconf.write('track_global_snapshots = false\n'
                         'global_snapshot_defer_time = 0\n'
                         'postgres_fdw.use_tsdtm = false\n'
                         'postgres_fdw.use_repeatable_read = off\n')

    test_row.nparts = test_row.nodes * test_row.nparts_per_node
    if test_row.redundancy == 0:
        test_row.sync_replication = ''
    test_row.nodes_in_repgroup = test_row.redundancy + 1

    if test_row.use_pgbouncer and (conf.get('pgbouncer_pool_mode') is not None):
        test_row.pgbouncer_pool_mode = conf['pgbouncer_pool_mode']
        test_row.pgbouncer_pool_size = conf['pgbouncer_pool_size']

    if prepare:
        print("Preparing pgbench for {}".format(test_row.__dict__))
        cmd = '''
        ansible-playbook -i inventory_ec2/ pgbench_prepare.yml \
        -e "scale={} nparts={} redundancy={} nodes_in_repgroup={} use_pgbouncer={} pgbouncer_pool_mode={} pgbouncer_pool_size={}"
        '''.format(test_row.scale, test_row.nparts, test_row.redundancy,
                       test_row.nodes_in_repgroup, test_row.use_pgbouncer,
                       test_row.pgbouncer_pool_mode, test_row.pgbouncer_pool_size)
        print("Executing {}".format(cmd))
        check_call(cmd, shell=True)
        if test_row.sync_replication:
            print("Wait a bit to let replicas arrive and copied data sync")
            time.sleep(20)

    for test_row.duration, test_row.test, test_row.active_nodes, test_row.clients in product(conf['duration'], conf['test'], conf['active_nodes'], conf['clients']):
        test_row_dup = copy.copy(test_row)
        run(conf, test_row_dup)

def run(conf, test_row):
    print("Running test {}".format(test_row.__dict__))

    if test_row.active_nodes == 'all':
        test_row.active_nodes = test_row.nodes
    pgbench_opts = ''
    if test_row.test == 'pgbench -N':
        pgbench_opts = '-N'
    elif test_row.test == 'pgbench -S':
        pgbench_opts = '-S'
    elif test_row.test == 'pgbench full':
        pgbench_opts = ''
    else:
        raise ValueError('Wrong "test" {}'.format(test_row.test))

    # monitor wal lag if we test async logical replication
    if test_row.sync_replication == False:
        inventory = ec2_elect_shardlord()
        mon_node = "ubuntu@{}".format(inventory['ec2_workers'][0])
        print("mon_node is {}".format(mon_node))
        check_call('scp monitor_wal_lag.sh {}:'.format(mon_node), shell=True)
        check_call(
            '''
            ssh {} 'nohup /home/ubuntu/monitor_wal_lag.sh > monitor_wal_lag.out 2>&1 &'
            '''.format(mon_node), shell=True)

    try:
        run_output = check_output(
            '''
            ansible-playbook -i inventory_ec2/ pgbench_run.yml -e \
            'tmstmp=true tname=t active_workers={} pgbench_opts="{} -c {} -T {}"'
            '''.format(test_row.active_nodes, pgbench_opts, test_row.clients,
                       test_row.duration), shell=True,
            stderr=subprocess.STDOUT).decode("ascii")
    except subprocess.CalledProcessError as e:
        print('pgbench_run failed, stdout and stderr was:')
        print(e.output.decode('ascii'))
        raise e

    print('Here is run output:')
    print(run_output)

    # stop wal lag monitoring and pull the results
    if test_row.sync_replication == False:
        check_call('ssh {} pkill -f -SIGTERM monitor_wal_lag.sh'
                   .format(mon_node), shell=True)
        check_call('scp {}:wal_lag.txt .'.format(mon_node), shell=True)
        max_lag_bytes = int(check_output(
            "awk -v max=0 '{if ($1 > max) {max=$1} }END {print max}' wal_lag.txt",
            shell=True))
        test_row.wal_lag = max_lag_bytes
    else:
        test_row.wal_lag = ''

    test_id_re = re.compile('test_id is ([\w-]+)')
    test_id = test_id_re.search(run_output).group(1)
    print('test id is {}'.format(test_id))
    test_row.test_id = test_id
    tps_sum = int(float(check_output('tail -1 res/{}/tps.txt | xargs echo -n'.
                                     format(test_id),
                                     shell=True).decode("ascii")))
    print('tps sum is {}'.format(tps_sum))
    test_row.tps_sum = tps_sum
    resfile_writer.writerow(form_csv_row(test_row))

if __name__ == '__main__':
    if os.path.isfile(resfile_path):
        shutil.copy(resfile_path, "{}.old".format(resfile_path))
    resfile = open(resfile_path, 'w', newline='', buffering=1)
    resfile_writer = csv.writer(resfile)
    resfile_writer.writerow(res_header())

    if len(argv) > 1:
        conf_file_path = argv[1]
    else:
        conf_file_path = 'tester_conf.json'
    with open(conf_file_path) as conf_file:
        confs = json.load(conf_file)
        for conf in confs:
            print('Running conf {}'.format(conf))
            test_conf(conf)

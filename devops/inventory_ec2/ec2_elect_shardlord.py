#!/usr/bin/env python3

'''
Run ec2.py stored in .. directory and append ec2_shardlord, ec2_workers and
ec2_init_node groups to it.
'''

import os
import subprocess
import json
import socket

def ec2_elect_shardlord():
    script_dir = os.path.dirname(os.path.realpath(__file__))
    ec2_path = os.path.join(os.path.dirname(script_dir), "ec2.py")
    ec2_inv_txt = subprocess.check_output([ec2_path, "--list"]).decode('ascii')
    ec2_inv = json.loads(ec2_inv_txt)
    hosts = list(ec2_inv["_meta"]["hostvars"].keys())
    # sort to choose the same shardlord each time
    hosts.sort(key = socket.inet_aton)
    shardlord, workers, init_node = hosts[0:1], hosts[1:], hosts[1:2]
    ec2_inv["ec2_shardlord"] = shardlord
    ec2_inv["ec2_workers"] = workers
    ec2_inv["ec2_init_node"] = init_node
    return ec2_inv

if __name__ == '__main__':
    print(json.dumps(ec2_elect_shardlord(), sort_keys=True, indent=2))

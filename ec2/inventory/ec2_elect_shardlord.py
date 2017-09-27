#!/usr/bin/env python

'''
Run ec2.py stored in .. directory and appends ec2_shardlord ec2_workers groups
to it.
'''

import os
import subprocess
import json
import socket

def ec2_elect_shardlord():
    script_dir = os.path.dirname(os.path.realpath(__file__))
    ec2_path = os.path.join(os.path.dirname(script_dir), "ec2.py")
    ec2_inv_txt = subprocess.check_output([ec2_path, "--list"])
    ec2_inv = json.loads(ec2_inv_txt)
    hosts = list(ec2_inv["_meta"]["hostvars"].keys())
    # sort to choose the same shardlord each time
    hosts.sort(key = socket.inet_aton)
    shardlord, workers = [hosts[0]], hosts[1:]
    ec2_inv["ec2_shardlord"] = shardlord
    ec2_inv["ec2_workers"] = workers
    print json.dumps(ec2_inv, sort_keys=True, indent=2)


if __name__ == '__main__':
    # Run the script
    ec2_elect_shardlord()

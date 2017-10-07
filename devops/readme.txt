I used ansible 2.5.0.

This stuff allows to deploy and configure pg_shardman on ec2 nodes or on
manually hardcoded ones, inventories are inventory_ec2 and inventory_manual.

For manual setup, type nodes to inventory_manual/manual file, see manual.example
file there and comments in ./inventory. You can also check out Vagrantfile I
used for testing this.

EC2 setup:
Download aws ssh key in needed region
chmod 0400 ~/.ssh/aws_rsa

install python packages for ec2.py: (venv is recommended):
python3 -m venv ~/venv/aws
As of writing this, python 3.5 has a nasty bug, so better use python 2:
virtualenv -p /usr/bin/python ~/venv/aws_2.7
source ~/venv/aws_2.7/bin/activate
pip install boto boto3 six ansible

Download and set up ec2.ini:
sudo wget https://raw.githubusercontent.com/ansible/ansible/devel/contrib/inventory/ec2.ini
rds = False # we don't need rds
regions = eu-central-1 # set region
set aws_access_key_id = xxx
set aws_secret_access_key = xxx
export EC2_INI_PATH=/etc/ansible/ec2.ini

Now launch some instances and check that ec2.py works:
inventory_ec2/ec2_elect_shardlord.py --list

Check that ansible works:
ansible -i inventory_ec2 -m ping all
For manually configured nodes:
ansible -i inventory_manual -m ping all
Now you are ready to go.

Short usage guide & Ansible cheatsheet:

Build only PG on ec2:
ansible-playbook -i inventory_ec2 --tags "build_pg"
Same on hardcoded nodes:
ansible-playbook -i inventory_manual --tags "build_pg"
Provision only the second node:
ansible-playbook -i inventory_ec2 provision.yml --limit nodes[1]
Build PG, but don't remove src and build with O0:
ansible-playbook -i inventory_ec2/ provision.yml --skip-tags "rm_pg_src" -e "cflags='-O0'"

Reload configs:
ansible-playbook -i inventory_ec2/ send_config.yml
Restart nodes:
ansible-playbook -i inventory_ec2 pg_ctl.yml -e "pg_ctl_action=restart"

Read cmd log on shardlord:
ansible-playbook -i inventory_ec2/ psql.yml --limit 'shardlord' -e "cmd='\'table shardman.cmd_log\''"
Read nodes table on workers:
nodes': ansible-playbook -i inventory_ec2/ psql.yml --limit 'workers' -e "cmd='\'table shardman.nodes\''"

Create, fill and shard pgbench tables:
ansible-playbook -i inventory_ec2/ pgbench_prepare.yml -e "scale=10 nparts=3 repfactor=0"
Run pgbench test:
ansible-playbook -i inventory_ec2/ pgbench_run.yml -e 'tmstmp=false tname=t pgbench_opts="-c 1 -T 5"'
Run pgbench on single worker (to estimate shardman overhead):
ansible-playbook -i inventory_ec2/ pgbench_single.yml -e "scale=10 tmstmp=false tname=test t=false clients=8 seconds=15"

Gather logs to ./logs:
ansible-playbook -i inventory_ec2/ logs.yml

Start micro instances:
ansible-playbook -i inventory_ec2/ ec2.yml --tags "micro" -e "count=2"
Terminate all ec2 instances:
ansible-playbook -i inventory_ec2/ ec2.yml --tags "terminate"

Ubuntu images EC2 locator:
https://cloud-images.ubuntu.com/locator/ec2/
We need ami-4199282e.

Other hints:
Currently pgbench exits on first error. postgres_fdw currently supports only
repeatable read / serializable isolation levels which immediately leads to
serialization errors, so you should use either patched postgres_fdw or pgbench.

If you don't want to measure the dist performance, keep data dir on tmpfs or
turn of fsync, the effect it similar.

Things that made me wonder during writing this:
* Reusability of tasks. Playbooks, files with tasks, roles, includes, imports,
  old include API...probably working directly from Python would be easier? There
  seems to be no way to run only one play from playbook, and the only way to run
  several tasks from play is to use tags, which is a bit ugly. This is the reason
  why projects like
  https://github.com/larsks/ansible-toolbox
  emerge. Printing cmd output via saved var is pretty horrible too.
* They also try to reinvent all the concepts of normal languages in YML. How
  about infinite repeat..until?
* No way to specify multiple inventory files, only whole directory or one file.
* No way to append file to file, except for j2 templates, which doesn't work
  with files outside the template dir, really?
* Writing yml files makes me nervous.

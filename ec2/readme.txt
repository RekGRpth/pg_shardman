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

Now launch some instance and check that ec2.py works:
inventory/ec2.py --list

Check that ansible works:
ansible -i inventory/  -m ping all

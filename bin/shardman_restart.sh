#!/bin/bash

script_dir=`dirname "$(readlink -f "$0")"`
source "${script_dir}/common.sh"

restart_nodes

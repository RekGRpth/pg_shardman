#!/bin/bash

kill -SIGUSR1 `ps aux | grep 'postgres: bgworker: shardlord' | grep -v 'grep' | awk '{print $2}'`

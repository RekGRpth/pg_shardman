#!/bin/bash

kill -SIGUSR1 `ps aux | grep 'postgres: bgworker: shardmaster' | grep -v 'grep' | awk '{print $2}'`

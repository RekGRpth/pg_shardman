#!/bin/sh

DIR=$(dirname $0)

yapf -i "$DIR"/*.py
flake8 "$DIR"

# we need to cd into dir with tests.py, otherwise unittest won't find it
# dir containing this makefile
mkfile_dir := $(shell dirname $(shell readlink -f $(abspath $(lastword $(MAKEFILE_LIST)))))

all:
	cd $(mkfile_dir) && python3 tests.py

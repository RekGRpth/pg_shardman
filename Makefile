# the extension name
EXTENSION = pg_shardman
EXTVERSION = 0.0.2
# This file will be executed by CREATE EXTENSION, so let pgxs install it.
DATA = $(EXTENSION)--$(EXTVERSION).sql

REGRESS = shardman_installation

MODULE_big = pg_shardman
OBJS = pg_shardman.o
PGFILEDESC = "pg_shardman - sharding for Postgres"

mkfile_path := $(abspath $(lastword $(MAKEFILE_LIST))) # abs path to this makefile
mkfile_dir := $(shell basename $(shell dirname $(dir $(mkfile_path)))) # parent dir of the project
ifndef USE_PGXS # hmm, user didn't requested to use pgxs
ifneq ($(strip $(mkfile_dir)),contrib) # a-ha, but we are not inside 'contrib' dir
USE_PGXS := 1 # so use it anyway, most probably that's what the user wants
endif
endif
$(info $$USE_PGXS is [${USE_PGXS}] (we use it automatically if not in contrib dir))

ifdef USE_PGXS # use pgxs
# You can specify path to pg_config in PG_CONFIG var
ifndef PG_CONFIG
	PG_CONFIG := pg_config
endif
PG_CONFIG = pg_config
INCLUDEDIR := $(shell $(PG_CONFIG) --includedir)
PG_CPPFLAGS += -I$(INCLUDEDIR) # add server's include directory for libpq-fe.h
SHLIB_LINK += -lpq # add libpq
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

else # assume the extension is in contrib/ dir of pg distribution
PG_CPPFLAGS = -I$(libpq_srcdir) # include libpq-fe, defined in Makefile.global.in
SHLIB_LINK = $(libpq) # defined in Makefile.global.in
SHLIB_PREREQS = submake-libpq
subdir = contrib/pg_shardman
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

python_tests:
	$(MAKE) -C tests/python

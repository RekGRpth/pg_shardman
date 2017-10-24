# the extension name
EXTENSION = pg_shardman
EXTVERSION = 1.0
# This file will be executed by CREATE EXTENSION, so let pgxs install it.
DATA = $(EXTENSION)--$(EXTVERSION).sql

MODULE_big = pg_shardman
OBJS = pg_shardman.o
PGFILEDESC = "pg_shardman - sharding for Postgres"

ifdef USE_SOURCETREE # assume the extension is in contrib/ dir of pg distribution
PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK = $(libpq)
SHLIB_PREREQS = submake-libpq
subdir = contrib/pg_shardman
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
else # use pgxs
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
endif

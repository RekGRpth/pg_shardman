# contrib/pg_shardman/Makefile

# the extension name
EXTENSION = pg_shardman
# This file will be executed by CREATE EXTENSION, so let pgxs install it.
DATA = pg_shardman--1.0.sql

MODULE_big = pg_shardman
OBJS = pg_shardman.o
PGFILEDESC = "pg_shardman - sharding for Postgres"

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK = $(libpq)

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
SHLIB_PREREQS = submake-libpq
subdir = contrib/pg_shardman
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

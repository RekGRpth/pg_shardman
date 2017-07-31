# the extension name
EXTENSION = pg_shardman
EXTVERSION = 0.0.1
# This file will be executed by CREATE EXTENSION, so let pgxs install it.
DATA_built = $(EXTENSION)--$(EXTVERSION).sql


MODULE_big = pg_shardman
OBJS = src/pg_shardman.o src/udf.o src/shard.o src/timeutils.o

PG_CPPFLAGS += -Isrc/include

# You can specify path to pg_config in PG_CONFIG var
ifndef PG_CONFIG
	PG_CONFIG := pg_config
endif
PGXS := $(shell $(PG_CONFIG) --pgxs)

INCLUDEDIR := $(shell $(PG_CONFIG) --includedir)
PG_CPPFLAGS += -I$(INCLUDEDIR) # add server's include directory for libpq-fe.h
SHLIB_LINK += -lpq # add libpq

EXTRA_CLEAN = $(EXTENSION)--$(EXTVERSION).sql
$(info $$var is [${EXTRA_CLEAN}])

include $(PGXS)

# Build big sql file from pieces
$(EXTENSION)--$(EXTVERSION).sql: init.sql membership.sql shard.sql
	cat $^ > $@

EXTENSION = pg_shardman        # the extension name
DATA = pg_shardman--0.0.1.sql  # script files to install with CREATE EXTENSION

MODULE_big = pg_shardman
OBJS = src/pg_shardman.o

PG_CPPFLAGS += -Isrc/include

# You can specify path to pg_config in PG_CONFIG var
ifndef PG_CONFIG
	PG_CONFIG := pg_config
endif
PGXS := $(shell $(PG_CONFIG) --pgxs)

INCLUDEDIR := $(shell $(PG_CONFIG) --includedir)
PG_CPPFLAGS += -I$(INCLUDEDIR) # add server's include directory for libpq-fe.h
SHLIB_LINK += -lpq # add libpq

include $(PGXS)

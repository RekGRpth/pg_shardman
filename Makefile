EXTENSION = pg_shardman        # the extension name
DATA = pg_shardman--0.0.1.sql  # script files to install with CREATE EXTENSION

# You can specify path to pg_config in PG_CONFIG var
ifndef PG_CONFIG
	PG_CONFIG := pg_config
endif
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

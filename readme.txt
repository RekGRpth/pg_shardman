How to build:
PostgreSQL location is derived from pg_config, you can also specify path to it
in PG_CONFIG var.

git clone
cd pg_shardman
make
make install

add to postgresql.conf
shared_preload_libraries = '$libdir/pg_shardman'

restart postgres server and run
drop extension if exists pg_shardman;
create extension pg_shardman;

The master itself can't be worker node for now, because it requires special
handling of LR channels setup.

ALTER TABLE for sharded tables is not supported for now.

# pg_shardman: PostgreSQL sharding built on pg_pathman, postgres_fdw and logical replication.

`pg_shardman` is PG 10 extenstion which aims (but not yet fully reaches) for
scalability and high availability with decent transactions, oriented on mainly
OLTP workload. It allows to hash-shard tables using `pg_pathman` and move them
across nodes, balancing read/write load. You can issue queries to any node,
`postgres_fdw` is responsible for redirecting them to the proper one. To avoid
data loss, we support replication of partitions via synchronous or asynchronous
logical replication (LR), redundancy level is configurable. While `pg_shardman`
can be used with vanilla PostgreSQL 10, some features require patched core. Most
importantly, to support sane cross-node transactions, we use patched
`postgres_fdw` with 2PC for atomicity and distributed snapshot manager providing
`snapshot isolation` level of xact isolation. We support current
version
[here](https://github.com/postgrespro/postgres_cluster/tree/pg_shardman).

To manage this zoo, we need one designated PG node which we call
*shardlord*. This node accepts sharding commands from the user and makes sure
the whole cluster changes its state as desired. Shardlord keeps several tables
(see below) forming cluster metadata -- which nodes are in cluster and which
partitions they keep. Currently shardlord can't keep usual data itself.

Some terminology:
  * 'commands' is what constitutes shardman interface: functions for sharding
     management.
  * 'shardlord' or 'lord' is postgres instance which manages sharding.
  * 'worker nodes' or 'workers' are other nodes with data.
  * 'sharded table' is table managed by shardman.
  * 'shard' or 'partition' is any table containing part of sharded table.
  * 'primary' is main partition of sharded table, i.e. the only writable
     partition.
  * 'replica' is secondary partition of sharded table, i.e. read-only partition.
  * 'cluster' -- either the whole system of shardlord and workers, or cluster in
	 traditional PostgreSQL sense, this should be clear from the context.

## Installation and configuration

For quick example setup, see scripts in `bin/` directory. Setup is configured in
file `setup.sh` which needs to be placed in the same directory; see
setup.sh.example for example. `shardman_init.sh` performs initdb for shardlord &
workers, deploys example configs and creates extension; shardman_start.sh
reinstalls extension, which is useful for development. Besides, `devops/` dir
contains a bunch of scripts we used for deploying and testing `pg_shardman` on
ec2 which might be helpful too.

Both shardlord and workers require extension built and installed. We depend on
`pg_pathman` extension so it must be installed too. PostgreSQL location for
building is derived from `pg_config` by default, you can also specify path to it
in `PG_CONFIG` environment variable. PostgreSQL 10 (`REL_10_STABLE branch`) is
required. Extension links with libpq, so if you install PG from packages, you
should install libpq-dev package. The whole process of building and copying
files to PG server is just:

```shell
git clone https://github.com/postgrespro/pg_shardman
cd pg_shardman
make install
```

To actually install extension, add `pg_pathman` to `shared_preload_libraries`
(see
[pg_pathman's docs](http://github.com/postgrespro/pg_pathman), restart the
server and run

```plpgsql
create extension pg_shardman cascade;
```

Have a look at `postgresql.conf.common`, `postgresql.conf.lord` and
`postgresql.conf.worker` example configuration files. The first contains all
shardman's and important PostgreSQL GUCs for either shardlord and workers. The
second and the third show GUCs you should care for on shardlord and worker nodes
accordingly. Basic configuration:
  * `shardman.shardlord` must be `on` on shardlord and `off` on worker;
  * `shardman.shardlord_connstring` must be configured everywhere for now --
	on workers for cmd redirection, on shardlord for connecting to itself;
  * If `shardman.sync_replication` is `on`, shardlord configures sync replicas
    async otherwise.
  * Logical replication settings, see `postgresql.conf.worker`

Currently extension scheme is fixed, it is, who would have thought, 'shardman'.

## Usage

All cluster-management commands are executed on shardlord, thery are usual
PostgreSQL functions in `shardman` schema. If such command is called on worker
node, we will try to redirect it to the shardlord, but this is not yet fully
supported.

If shardlord has failed during command execution or you just feel that something
goes wrong, run `shadman.recovery()` function. It will verify nodes state
against current shardlord's metadata and try to fix up things, e.g. reconfigure
LR channels, repair FDW, etc.

We have concept of *replication groups* (RG) for replication. Replication group
is a set of nodes which can create replicas on each other. Every node in the
cluster belongs to some replication group and RGs are not intersect. Node can't
replicate partitions to nodes not from its RG. We have this concept because
  * Currently logical replication in PG is relatively slow if there are many
  walsenders on the node. Replication groups limit number of nodes where
  replicas can be located. For example, if we have RG from 3 nodes A, B, C,
  node A keeps 10 partitions and have one replica for each partition, all these
  10 replicas will be located on nodes B and C, so only 2 walsenders are spinning
  on A.
  * It provides logical structuring of replication. For instance, we can
  configure each node from replication group to be in separate rack for safety,
  or, on the contrary, to be close to each other for faster replication.
In general, you should have number of nodes in each replication group equal to
redundancy level + 1, or a bit more to be able to increase redundancy later.
It means 1 (no redundancy) - 3 (redundancy 2) nodes in replication group in
practice.

Let's get to the actual commands.

### Membership

```plpgsql
add_node(super_conn_string text, conn_string text = NULL, repl_group text = 'default')
```
Add node with given libpq connstring(s) to the cluster. Node is assigned unique
id. If node previously contained shardman state from old cluster (not one
managed by current shardlord), this state will be lost.

`super_conn_string` is connection string to the node which must provide
superuser access to the node, and `conn_string` can be some other connstring. The
former is used for configuring logical replication, the latter for DDL and for
setting up FDW. This separation serves two purposes:
  * It allows to access data without requiring superuser privileges.
  * It allows to set up pgbouncer, as replication can't go through it.
If `conn_string` is `NULL`, `super_conn_string` is used everywhere.

`repl_group` is the name of node's replication group. We have no explicit
command for changing RG of already added node -- you have to remove it and add
again.

We don't move any parts and replicas to newly added node, see `rebalance_*`
commands for that below. However, freshly added node instantly becomes aware
of sharded tables and can accept queries to them.

```plpgsql
get_my_id()
```
Get this node's id. Executed on any node.

```plpgsql
rm_node(rm_node_id int, force bool = false)
```
Remove node from the cluster. If 'force' is true, we don't care whether node
contains any partitions. Otherwise we won't allow to rm node holding shards.
We will try to remove shardman's stuff (pubs, subs, repslots) on deleted node if
node is alive, but the command succeeds even if we can't. Currently we don't
remove tables with data on removed node.

If node contained partitions, for each one we automatically promote random
replica. NOTE: currently promotion procedure is not safe if there are more than
1 replica because we don't handle different state of replicas.

You can see all cluster nodes on shardlord by examining `shardman.nodes` table:
```plpgsql
CREATE TABLE nodes (
	id serial PRIMARY KEY,
	system_id bigint NOT NULL,
    super_connection_string text UNIQUE NOT NULL,
	connection_string text UNIQUE NOT NULL,
	replication_group text NOT NULL -- group of nodes within which shard replicas are allocated
);
```

### Shards and replicas

```plpgsql
create_hash_partitions(rel regclass, expr text, part_count int, redundancy int = 0)
```
To shard the table, you must create it on shardlord with usual
`CREATE TABLE ...` and then call this function. It hash-shards table `rel` by
key `expr`, creating `part_count` shards, distributing shards evenly among the
nodes. As you probably noticed, the signature mirrors
pathman's function with the same name. `redundancy` replicas will be immediately
created for each partition, evenly scattered among the nodes.

There are three tables describing sharded tables (no pun intended) state,
`shardman.tables`, `shardman.partitions` and `shardman.replicas`:
```plpgsql
-- List of sharded tables
CREATE TABLE tables (
	relation text PRIMARY KEY,     -- table name
	sharding_key text,             -- expression by which table is sharded
	master_node integer REFERENCES nodes(id) ON DELETE CASCADE,
	partitions_count int,          -- number of partitions
	create_sql text NOT NULL,      -- sql to create the table
	create_rules_sql text          -- sql to create rules for shared table
);

-- Main partitions
CREATE TABLE partitions (
	part_name text PRIMARY KEY,
	node_id int NOT NULL REFERENCES nodes(id) ON DELETE CASCADE, -- node on which partition lies
	relation text NOT NULL REFERENCES tables(relation) ON DELETE CASCADE
);

-- Partition replicas
CREATE TABLE replicas (
	part_name text NOT NULL REFERENCES partitions(part_name) ON DELETE CASCADE,
	node_id int NOT NULL REFERENCES nodes(id) ON DELETE CASCADE, -- node on which partition lies
	relation text NOT NULL REFERENCES tables(relation) ON DELETE CASCADE,
	PRIMARY KEY (part_name,node_id)
);
```

```plpgsql
rm_table(rel regclass)
```
Drop sharded table. Removes data everywhere.

```plpgsql
set_redundancy(rel regclass, redundancy int)
```
Create replicas for parts of sharded table `rel` until each shard has
`redundancy` replicas. If existing level of redundancy is greater than specified,
then currently this function does nothing. Note that this function only
starts replication, it doesn't wait for full initial data copy.
See the next function.

```plpgsql
ensure_redundancy()
```
Wait completion of initial table sync for all replication subscriptions. This
function can be used after `set_redundancy` to ensure that partitions are copied
to replicas.

```plgpsql
rebalance(table_pattern text = '%')
rebalance_replicas(table_pattern text = '%')
```
Rebalance parts/replicas between nodes. These functions try to evenly
redistribute partitions (or replicas of partitions) of tables which names match
`LIKE 'pattern'` between all nodes of replication groups, so they are should
be called after nodes addition/removal. It can't move parts/replicas
between replication groups. Parts/replicas are moved sequentially to minimize
influence on system performance. Thanks to logical replication, you can continue
writing to the table during moving.

```plpgsql
mv_partition(mv_part_name text, dst_node_id int)
mv_replica(mv_part_name text, src_node_id int, dst_node_id int)
```
Move single partition/replica to other node. This function is able to move
parts/replicas only within replication group. Such fine-grained control is
rarely needed -- see `rebalance_*` functions.


```plpgsql
recovery()
```
Check consistency of cluster state against current metadata and perform recovery,
if needed (reconfigure LR channels, repair FDW, etc).

## Importing data
Vanilla PostgreSQL doesn't support COPY FROM to foreign tables. To make it work,
for now we need
  * [patched postgres](https://github.com/postgrespro/postgres_cluster/tree/pg_shardman)
  * [and patched pg_pathman](https://github.com/arssher/pg_pathman/tree/foreign_copy_from)

## Transactions
When using vanilla PostgreSQL, local changes are handled by PostgreSQL as usual
-- so if you queries touch only only node, you are safe. Distributed
transactions for `postgres_fdw` are provided by [patched postgres](https://github.com/postgrespro/postgres_cluster/tree/pg_shardman), but
that's largely work in progress:
  * `postgres_fdw.use_twophase = on` GUC turns on 2PC commit, but resolving procedure
    is fully manual for now, and replicas don't receive `PREPARE` yet.
  * `track_global_snapshots` and `postgres_fdw.use_global_snapshots` GUCs control
     distrubuted snapshot manager, providing `snapshot isolation` transaction
     isolation level.

## Shardlord failover
The only shardlord's state is tables with metadata mentioned above. They can be
replicated (with physical or logical replication) to some other node, which can
be made new shardlord at any moment.

## Some limitations:
  * You should not touch `sync_standby_names` manually while using pg_shardman.
  * The shardlord itself can't be worker node for now.
  * ALTER TABLE for sharded tables is not supported yet.
  * All [limitations of `pg_pathman`](https://github.com/postgrespro/pg_pathman/wiki/Known-limitations),
  e.g. we don't support global primary keys and foreign keys to sharded tables.

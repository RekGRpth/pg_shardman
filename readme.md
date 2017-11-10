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

Number of working nodes in cluster depends on number of servers you have, volume
of data you are going to store and workload. Increasing number of nodes in
cluster allows to store more data and provide better performance for some
queries (which are not affecting all nodes). Number of nodes can be increased in
future. But from the very beginning you should properly choose number of shards
(partitions) into which your table will be spitted, because we currently can't
change that later. Obviously it should not be smaller than number of nodes,
otherwise shardman will not be able to scatter data though all nodes.  Having
one partition per node will provide the best performance, especially in case of
using synchronous replication. But in this case you will not be able to add new
nodes in future. Or, more precisely, you will be able to add new nodes, but not
rebalance existed data between this new nodes. Also you will not be able to
address data skew, when some data is accessed much more often than other.

This is why it is recommended to have number of shards about ten times larger
then number of nodes. In this case you can increase number of nodes up to ten
times and manually move partitions between nodes to provide more or less uniform
load of all cluster nodes.

If you need fault tolerance, you need to store data with redundancy. Shardman
provides redundancy using logical replication.  In theory you can choose any
redundancy level: from 0 to infinity. But right now having redundancy level
larger than one doesn't have much sense. Even in case of using synchronous
replication, failure of some of node of the cluster can cause different states
of replicas of partition from the failed node. Shardman can't synchronize state
of replicas and just randomly chooses one of them as new primary partition.

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

### Basics

All cluster-management commands are executed on shardlord, thery are usual
PostgreSQL functions in `shardman` schema. So to get started, log in to
shardlord and register all cluster nodes using shardman.add_node function (
see description of all commands below), e.g.

```plpgsql
select shadman.add_node('port=5433', repl_group => 'default')
```

`repl_group` is arbitrary string identifying *replication group* (RG) we are
adding node to. Replication group is a set of nodes which can create replicas on
each other. Every node in the cluster belongs to some replication group and RGs
are not intersect. Node can't replicate partitions to nodes not from its RG. We
have this concept because
  * Currently logical replication in PG is relatively slow if there are many
  walsenders on the node, because each walsender decodes the whole
  WAL. Replication groups limit number of nodes where replicas can be
  located. For example, if we have RG from 3 nodes A, B, C, node A keeps 10
  partitions and have one replica for each partition, all these 10 replicas will
  be located on nodes B and C, so only 2 walsenders are spinning on A. The
  problem of poor performance with many walsenders is especially shard in case
  of synchronous replication. There is almost linear degradation of performance
  with increasing number of synchronous standbys.
  * It provides logical structuring of replication. For instance, you can
  include in replication group nodes connected to one switch. Modern
  non-blocking switches provides high speed throughput between any pair of nodes
  connected to this switch, while inter-switch link can still be bottleneck if
  you need send data between nodes connected to different switches. Including
  nodes connected to on switch in the same replication group can increase speed
  of replication. Replication group can be also used for opposite purpose:
  instead of increasing replication speed we can more worry about data
  reliability and include in a replication group nodes with different
  geographical location (hosted in different data centers). In this case
  incident in one data center (power failure, fire,...) will not cause loose of
  data.

In general, you should have number of nodes in each replication group equal to
redundancy level + 1, or a bit more to be able to increase redundancy later.
It means 1 (no redundancy) - 3 (redundancy 2) nodes in replication group in
practice.

By default all nodes are included in replication group "default". So be careful:
if you do not explicitly specify replication group, then all nodes will be
placed in the single replication group "default". As it was mentioned above, it
can cause serious problems with performance, especially in case of using
synchronous replication.

But if you do not need fault tolerance and are not going to use replication at
all, then using default replication group can also cause performance problems.
The reason is that `pg_shardman` configures replication channels between
replication group members at the moment when node is added to the group.  Even
if you specify redundancy level 0, there are still be active WAL senders, which
parse WALs, decode transactions and send them to other nodes.  These
transactions are empty, because there are not published tables, but decoding WAL
and sending empty transaction still adds signficant overhead and slows down
performance up to two times.

If you are not going to use replication, assign unique replication group name to
each node. In this case there are no logical replication subscriptions.

After adding all nodes you can shard you tables.
It should be done using `shardman.create_hash_partition function`.
This function has four arguments:
 * name of the name
 * name of sharding key
 * number of partitions
 * redundancy level (default is 0).

Right now shardman scatters shards between nodes using round-robin algorithm.
Replicas are randomly chosen within replication group, but with a guarantee that
there is no more than one copy of the partition per node.

### Local and shared tables

Apart from sharded tables, application may need to have local and/or shared tables.

Local table stores data which is unique for the particular node. Usually it is
some temporary data, for example temporary tables.  Local tables do not require
any assistance from shardman: just create and use them locally at each node.

Shared table can be used for dictionaries: rarely updated data required for all
queries. Shardman stores shared table at one of cluster nodes (master) and
broadcast it to all other nodes using logical replication.  All modifications of
shared table should be performed through the master. `pg_shardman` creates
'instead rules' for redirecting updates of shared table to the master mode. So
access to shared tables is transparent for application, except that transaction
doesn't see its own changes.

Shared table can be created using `shardman.create_shared_table(table_name,
master_node_id)` function.  `master_node_id` is unique identifier assigned to
the node when it is added to the cluster. You can check node identifiers in
`shardman.nodes` table at shardlord. Node identifies start from 1 and are
incremented on each add of a node.

### Importing data
After creation of sharded and shared tables at all nodes, you can upload data to
the cluster. The most efficient way is to use `COPY` command. Vanilla PostgreSQL
doesn't support `COPY FROM` to foreign tables. To make it work, for now we need
  * [patched postgres](https://github.com/postgrespro/postgres_cluster/tree/pg_shardman)
  * [and patched pg_pathman](https://github.com/arssher/pg_pathman/tree/foreign_copy_from)
Right now `pg_shardman` supports only text format and doesn't allow to specify
columns in COPY command.  It is possible to load data in parallel from several
nodes.

Certainly it is possible to populate data using normal inserts, which are
redirected by pathman to the proper node. Right now sharding is performed using
internal Postgres hash function, so it is not possible to predict at client
application level at which node particular record will be stored.

If redundancy level is not zero, then it is better to avoid large transactions,
because WAL sender decoder will spill large transactions to the disk, which
significantly reduces speed.

### Accessing the cluster

Each shardman node is able to execute any DML statement. It is possible to
execute queries involving data from more than one shard. It works using standard
Postgres inheritance mechanism: all partitions are derived from parent table. If
a partition is located at some other node, it will be accessed using foreign
data wrapper (`postgres_fdw`).  Unfortunately inheritance and FDW in Postgres
have some limitations which doesn't allow to build efficient execution plans for
some queries.

Shardlord commands called on workers are implicitly redirected to shardlord, but
it is highly recommended to perform them directly at shardlord. It is possible to
broadcast command to all cluster nodes using `shardman.forall(cmd text, use_2pc bool = false, including_shardlord bool = false)` function.
But if you are going to alter sharded or shared table, you should use
`shardman.forall(rel regclass, alter_clause text)` function.
You can also create new indexes. It must be done per each partition
separately, see
[pathman docs](https://github.com/postgrespro/pg_pathman/wiki/How-do-I-create-indexes)

Although Postgres now is capable of pushing aggregates to FDW, it can't merge
partial aggregate values from different nodes. Also it can't execute query at
all nodes in parallel: foreign data wrappers do not support parallel scan
because of using cursors. So execution of OLAP queries at shardman may be not so
efficient. Shardman is oriented mainly on OLTP workload.

### Administrating the cluster.

Shardman doesn't support now automatic failure detection and recovery. It has to
be done manually by DBA. Failed node should be excluded from the cluster using
`shardman.rm_node(node_id int, force bool = false)` command.  To prevent
unintentional loose of data, this function prohibit node deleting if there is
some data located at this node.  To allow deleting of such node set `force`
parameter to `true`.

If redundancy level is greater than zero, then shardman tries to replace
partitions of the deleted node with replica. Right now random replica is used.
In case of presence of more than one replica, shardman can not enforce
consistency if all this replicas.

It is possible to explicitly move partition or its replica to some other
node. Moving of partition with existed replicas can be done only within
replication group.  Replicas also can not migrate to some other replication
group.  Shardman provides `shardman.mv_partition(mv_part_name text, dst_node_id
int)` and `shardman.mv_replica(mv_part_name text, src_node_id int, dst_node_id
int)` functions. Both takes as first argument name of moved partition. Shardman
knows original location of partition, so it is enough to specify just
destination node. For replica it is also necessary to specify source node.

As alternative to explicit partition movement, it is possible to use
`shardman.rebalance(table_pattern text = '%')` and
`shardman.rebalance_replicas(table_pattern text = '%')` functions, which try to
uniformly distribute partitions/replicas of the specified tables between all
nodes. The single argument of this functions specifies table name pattern. But
default they try to rebalance all sharded tables. Rebalance tries to minimize
transfers and reduce impact of this operation on system. They are intended to be
performed in background and should not affect normal work of the cluster. This
is why they move only one partition per time.

It is also possible to increase redundancy level of existed table using
`shardman.set_redundancy(rel regclass, redundancy int, copy_data bool = true)`
function. This function can only increase redundancy level, not decrease it. It
is highly not recommended to specify optional `copy_data=false` parameter unless
you absolutely sure about what you are doing. `set_redundancy` function doesn't
wait completion of initial table sync for new replicas.  If you want to wait it,
to ensure that requested redundancy level is reached, then use
`shardman.ensure_redundancy()` function.

It is possible to remove table together with all its partitions and replicas
using `shardman.rm_table(rel regclass)` function. Please be careful: it doesn't
require any confirmation and data will be lost after successful completion of
this command.

If shardlord has failed during command execution or you just feel that something
goes wrong, run `shadman.recovery()` function. It will verify nodes state
against current shardlord's metadata and try to fix up things, e.g. reconfigure
LR channels, repair FDW, etc.

There is also `shardman.replication_lag` view which can be used it monitor
replication lag (which can be critical for asynchronous replication).

There are also several useful administrative functions:
 * `shardman.get_redundancy_of_partition(pname text)` returns redundancy level for the particular partition.
 * `shardman.get_min_redundancy(rel regclass)` returns minimal redundancy level for the whole relation.
 * `shardman.get_node_partitions_count(node int)` returns number of partitions at the particular node.
 * `shardman.get_node_replicas_count(node int)` returns number of replicas at the particular node.
All this functions can be executed only at shardlord.

And conversely `shardman.get_my_id()` can be executed at any working node to
obtain it's node id.

Below goes full list of commands.

## List of commands

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
create_shared_table(rel regclass, master_node_id int = 1)
```
Share table between all nodes. This function should be executed at
shardlord. The empty table should be present on shardlord, but not on nodes.

### Other functions

```plpgsql
recovery()
```
Check consistency of cluster state against current metadata and perform recovery,
if needed (reconfigure LR channels, repair FDW, etc).

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
  * ALTER TABLE for sharded is mostly not supported.
  * All [limitations of `pg_pathman`](https://github.com/postgrespro/pg_pathman/wiki/Known-limitations),
  e.g. we don't support global primary keys and foreign keys to sharded tables.

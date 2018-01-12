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
[here](https://github.com/postgrespro/postgres_cluster/tree/pg_shardman), which
we call 'patched Postgres' in this document.

To manage this zoo, we need one designated PG node which we call
*shardlord*. This node accepts sharding commands from the user and makes sure
the whole cluster changes its state as desired. Shardlord holds several tables
(see below) forming cluster metadata -- which nodes are in cluster and which
partitions they keep. Currently shardlord can't keep usual data itself and
is manually configured by the administrator.

Some terminology:
  * 'commands' is what constitutes `pg_shardman` interface: functions for sharding
     management.
  * 'shardlord' or 'lord' is Postgres instance which manages sharding.
  * 'worker nodes' or 'workers' are other nodes with data.
  * 'sharded table' is table managed by shardman.
  * 'shard' or 'partition' is any table containing part of sharded table.
  * 'primary' is main partition of sharded table, i.e. the only writable
     partition.
  * 'replica' is secondary partition of sharded table, i.e. read-only partition.
  * 'cluster' is either the whole system of shardlord and workers, or cluster in
	 traditional PostgreSQL sense, this should be clear from the context.

Number of working nodes in cluster depends on number of servers you have, volume
of data you are going to store and workload. Increasing number of nodes in
cluster allows to store more data and provide better performance for some
queries (which are don't affect all nodes). Number of nodes can be increased in
future. But from the very beginning you should properly choose number of shards
(partitions) into which your table will be spitted, because we currently can't
change that later. Obviously it should not be smaller than number of nodes,
otherwise `pg_shardman` will not be able to scatter the data among all nodes.
Having one partition per node will provide the best performance, especially in
case of using synchronous replication. But in this case you will not be able to
add new nodes in future. Or, more precisely, you will be able to add new nodes,
but not rebalance existing data between them. Also you will not be able to
address data skew, when some data is accessed much more often than other.

That's why it is recommended to have number of shards about ten times larger
then number of nodes. In this case you can increase number of nodes up to ten
times and manually move partitions between nodes to provide more or less uniform
load of all cluster nodes.

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
required. Extension links with `libpq`, and if you install PG from packages, you
should install `libpq-dev` package or something like that. The whole process of
building and copying files to PG server is just:

```shell
git clone https://github.com/postgrespro/pg_shardman
cd pg_shardman
make install
```

To actually install extension, add `pg_pathman` and `pg_shardman` to
`shared_preload_libraries`:
```shell
shared_preload_libraries='pg_shardman, pg_pathman'
```
restart the server and run

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
  * If `shardman.sync_replication` is `on`, shardlord configures sync replicas,
    otherwise async.
  * Logical replication settings, see `postgresql.conf.worker`

Currently extension scheme is fixed, it is, who would have thought, `shardman`.

## Usage guide

### Basics

All cluster-management commands are executed on shardlord, thery are usual
PostgreSQL functions in `shardman` schema. Most commands will be redirected to
shardlord if you execute them on worker node, but we recommend to run them
directly on the lord.

To get started, direct your steps to shardlord and register all cluster nodes on
it using `shardman.add_node` function (see full description of all commands
below), e.g.

```plpgsql
select shardman.add_node('port=5433', repl_group => 'rg_0')
```

`repl_group` is an arbitrary string identifying *replication group* (RG) we are
adding node to. Replication group is a set of nodes which can create replicas on
each other. Every node in the cluster belongs to some replication group and RGs
don't intersect. Node can't replicate partitions to nodes not from its RG. We
have this concept because
  * Currently logical replication in PG is relatively slow if there are many
  walsenders on the node, because each walsender decodes the whole
  WAL. Replication groups limit number of nodes where replicas can be
  located. For example, if we have RG from 3 nodes A, B, C, node A keeps 10
  partitions and have one replica for each partition, all these 10 replicas will
  be located on nodes B and C, so only 2 walsenders are spinning on A. The
  problem of poor performance with many walsenders is especially sharp in case
  of synchronous replication. There is almost linear degradation of performance
  with increasing number of synchronous standbys.
  * It provides logical structuring of replication. For instance, you can
  include in replication group nodes connected to one switch. Modern
  non-blocking switches provide high speed throughput between any pair of nodes
  connected to this switch, while inter-switch link can still be bottleneck if
  you need send data between nodes connected to different switches. Including
  nodes connected to on switch in the same replication group can increase speed
  of replication. Replication group can be also used for opposite purpose:
  instead of increasing replication speed we can more worry about data
  reliability and include in a replication group nodes with different
  geographical location (hosted in different data centers). In this case
  incident in one data center (power failure, fire, black raven with machine
  gun, etc) will not cause data loss.

In general, you should have number of nodes in each replication group equal to
redundancy level + 1, or a bit more to be able to increase redundancy later. It
means 1 (no redundancy) - 3 (redundancy 2) nodes per replication group in
practice. Since performance degrades as size of RG grows, it makes sense to have
replication groups of equal size. Unfortunately, we are not able to move
partitions between replication groups. It means that if we have e.g. cluster of
10 nodes with already sharded data and suddenly decide to throw 10 more nodes
into the battle, the only way to do that without resharding is to enlarge
existing replication groups.

By default node is assigned to replication group with name equal to node's
unique system identifier, which means that replications groups consist of a
single node and no logical replication channes are configured: you can't create
replicas, and no overhead is added.

BTW, even in absence of replicas, configured logical replication channels adds
serious overhead (up to two times on our measurments on ~10 nodes in one
RG). That's because walsenders still have to decode whole WAL, and moreover they
still send decoded empty transactions to subscribers.

With nodes added, you can shard tables. To do that, create the table
on shardlord with usual `CREATE TABLE` DDL and execute
`shardman.create_hash_partitions` function, for example

```plpgsql
CRATE TABLE horns (id int primary key, branchness int);
select create_hash_partitions('horns', 'id', 30, redundancy = 1)
```

On successfull execution, table `horns` will be hash-sharded into 30 partitions
(`horns_0`, `horns_1`... `horns_29`) by `id` column. For each partition a one
replica will be spawn and added to logical replication channel between nodes
holding primary and replica.

After table was sharded, each `pg_shardman` worker node can execute any DML statement
involving it. You are free to issue queries involving data from more than one
shard, remote or local. It works using standard Postgres inheritance mechanism:
all partitions are derived from parent table. If a partition is located at some
other node, it will be accessed using foreign data wrapper
`postgres_fdw`. Unfortunately inheritance and FDW in Postgres have some
limitations which doesn't allow to build efficient execution plans for some
queries. Though Postgres 10 is capable of pushing aggregates to FDW, it can't
merge partial aggregate values from different nodes. Also it can't execute query
on all nodes in parallel: foreign data wrappers do not support parallel scan
because of using cursors. Execution of OLAP queries at `pg_shardman` might not
be that efficient. `pg_shardman` is oriented mainly on OLTP workload.

Shardlord doesn't hold any data and can't read it, this node is used only for
managing the cluster.

Presently internal Postgres function is used for hashing; there is no easy way
for client app to determine the node for particular record to perform local
reads/writes.

### Importing data

The most efficient way to import data is to use `COPY` command. Vanilla
PostgreSQL doesn't support `COPY FROM` to foreign tables yet, and we have
implemented this feature in
[patched Postgres](https://github.com/postgrespro/postgres_cluster/tree/pg_shardman).
Binary format is not supported there. Data can be loaded parallel from several
nodes.

Obviously nobody forbids you to populate the cluster using normal inserts.

If redundancy level is not zero, then it is better to avoid large transactions,
because WAL decoder will spill large transactions to the disk, which
significantly reduces speed.

### Replication
Apart from specifying redundancy in `create_hash_partitions` call, replicas for
existing tables can be bred with `shardman.set_redundancy(rel_name name,
redundancy int)` function. This function can only increase redundancy level, it
doesn't delete replicas. `set_redundancy` function doesn't wait for completion
of initial table sync for new replicas. If you want to wait it to ensure that
the requested redundancy level is reached, run `shardman.ensure_redundancy()`
function.

If GUC `shardman.sync_replication` is `on` during replica creation, replica will
be added to `synchronous_standby_names` of the primary holder, making the
replication synchronous: though transactions on primary will be committed
locally immediately after the `COMMIT` request, the client will not get
successfull confirmation until it is committed on replica holder.

The trade-off is well-known: asynchronous replication is faster, but allows
replica to lag arbitrary behind the primary, which might lead to loss of a bunch
of recently committed transactions, or WAL puffing up in case of replica
failure. Synchronous replication is slower, but committed transaction are
*typically* not dropped. Typically, because it is actually still possible
to lose them without kind of 2PC commit. Imagine the following scenario:
 * Primary's connection with replica is teared down.
 * Primary executes a transaction, e.g. adds some row with id `42`, commits it
   locally and blocks because there is no connection with replica.
 * Client suddenly loses connection with primary for a moment and reconnects
   to learn the status of the transaction, sees the row with id `42` and
   thinks that it has been committed.
 * Now primary fails permanently and we switch to the replica. Replica has
   no idea of that transaction, but client is sure it is committed.

Though this situation is rather unlikely in practice, it is possible.

We don't track replication mode for each table separately, and changing
`shardman.sync_replication` GUC during cluster operation might lead to a mess.
It is better to set it permanently for now.

`pg_shardman` currently doesn't bother itself with configuring replication
identities. It is strongly recommended to use primary key as sharding key to
avoid problems with `UPDATE` and `DELETE` operations. Besides, primary key is
necessary to synchronize multiple replicas after failure.

### Balancing the load

Newly added nodes are initially empty. To bring some sense into their existence,
we need to rebalance the data. `pg_shardman` currently doesn't know how to move
it between replication groups. Functions
`shardman.rebalance(part_pattern text = '%')` and
`shardman.rebalance_replicas(replica_pattern text = '%')` try to
distribute partitions/replicas which names are `LIKE` given pattern uniformly
between all nodes of their replication groups. In `pg_shardman`, partitions are
called `$sharded_table_num_$part_num`. For instance, if you have sharded
table `horns`, issuing `shardman.rebalance('horns%')` should be enough to
rebalance its partitions. These functions move partitions/replicas sequentially,
one at time. We pretend that is was done to minimize the impact on the normal
work of the cluster -- they are expected to work in background. However, you are
free to think that we were just too indolent to implement parallel migration as
well.

You can also achieve more fine-grained control over data distribution by moving
explicitly partition or its replica to some other node. Again, moves are only
possible inside replication groups. Shardman provides
`shardman.mv_partition(mv_part_name text, dst_node_id int)` and
`shardman.mv_replica(mv_part_name text, src_node_id int, dst_node_id int)`
functions. Both take as first argument name of partition to move. `pg_shardman`
knows original location of partition, so it is enough to specify just the
destination node; for replica it is also necessary to specify the source node.

### Transactions

Atomicity and durability of transactions touching only single node is handled by
vanilla PostgreSQL, which does a pretty good job at that. However, without
special arrangments result of cross-node transaction might be non-atomic: if
coordinator (node where transaction started) has committed it on some nodes and
then something went wrong (e.g. it failed), the transaction will be aborted on
the rest of nodes. Because of
that
[patched Postgres](https://github.com/postgrespro/postgres_cluster/tree/pg_shardman) implements
two-phase commit (2PC) in `postgres_fdw`, which is turned on when
`postgres_fdw.use_twophase` GUC is set to `true`. With 2PC, the transaction is
firstly *prepared* on each node, and only then committed. Successfull *prepare*
on the node means that this node has promised to commit the transaction, and it
is also possible to abort the transaction in this state, which allows to get
consistent behaviour of all nodes.

The problem is that presently `PREPARE` is not transferred via logical
replication to replicas, which means that in case of permanent node failure we
still might lost part of distributed transaction and get non-atomic result if
primary has prepared the transaction, but died without committing it and now
replica has no idea about the xact. Properly implemented `PREPARE` going over
logical replication will also mitigate the possibilty of losing transactions
described in 'Replication' section, and we plan to do that.

Similarly, if transactions affect only single nodes, plain PostgreSQL isolation
rules are applicable. However, for distributed transactions we need distributed
visibility, implemented in patched Postgres. GUC
`postgres_fdw.use_global_snapshots` turns on distributed transaction manager
based on Clock-SI algorithm. It provides snapshot isolation (called `REPEATABLE
READ` in Postgres) consistency.

Yet another problem is distributed deadlocks. They can be detected and resolved
using `monitor` function, see the reference below.

## Worker failover

`pg_shardman` doesn't support presently automatic failure detection and recovery. It
has to be done manually by the DBA. If some node is down, its data is not
available until either node rises or it is ruled out from the cluster by
`rm_node` command. Moreover, if failed node holds replicas and sync replication
is used, queries touching replicated partitions would block. When failed node is
reestablished, data on it becomes reachable again and the node receives missed
changes for holded replicas automatically. However, you should run
`recover_xacts` or `monitor` functions to resolve possibly hanged distributed
transactions.

If failure is permanent and the node never intends to be up again, it should be
excluded from the cluster with `rm_node` command. This function also promotes
replicas of partitions held by removed node. The general procedure for failover
is the following.
* Make sure failed node is really turned off, and never make it online without
  erasing data on it, or make sure no one tries to access the node -- otherwise
  stale reads and inconsistent writes on it are possible.
* Run `select shardman.rm_node($failed_node_id, force => true)` to exclude it
  and promote replicas. The most advanced replica is choosen and state of other
  replicas is synchronized.
* Run `select shardman.recover_xacts()` to resolve possibly hanged 2PC
  transactions.

Note that presently some recent transactions (or parts of distributed
transactions) still might be lost as explained in 'Transactions' section.

## Shardlord failover

Shardlord doesn't participate in normal cluster operation, and its failure is
not that terrible. Anyway, it can be done relatively easily. The only
shardlord's state is tables with metadata listed in section 'Metadata
tables'. They can be replicated (with physical or logical replication) to some
other node, which can be made new shardlord at any moment by adjusting
`shardman.shardlord` GUC. It is DBA's responsibility that two lords are not used
at the same time. Also, don't forget to update `shardman.shardlord_connstring`
everywhere.

### Local and shared tables

Apart from sharded tables, application may need to have local and/or shared
tables.

Local table stores data which is unique for the particular node. Usually it is
some temporary data, for example temporary tables. Local tables do not require
any assistance from `pg_shardman`: just create and use them locally at each
node.

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

### What else you can do with the cluster

It is possible to remove table together with all its partitions and replicas
using `shardman.rm_table(rel regclass)` function. Please be careful: it doesn't
require any confirmation.

It is possible to execute some SQL on all cluster nodes using
`shardman.forall(cmd text, use_2pc bool = false, including_shardlord bool =
false)` function.  But if you are going to alter sharded or shared table, you
should use `shardman.forall(rel regclass, alter_clause text)` function. You can
also create new indices. It must be done per each partition separately,
see
[pathman docs](https://github.com/postgrespro/pg_pathman/wiki/How-do-I-create-indexes)

`monitor` function on shardlord can continiously track failed nodes and resolve
distributed deadlocks, see the reference.

If shardlord has failed during command execution or you just feel that something
goes wrong, run `shardman.recover()` function. It will verify nodes state
against current shardlord's metadata and try to fix up things, i.e. reconfigure
LR channels, repair FDW, etc.

There is also `shardman.replication_lag` view which can be used to monitor
replication lag, which can be critical for asynchronous replication.

## Metadata tables

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

## Commands reference

### Membership

```plpgsql
add_node(super_conn_string text, conn_string text = NULL, repl_group text = 'default') returns int
```
Add node with given libpq connstring(s) to the cluster. Node is assigned unique
id. If node previously contained shardman state from old cluster (not one
managed by current shardlord), this state will be lost.

Returns cluster-unique id of added node. Identifiers start from 1 and are
incremented on each node addition.

`super_conn_string` is connection string to the node which must provide
superuser access to the node, and `conn_string` can be some other connstring. The
former is used for configuring logical replication, the latter for DDL and for
setting up FDW, i.e. accessing the data. This separation serves two purposes:
  * It allows to work with the data without requiring superuser privileges.
  * It allows to set up pgbouncer, as replication can't go through it.
If `conn_string` is `NULL`, `super_conn_string` is used everywhere.

`repl_group` is the name of node's replication group. We have no explicit
command for changing RG of already added node -- you have to remove it and add
again.

We don't move any parts and replicas to newly added node, see `rebalance_*`
commands for that below. However, freshly added node instantly becomes aware
of sharded tables and can accept queries to them.

Returns id of the new node.

```plpgsql
get_my_id() returns int
```
Get this worker's id. Executed on any worker node. Fails on shardlord.

```plpgsql
rm_node(rm_node_id int, force bool = false)
```
Remove node from the cluster. If `force` is true, we don't care whether node
contains any partitions. Otherwise we won't allow to rm node holding shards. We
will try to execute `wipe_state` on deleted node if node is alive, but the
command succeeds even if we can't. We don't remove tables with data on
removed node.

If node contained partitions, for each one we automatically promote random
replica.

### Shards and replicas

```plpgsql
create_hash_partitions(rel_name name, expr text, part_count int, redundancy int = 0)
```
To shard the table, you must create it on shardlord with usual `CREATE TABLE
...` and then call this function. It hash-shards table `rel_name` by key `expr`,
creating `part_count` shards, distributing shards evenly among the nodes. As you
probably noticed, the signature mirrors `pg_pathman`'s function with the same
name. `redundancy` replicas will be immediately created for each partition.

Shards are scattered among nodes using round-robin algorithm. Replicas are
randomly chosen within replication group, but with a guarantee that there is no
more than one copy of the partition per node. Distributed table is always
created empty, it doesn't matter had the original table on shardlord had any
data or not.

Column(s) participating in `expr` must be marked `NOT NULL`, as in
`pg_pathman`. Generally we strongly recommend to use primary key there.

```plpgsql
rm_table(rel_name name)
```
Drop sharded table. Removes data from all workers. Doesn't touch table on
shardlord.

```plpgsql
set_redundancy(rel_name name, redundancy int)
```
Create replicas for parts of sharded table `rel` until each shard has
`redundancy` replicas. Replicas holders are choosen randomly among members of
partition's replication group. If existing level of redundancy is greater than
specified, then currently this function does nothing. Note that this function
only starts replication, it doesn't wait for full initial data copy.  See the
next function.

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
redistribute partitions (replicas) which names are `LIKE` pattern between all
nodes of corresponding replication groups, they are should be called after nodes
addition. It can't move parts/replicas between replication
groups. Parts/replicas are moved sequentially to minimize influence on system
performance. Thanks to logical replication, you can continue writing to the
table during moving.

```plpgsql
mv_partition(mv_part_name text, dst_node_id int)
mv_replica(mv_part_name text, src_node_id int, dst_node_id int)
```
Move single partition/replica to other node. This function can move
parts/replicas only within replication group. Such fine-grained control is
rarely needed -- see `rebalance_*` functions.

```plpgsql
create_shared_table(rel regclass, master_node_id int = 1)
```
Share table between all nodes. This function should be executed at
shardlord. The empty table should be present on shardlord, but not on nodes.

The rest of functions in this section can be executed only on shardlord.

```plpgsql
shardman.get_redundancy_of_partition(pname text)
```
Returns redundancy level for the particular partition.

```plpgsql
shardman.get_min_redundancy(rel_name name)
```
Returns minimal redundancy level for the whole relation.

```plpgsql
shardman.get_node_partitions_count(node int)
```
Returns number of partitions at the particular node.

```plpgsql
shardman.get_node_replicas_count(node int)
```
Returns number of replicas at the particular node.

### Other functions

```plpgsql
recover()
```
Check consistency of cluster state against current metadata and perform recovery,
if needed (reconfigure LR channels, repair FDW, etc).

```plpgsql
monitor(check_timeout_sec int = 5, rm_node_timeout_sec int = 60)
```

Monitor cluster for presence of distributed deadlocks and node failures. This
function is intended to be executed at shardlord and is redirected to shardlord
been launched at any other node. It starts infinite loop which polls all
clusters nodes, collecting local *lock graphs* from all nodes. Period of poll is
specified by `check_timeout_sec` parameter (default value is 5 seconds).  Local
lock graphs are combined into global lock graph which is analyzed for the
presence of loops. A loop in the lock graph means distributed deadlock. Monitor
function tries to resolve deadlock by canceling one or more backends involved in
the deadlock loop (using `pg_cancel_backend` function, which doesn't actually
terminate backend but tries to cancel current query). Canceled backend is
randomly chosen within deadlock loop. Since not all deadlock members are
hanged in 'active query' state, it might be needed to send cancel several times.

Since local graphs collected from all nodes do not form consistent global
snapshot, false postives are possible: edges in deadlock loop correspond to
different moment of times. To prevent false deadlock detection, monitor function
doesn't react on detected deadlock immediately. Instead of it, previous deadlock
loop located at previous iteration is compared with current deadlock loop and
only if they are equal, deadlock is reported and resolving is performed.

If some node is unreachable then monitor function prints correspondent error
message and retries access until `rm_node_timeout_sec` timeout expiration. After
it node is removed from the cluster using `shardman.rm_node` function.  If
redundancy level is non-zero, then primary partitions from the disabled node are
replaced with replicas.  Finally `pg_shardman` performs recovery of distributed
transactions for which failed node was the coordinator. It is done using
`shardman.recover_xacts()` function which collects status of distributed
transaction at all participants and tries to make decision whether it should be
committed or aborted.
If `rm_node_timeout_sec` is `NULL`, `monitor` will not remove nodes.

```plpgsql
recover_xacts()
```

Function `shardman.recover_xacts()` can be also manually invoked by database
administrator on shardlord after abnormal cluster restart to recover not
completed distributed transactions. If the coordinator is still in the cluster,
we ask it about transaction outcome. Otherwise, we inquire every node's opinion
on the xact; if there is at least one commit (and no aborts), we commit it, if
there is at least one abort (and no commits), we abort it. All nodes in the
cluster must be online to let this function resolve the transaction. Patched
Postgres is needed for proper work of this function.

Another limitation of `shardman.recover_xacts` is that we currently don't
control recycling of WAL and clog used to check for completed transaction
status. Though unlikely, in theory it is possible that we won't be able to
learn it and resolve the transaction.

```plpgsql
wipe_state(drop_slots_with_fire bool DEFAULT true)
```
Remove unilaterally all publications, subscriptions, replication slots, foreign
servers and user mappings created on the worker node by
`pg_shardman`. PostgreSQL forbids to drop replication slot with active
connection; if `drop_slots_with_fire` is true, we will try to kill the
walsenders before dropping the slots. Also, immediately after transaction commit
set `synchronous_standby_names` GUC to empty string -- this is a
non-transactional action and there is a very small chance it won't be
completed. You probably want to run it before `DROP EXTENSION pg_shardman`.
Data is not touched by this command.

## Some limitations:
  * You should not touch `synchronous_standby_names` manually while using pg_shardman.
  * The shardlord itself can't be worker node for now.
  * All [limitations](https://github.com/postgrespro/pg_pathman/wiki/Known-limitations)  (and some features) of `pg_pathman`,
	e.g. we don't support global secondary indexes and foreign keys to sharded tables.
  * All [limitations of logical replications](https://www.postgresql.org/docs/10/static/logical-replication-restrictions.html).
    `TRUNCATE` statements on sharded tables will not be replicated.

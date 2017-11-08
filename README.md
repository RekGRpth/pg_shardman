1. Postgres configuration

First of all you need to add pg_pathman extension to shared_preload_libraries in PostgreSQL configuration file.
Also you need to specify shardman.shardlord_connstring to provide access to shardlord.
At shardlord instance you need also to set shardman.shardlord = on
To make it possible for shardlord to access cluster nodes and establish replication channels between them, you need to tune pg_hba.conf,
for example:

# "local" is for Unix domain socket connections only
local   all             all                                     trust
# IPv4 local connections:
host    all             all             127.0.0.1/32            trust
# IPv6 local connections:
host    all             all             ::1/128                 trust
# Allow replication connections from localhost, by a user with the
# replication privilege.
local   replication     USERNAME                                trust
host    replication     USERNAME        127.0.0.1/32            trust
#host    replication    USERNAME        ::1/128                 trust

Once cluster is started, you need to create three extensions:

CREATE EXTENSION postgres_fdw;
CREATE EXTENSION pg_pathman;
CREATE EXTENSION pg_shardman;

Shardman is using logical replication, two pahse commit and foreign data wrapper so the following PostgreSQL parameters
need to be adjusted:
max_connections, max_worker_processes, max_prepared_transactions, wal_level, max_wal_senders, max_replication_slots,
max_logical_replication_worker.

Below are recommended values of this parameters for cluster with 10 nodes:

max_connections = 200
max_prepared_transactions = 200
max_worker_processes = 200
wal_level = logical
max_wal_senders = 50
max_replication_slots = 100

There are few shardman/postgres_fdw parameter you may need to specify:

postgres_fdw.use_2pc               Toggle use of two phase commit in postgres_fdw.

postgres_fdw.use_repeatable_read   By default postgres_fdw use repeatable read isolation level at remote site,
								   which can cause serializability errors. Setting this option to off, cause postgres_fdw
								   to use default isolation level (read committed)

shardman.sync_replication          Use synchronous replication: commit will wait until changes are saved at all replicas.


2. Cluster configuration

Shardman cluster consists of several working nodes managed by "shardlord" - special  instance of postgres,
which maintains cluster metadata. There is no data stored at shardlord, bit all tables, indexes,... should be created
at shardlord and then will be broadcasted to working nodes by shardman.

Shardlord can be protected from fault using standard streaming replication, which should be configured manually by user.
Shardlord requires separate Postgres instance, it can be started at one of the working nodes, but can not share database with this working node.

Number of working nodes in cluster depends on number of servers you have, volume of data you are going to store and workload.
Increasing number of nodes in cluster allows to store more data and provide better performance for some queries (which are not affecting 
all nodes). Number of nodes can be increased in future. But from the very beginning you should properly choose number of shards (partitions) into which
your table will be spitted. Obviously it should not be smaller than number of nodes, otherwise shardman will not be able to scatter data though all nodes.
Having one partition per node will provide the best performance, especially in case of using synchronous replication.
But in this case you will not be able to add new nodes in future. Or, more precisely, you will be able to add new nodes, but not rebalance existed data between this new
nodes. Also you will not be able to address data skew (non-uniform data distribution). Some shard can be accessed much more frequently than others.

This is why it is recommended to have number of shards about ten times larger then number of nodes. In this case you can increase number of nodes up to ten times
and manually move partitions between nodes to provide more or less uniform load of all cluster nodes.

If you need fault tolerance, you need to store data with redundancy. Shardman provides redundancy using logical replication.
In theory you can choose any redundancy level: from 0 to infinity. But right now having redundancy level larger than one have not so much sense.
Even in case of using synchronous replication, failure of some of node of the cluster can cause different states of replicas of partition from the failed node.
Shardman is not able to synchronize state of replicas and just randomly choose one of them as new primary partition.

3. Cluster initialization

Taken in account all consideration above we are now ready to initialize our cluster.
Login to shardlord database and initialize database schema at it.
Then register all cluster nodes using shardman.add_node function. Parameters of this function is superuser connection string (needed to create
logical replication subscriptions), normal user connection string (by default it is the same as superuser connection string) and replication group.

Replication group name is just arbitrary string identifying "replication group". All nodes with the same group name are assumed to belong to the same replication
group. Shardman performs replication only within replication group. It means that replicas of partition can be created only at one of the other nodes in
this replication group.

Replication group is needed to restrict number of replication channels between node. If there are multiple sharded tables and randomly scattered replicas of their
partitions between all nodes, then number of replication channels from each node will be the same as number of nodes. But number of nodes can be very large:
hundreds or even thousands. Maintaining thousands of subscriptions adds very large overhead: PostgreSQL starts separate WAL sender process for each subscription
and each of them individually decodes the whole WAL. But the most negative influence on performance will have in case
of synchronous replication. There is almost linear degradation of performance with increasing number of synchronous standbys.

This is why for large number of nodes (>10) it very desirable to limit number of subscriptions and split all cluster nodes into several replication groups.
Number of nodes in each replication group should not exceed 10.

Replication group can be also used for more optimal utilization of particular network topology. For example, you can include in replication group nodes
connected to one switch. Modern non-blocking switches provides high speed throughput between any pair of nodes connected to this switch, while inter-switch link
can still be bottleneck if you need send data between nodes connected to different switches. Including nodes connected to on switch in the same replication
group can increase speed of replication.

Replication group can be also used for opposite purpose: instead of increasing replication speed we can more worry about data reliability and include in a
replication group nodes with different geographical location (hosted in different data centers). In this case incident in one data center (power failure, fire,...) will not cause loose of data.

By default all nodes are included in replication group "default". So be careful: if you do not explicitly specify replication group, then
all nodes will be placed in the single replication group "default". As it was mentioned above, it can cause serious problems with performance,
especially in case of using synchronous replication.

But if you do not need fault tolerance and are not going to use replication at all, then using default replication group can also cause performance problems.
The reason is that shardman configures replication channels between replication group members at the moment when node is added to the group.
Even if you specify redundancy level 0, there are still be active WAL senders, which parse WALs, decode transactions and send them to other nodes.
These transactions are empty, because there are not published tables, but decoding WAL and sending empty transaction still adds signficant overhead
and slows down performance up to two times.

If you are not going to use replication, assign unique replication group name to each node. In this case there are no logical replication subscriptions.

After adding all nodes you can shard you tables.
It should be done using shardman.create_hash_partition function. This function has four arguments:
- name of the name
- name of sharding key
- number of partitions
- redundancy level (default is 0).

Right now shardman scatters shards between nodes using round-robin algorithm.
Replicas are randomly chosen within replication group, but with guarantee that there is no more than one image of the partition per node.

Except sharded table application may need to have local and shared tables.

Local table stores data which is unique for the particular node. Usually it is some temporary data, for example temporary tables.
Local tables do not require any assistance from shardman: just create and use them locally at each node.

Shared table can be used for dictionaries: rarely updated data required for all queries.
Shardman stores shared table at one of cluster nodes (master) and broadcast it to all other nodes using logical replication.
All modifications of shared table should be performed through the master. Shardman creates "instead rules" for redirecting updates of shared table
to the master mode. So access to shared tables is transparent for application, except that transaction doesn't see its own changes.

Shared table can be created using shardman.create_shared_table(table_name, master_node_id) function.
master_node_id is unique identifier assigned to the node when it is added to the cluster. You can check node identifiers in shardman.nodes table at shardlord.
Or just take in account, that node identifies start from 1 and are incremented on each add of a node.


4. Populating data in the cluster.

After creation of sharded and shared tables at all nodes, you can upload data to the cluster.
The most efficient way is to use COPY command. Right now shardman supports only text format and doesn't allow to specify columns in COPY command.
It is possible to load data in parallel from several nodes.

Certainly it is possible to populate data using normal inserts, which are redirected by pathman to the proper node.
Right now sharding is performed using internal Postgres hash function, so it is not possible to predict at client application level at which node
particular record will be stored.

If redundancy level is not zero, then it is better to avoid large transactions, because WAL sender decoder will spill large transactions to the disk,
which significantly reduce speed.


5. Accessing cluster.

Each shardman node is able to execute any DML statement. Shardlord commands are implicitly redirected to shardlord, but it is highly
recommended to perform them directly at shardlord. DML commands can be broadcasted to all cluster nodes using shardman.forall(cmd text) function,
but right now shardman doesn't support altering of existed tables (except adding indexes, constraints and other modification not affecting table data).

It is possible to execute queries involving data from more than one shard. It works using standard Postgres inheritance mechanism:
all partitions are derived from parent table. If a partition is located at some other node, it will be accessed using foreign data wrapper (postgres_fdw).
Unfortunately inheritance and FDW in Postgres have some limitations which doesn't allow to build efficient execution plans for some queries.

Although Postgres is now able to push aggregates to FDW, it is not able to merge partial aggregate values from different nodes.
Also it is not able to execute query at all nodes in parallel: foreign data wrappers do not support parallel scan because of using cursors.
So execution of OLAP queries at shardman may be not so efficient. Shardman is first of all oriented on OLTP workload.


6. Administrating cluster.

Shardman doesn't support now automatic failure detection and recovery.
It has to be done manually by DBA. Failed node should be excluded from the cluster using shardman.rm_node(node_id int, force bool = false) command.
To prevent unintentional loose of data, this function prohibit node deleting if there is some data located at this node.
To allow deleting of such node set "force" parameter to true.

If redundancy level is greater than zero, then shardman tries to replace portions of the deleted node with replica. Right now random replica is used.
In case of presence of more than one replica, shardman can not enforce consistency if all this replicas.

It is possible to explicitly move partition or its replica to some other node. Moving of partition with existed replicas can be done only within replication group.
Replicas also can not migrate to some other replication group.

Shardman provides shardman.mv_partition(mv_part_name text, dst_node_id int) and shardman.mv_replica(mv_part_name text, src_node_id int, dst_node_id int)
functions. Both takes as first argument name of moved partition. Shardman knows original location of partition, so it is enough to specify just destination node.
For replica it is also necessary to specify source node.

As alternative to explicit partition movement, it is possible to use shardman.rebalance(table_pattern text = '%')  and shardman.rebalance_replicas(table_pattern text = '%')
functions, which try to uniformly distribute partitions/replicas of the specified tables between all nodes. The single argument of this functions
specifies table name pattern. But default them try to rebalance all sharded tables. Rebalance tries to minimize transfers and reduce impact of this operation
on system. Them are intended to be performed in background and should not affect normal work of the cluster.
This is why them move one partition per time.

It is also possible to increase redundancy level of existed table using shardman.set_redundancy(rel regclass, redundancy int, copy_data bool = true)
function. This function can only increase redundancy level, not decrease it. It is highly not recommended to specify optional "copy_data" parameter
unless you absolutely sure about what you are doing. set_redundancy function doesn't wait completion of initial table sync for new replicas.
If you want to wait it, to ensure that requested redundancy level is reached, then use shardman.ensure_redundancy() function.

It is possible to remove table together with all its partitions and replicas using shardman.rm_table(rel regclass) function.
Please be careful: it doesn't require any confirmation and data will be lost after successful completion of this command.

If execution of shardlord command was interrupted or abnormally terminated, then cluster may leave in inconsistent state.
Consistency can be restored using shardman.recovery() command. It tries to reestablish FDW and logical replication channels according to metadata.
This command reports all performed recovery actions, so it can be used to check consistency of the cluster.
If it reports nothing then no inconsistency is found in the cluster.

Current cluster configuration can be inspected at shardlord in shardman.nodes, shardman.tables, shardman.partitions and shardman.replicas tables.
These tables are create by shardman extension and are present at all nodes. But them are maintained only at shardlord.

Please find list shardman metadata tables in Appendix 1.

There is also shardman.replication_lag view which can be used it monitor replication lag (which can be critical for asynchronous replication).

There are also several useful administrative functions:

shardman.get_redundancy_of_partition(pname text) returns redundancy level for the particular partition.
shardman.get_min_redundancy(rel regclass) returns minimal redundancy level for the whole relation.
shardman.get_node_partitions_count(node int) returns number of partitions at the particular node.
shardman.get_node_replicas_count(node int) returns number of replicas at the particular node.

All this functions can be executed only at shardlord.
And conversely shardman.get_my_id() can be executed at any working node to obtain it's node id.


Appendix 1: Shardman metadata tables


-- List of nodes present in the cluster
CREATE TABLE nodes (
	id serial PRIMARY KEY,
	system_id bigint NOT NULL,
    super_connection_string text UNIQUE NOT NULL,
	connection_string text UNIQUE NOT NULL,
	replication_group text NOT NULL -- group of nodes within which shard replicas are allocated
);

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

-- View for monitoring replication lag
CREATE VIEW replication_lag(pubnode, subnode, lag);


Appendix 2: Shardman public functions:

-- Add a node: adjust logical replication channels in replication group and
-- create foreign servers.
-- 'super_conn_string' is connection string to the node which allows to login to
-- the node as superuser, and 'conn_string' can be some other connstring.
-- The former is used for configuring logical replication, the latter for DDL
-- and for setting up FDW. This separation serves two purposes:
-- * It allows to access data without requiring superuser privileges;
-- * It allows to set up pgbouncer, as replication can't go through it.
-- If conn_string is null, super_conn_string is used everywhere.
CREATE FUNCTION add_node(super_conn_string text, conn_string text = NULL,
						 repl_group text = 'default') RETURNS void;

-- Remove node: try to choose alternative from one of replicas of this nodes,
-- exclude node from replication channels and remove foreign servers.
-- To remove node with existing partitions use force=true parameter.
CREATE FUNCTION rm_node(rm_node_id int, force bool = false) RETURNS void;

-- Shard table with hash partitions. Parameters are the same as in pathman.
-- It also scatter partitions through all nodes.
-- This function expects that empty table is created at shardlord.
-- So it can be executed only at shardlord and there is no need to redirect this function to shardlord.
CREATE FUNCTION create_hash_partitions(rel regclass, expr text, part_count int, redundancy int = 0)
RETURNS void;

-- Provide requested level of redundancy. 0 means no redundancy.
-- If existing level of redundancy is greater than specified, then right now this
-- function does nothing.
CREATE FUNCTION set_redundancy(rel regclass, redundancy int, copy_data bool = true)
RETURNS void;

-- Provide requested level of redundancy. 0 means no redundancy.
-- If existing level of redundancy is greater than specified, then right now this
-- function does nothing.
CREATE FUNCTION set_redundancy(rel regclass, redundancy int, copy_data bool = true)
RETURNS void;

-- Wait completion of initial table sync for all replication subscriptions.
-- This function can be used after set_redundancy to ensure that partitions are copied to replicas.
CREATE FUNCTION ensure_redundancy() RETURNS void;

-- Remove table from all nodes.
CREATE FUNCTION rm_table(rel regclass)
RETURNS void;
-- Move partition to other node. This function is able to move partition only within replication group.
-- It creates temporary logical replication channel to copy partition to new location.
-- Until logical replication almost caught-up access to old partition is now denied.
-- Then we revoke all access to this table until copy is completed and all FDWs are updated.
CREATE FUNCTION mv_partition(mv_part_name text, dst_node_id int)
RETURNS void;

-- Get redundancy of the particular partition
-- This command can be executed only at shardlord.
CREATE FUNCTION get_redundancy_of_partition(pname text) returns bigint;

-- Get minimal redundancy of the specified relation.
-- This command can be executed only at shardlord.
CREATE FUNCTION get_min_redundancy(rel regclass) returns bigint;

-- Execute command at all shardman nodes.
-- It can be used to perform DDL at all nodes.
CREATE FUNCTION forall(sql text, use_2pc bool = false) returns void;

-- Count number of replicas at particular node.
-- This command can be executed only at shardlord.
CREATE FUNCTION get_node_replicas_count(node int) returns bigint;

-- Count number of partitions at particular node.
-- This command can be executed only at shardlord.
CREATE FUNCTION get_node_partitions_count(node int) returns bigint;

-- Rebalance partitions between nodes. This function tries to evenly
-- redistribute partitions of tables which names match LIKE 'pattern'
-- between all nodes of replication groups.
-- It is not able to move partition between replication groups.
-- This function intentionally moves one partition per time to minimize
-- influence on system performance.
CREATE FUNCTION rebalance(table_pattern text = '%') RETURNS void;

-- Share table between all nodes. This function should be executed at shardlord. The empty table should be present at shardlord,
-- but not at nodes.
CREATE FUNCTION create_shared_table(rel regclass, master_node_id int = 1) RETURNS void;

-- Move replica to other node. This function is able to move replica only within replication group.
-- It initiates copying data to new replica, disables logical replication to original replica, 
-- waits completion of initial table sync and then removes old replica.
CREATE FUNCTION mv_replica(mv_part_name text, src_node_id int, dst_node_id int)
RETURNS void;

-- Rebalance replicas between nodes. This function tries to evenly
-- redistribute replicas of partitions of tables which names match LIKE 'pattern'
-- between all nodes of replication groups.
-- It is not able to move replica between replication groups.
-- This function intentionally moves one replica per time to minimize
-- influence on system performance.
CREATE FUNCTION rebalance_replicas(table_pattern text = '%') RETURNS void;

-- Get self node identifier
CREATE FUNCTION get_my_id() RETURNS int;

-- Check consistency of cluster with metadata and perform recovery
CREATE FUNCTION recovery() RETURNS void;

Appendix 3:
Sample shardman startup script:


n_nodes=3
n_partitions=30
redundancy=1
export PATH=~/postgresql.vanilla/dist/bin/:$PATH
ulimit -c unlimited
pkill -9 postgres
sleep 2
rm -fr shardlord node? *.log
for ((i=1;i<=n_nodes;i++))
do
    port=$((5432+i))
    initdb node$i
done
initdb shardlord

echo Start nodes

for ((i=1;i<=n_nodes;i++))
do
    port=$((5432+i))
    sed "s/5432/$port/g" < postgresql.conf.shardman > node$i/postgresql.conf
	echo "shared_preload_libraries = 'pg_pathman'" >> node$i/postgresql.conf
	echo "shardman.shardlord_connstring = 'port=5432 dbname=postgres host=localhost sslmode=disable'" >> node$i/postgresql.conf
	cp pg_hba.conf node$i
    pg_ctl -D node$i -l node$i.log start
done

echo Start shardlord
cp postgresql.conf.shardman shardlord/postgresql.conf
echo "shared_preload_libraries = 'pg_pathman'" >> shardlord/postgresql.conf
echo "shardman.shardlord = on"  >> shardlord/postgresql.conf
echo "shardman.shardlord_connstring = 'port=5432 dbname=postgres host=localhost sslmode=disable'"  >> shardlord/postgresql.conf
cp pg_hba.conf shardlord
pg_ctl -D shardlord -l shardlord.log start

sleep 5

psql postgres -c "CREATE EXTENSION postgres_fdw"
psql postgres -c "CREATE EXTENSION pg_pathman"
psql postgres -c "CREATE EXTENSION pg_shardman"

for ((i=1;i<=n_nodes;i++))
do
    port=$((5432+i))
	psql -p $port postgres -c "CREATE EXTENSION postgres_fdw"
	psql -p $port postgres -c "CREATE EXTENSION pg_pathman"
	psql -p $port postgres -c "CREATE EXTENSION pg_shardman"
done

for ((i=1;i<=n_nodes;i++))
do
    port=$((5432+i))
	psql postgres -c "SELECT shardman.add_node('dbname=postgres host=localhost sslmode=disable port=$port')"
done
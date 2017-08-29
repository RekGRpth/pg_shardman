First, some terminology:
'shardlord' or 'lord' is postgres instance and background process (bgw) spinning
  on it which manages sharding.
'worker nodes' or 'workers' are other nodes with data.
'sharded table' is table managed by shardman.
'shard' or 'partition' is any table containing part of sharded table.
'primary' is main partition of sharded table, i.e. the only writable
  partition.
'replica' is secondary partition of sharded table, i.e. read-only partition.
'cluster' -- either the whole system of shardlord and workers, or cluster in
  traditional PostgreSQL sense, this should be clear from the context.

For quick setup, see scripts in bin/ directory. Setup is configured in file
setup.sh which needs to be placed in the same directory; see setup.sh.example
for example. shardman_init.sh performs initdb for shardlord & workers, deploys
example configs and creates extension; shardman_start.sh reinstalls extension,
which is useful for development.

Both shardlord and workers require extension built and installed. We depend
on pg_pathman extension so it must be installed too.
PostgreSQL location for building is derived from pg_config, you can also specify
path to it in PG_CONFIG var. PostgreSQL 10 (master branch as of writing this)
is required. The whole process is of building and copying files to PG server is just:

git clone
cd pg_shardman
make install

To actually install extension, add pg_shardman and pg_pathman to
shared_preload_libraries, restart the server and run

create extension pg_shardman cascade;

Have a look at postgresql.conf.common.template and postgresql.conf.lord.template
example configuration files. The former contains all shardman's and important
PostgreSQL GUCs for either shardlord and workers, the latter for shardlord only
-- in particular, shardman.shardlord defines whether the instance is shardlord or
not.

Immediately after starting the server with shardman library preloaded, but
before creating extension you will receive on shardlord warning like

WARNING: pg_shardman library is preloaded on shardlord, but extenstion is not
  created

This is normal as we have here kind of circular dependency: it is pointless to
create extension without the library, and library also uses SQL objects, so
shardlord won't start without installed extension.

Currently extension scheme is fixed, it is, who would have though, 'shardman'.

Now you can issue commands to the shardlord. All shardman commands (cmds) you
issue return immediately because they technically just submit the cmd to the
shardlord; he learns about them and starts the actual execution. At any time you
can cancel currently executing command, just send SIGUSR1 to the shardlord. This
is not yet implemented as a handy SQL function, but you can use cancel_cmd.sh
script from bin/ directory. All submitted cmds return unique command id which is
used to check the cmd status later by querying shardman.cmd_log and
shardman.cmd_opts tables:

CREATE TABLE cmd_log (
	id bigserial PRIMARY KEY,
	cmd_type cmd NOT NULL,
	status cmd_status DEFAULT 'waiting' NOT NULL
);
CREATE TABLE cmd_opts (
	id bigserial PRIMARY KEY,
	cmd_id bigint REFERENCES cmd_log(id),
	opt text
);

We will unite them into convenient view someday. Commands status is enum with
mostly obvious values ('waiting', 'canceled', 'failed', 'in progress',
'success', 'done'). You might wonder what is the difference between 'success'
and 'done'. We set the latter when the command is not atomic itself, but
consists of several atomic steps, some of which were probably executed
successfully and some failed.

Currently cmd_log can be seen and commands issued only on the shardlord, but
that's easy to change.

Let's get to the actual commands.

add_node(connstring text)
Add node with given connstring to the cluster. Node is assigned unique id. If
node previously contained shardman state from old cluster (not one managed by
current shardlord), this state will be lost.

rm_node(node_id int)
Remove node from the cluster. Its shardman state will be reset. We don't delete
tables with data and foreign tables though.

You can see all cluster nodes at any time by examining shardman.nodes table:
-- active is the normal mode, others needed only for proper node add and removal
CREATE TYPE worker_node_status AS ENUM (
	'active', 'add_in_progress', 'rm_in_progress', 'removed');
CREATE TABLE nodes (
	id serial PRIMARY KEY,
	connstring text NOT NULL UNIQUE,
	worker_status worker_node_status,
	-- While currently we don't support lord and worker roles on one node,
	-- potentially node can be either worker, lord or both, so we need 2 bits.
	-- One bool with NULL might be fine, but it seems a bit counter-intuitive.
	worker bool NOT NULL DEFAULT true,
	lord bool NOT NULL DEFAULT false,
	-- cmd by which node was added
	added_by bigint REFERENCES shardman.cmd_log(id)
);

create_hash_partitions(
	node_id int, relation text, expr text, partitions_count int,
	rebalance bool DEFAULT true)
Hash-shard table 'relation' lying on node 'node_id' by key 'expr', creating
'partitions_count' shards. As you probably noticed, the signature mirrors
pathman's function with the same name. If 'rebalance' is false, we just
partition table locally, making other nodes aware about it. If it is true,
we also immediately run 'rebalance' function on the table to distibute
partitions, see below.

There are two tables describing sharded tables (no pun intended) state, shardman.tables and shardman.partitions:
CREATE TABLE tables (
	relation text PRIMARY KEY, -- table name
	expr text NOT NULL,
	partitions_count int NOT NULL,
	create_sql text NOT NULL, -- sql to create the table
	-- Node on which table was partitioned at the beginning. Used only during
	-- initial tables inflation to distinguish between table owner and other
	-- nodes, probably cleaner to keep it in separate table.
	initial_node int NOT NULL REFERENCES nodes(id)
);
-- Primary shard and its replicas compose a doubly-linked list: nxt refers to
-- the node containing next replica, prv to node with previous replica (or
-- primary, if we are the first replica). If prv is NULL, this is primary
-- replica. We don't number parts separately since we are not ever going to
-- allow several copies of the same partition on one node.
CREATE TABLE partitions (
	part_name text,
	owner int NOT NULL REFERENCES nodes(id), -- node on which partition lies
	prv int REFERENCES nodes(id),
	nxt int REFERENCES nodes(id),
	relation text NOT NULL REFERENCES tables(relation),
	PRIMARY KEY (part_name, owner)
);

move_part(part_name text, dest int, src int DEFAULT NULL)
Move shard 'part_name' from node 'dest' to node 'src'. If src is NULL, primary
shard is moved. Cmd fails if there is already replica of this shard on 'dest'.

create_replica(part_name text, dest int)
Create replica of shard 'part_name' on node 'dest'. Cmd fails if there is already
replica of this shard on 'dest'.

rebalance(relation text)
Evenly distribute partitions of table 'relation' across all nodes. Currently
this is pretty dumb function, it just tries to move each shard once to node
choosen in round-robin manner, completely ignoring current distribution. Since
dest node can already have replica of this partition, it is not uncommon to see
warnings about failed moves during execution. After completion cmd status is
'done', not 'success'.

set_replevel(relation text, replevel int)
Add replicas to shards of sharded table 'relation' until we reach replevel
replicas for each one. Replica deletions is not implemented yet. Note that it is
pointless to set replevel to more than number of active workers - 1 since we
don't forbid several replicas on one node. Nodes for replicas are choosen
randomly. As in 'rebalance', we are fully oblivious about current shards
distribution, so you will see a bunch of warnings about failing replica
creation -- one for each time random chooses node with already existing replica.

Sharded tables dropping, as well as replica deletion is not implemented yet.

Limitations:
* We are bound to Linux since we use epoll, select should be added.
* We can't switch shardlord for now.
* The shardlord itself can't be worker node for now.
* ALTER TABLE for sharded tables is not supported.

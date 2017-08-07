/* ------------------------------------------------------------------------
 *
 * init.sql
 *   Commands infrastructure, interface funcs, common utility funcs.
 *
 * ------------------------------------------------------------------------
 */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_shardman" to load this file. \quit

-- Functions here use some gucs defined in .so, so we have to ensure that the
-- library is actually loaded.
DO $$
BEGIN
-- Yes, malicious user might have another extension containing 'pg_shardman'...
-- Probably better just call no-op func from the library
	IF strpos(current_setting('shared_preload_libraries'), 'pg_shardman') = 0 THEN
		RAISE EXCEPTION 'pg_shardman must be loaded via shared_preload_libraries. Refusing to
						proceed.';
	END IF;
END
$$;

-- available commands
CREATE TYPE cmd AS ENUM ('add_node', 'rm_node', 'create_hash_partitions',
						 'move_primary', 'create_replica');
-- command status
CREATE TYPE cmd_status AS ENUM ('waiting', 'canceled', 'failed', 'in progress',
								'success');

CREATE TABLE cmd_log (
	id bigserial PRIMARY KEY,
	cmd_type cmd NOT NULL,
	status cmd_status DEFAULT 'waiting' NOT NULL
);

-- Notify shardman master bgw about new commands
CREATE FUNCTION notify_shardmaster() RETURNS trigger AS $$
BEGIN
	NOTIFY shardman_cmd_log_update;
	RETURN NULL;
END
$$ LANGUAGE plpgsql;
CREATE TRIGGER cmd_log_inserts
	AFTER INSERT ON cmd_log
	FOR EACH STATEMENT EXECUTE PROCEDURE notify_shardmaster();

-- probably better to keep opts in an array field, but working with arrays from
-- libpq is not very handy
-- opts must be inserted sequentially, we order by them by id
CREATE TABLE cmd_opts (
	id bigserial PRIMARY KEY,
	cmd_id bigint REFERENCES cmd_log(id),
	opt text NOT NULL
);

-- Interface functions

-- Add a node. Its state will be reset, all shardman data lost.
CREATE FUNCTION add_node(connstring text) RETURNS void AS $$
DECLARE
	c_id int;
BEGIN
	INSERT INTO @extschema@.cmd_log VALUES (DEFAULT, 'add_node')
										   RETURNING id INTO c_id;
	INSERT INTO @extschema@.cmd_opts VALUES (DEFAULT, c_id, connstring);
END
$$ LANGUAGE plpgsql;

-- Remove node. Its state will be reset, all shardman data lost.
CREATE FUNCTION rm_node(node_id int) RETURNS void AS $$
DECLARE
	c_id int;
BEGIN
	INSERT INTO @extschema@.cmd_log VALUES (DEFAULT, 'rm_node')
										   RETURNING id INTO c_id;
	INSERT INTO @extschema@.cmd_opts VALUES (DEFAULT, c_id, node_id);
END
$$ LANGUAGE plpgsql;

-- Shard table with hash partitions. Params as in pathman, except for relation
-- (master doesn't know oid of the table)
CREATE FUNCTION create_hash_partitions(
	node_id int, expr text, relation text, partitions_count int)
	RETURNS void AS $$
DECLARE
	c_id int;
BEGIN
	INSERT INTO @extschema@.cmd_log VALUES (DEFAULT, 'create_hash_partitions')
										   RETURNING id INTO c_id;
	INSERT INTO @extschema@.cmd_opts VALUES (DEFAULT, c_id, node_id);
	INSERT INTO @extschema@.cmd_opts VALUES (DEFAULT, c_id, expr);
	INSERT INTO @extschema@.cmd_opts VALUES (DEFAULT, c_id, relation);
	INSERT INTO @extschema@.cmd_opts VALUES (DEFAULT, c_id, partitions_count);
END
$$ LANGUAGE plpgsql;

-- Move master partition to another node. Params:
-- 'part_name' is name of the partition to move
-- 'dest' is id of the destination node
CREATE FUNCTION move_primary(part_name text, dest int) RETURNS int AS $$
DECLARE
	c_id int;
BEGIN
	INSERT INTO @extschema@.cmd_log VALUES (DEFAULT, 'move_primary')
										   RETURNING id INTO c_id;
	INSERT INTO @extschema@.cmd_opts VALUES (DEFAULT, c_id, part_name);
	INSERT INTO @extschema@.cmd_opts VALUES (DEFAULT, c_id, dest);
	RETURN c_id;
END $$ LANGUAGE plpgsql;

-- Create replica partition. Params:
-- 'part_name' is name of the partition to replicate
-- 'dest' is id of the node on which part will be created
CREATE FUNCTION create_replica(part_name text, dest int) RETURNS int AS $$
DECLARE
	c_id int;
BEGIN
	INSERT INTO @extschema@.cmd_log VALUES (DEFAULT, 'create_replica')
										   RETURNING id INTO c_id;
	INSERT INTO @extschema@.cmd_opts VALUES (DEFAULT, c_id, part_name);
	INSERT INTO @extschema@.cmd_opts VALUES (DEFAULT, c_id, dest);
	RETURN c_id;
END $$ LANGUAGE plpgsql;

-- Move primary or replica partition to another node. Params:
-- 'part_name' is name of the partition to move
-- 'src' is id of the node with partition
-- 'dest' is id of the destination node
CREATE FUNCTION move_part(part_name text, src int, dest int) RETURNS int AS $$
DECLARE
	c_id int;
BEGIN
	INSERT INTO @extschema@.cmd_log VALUES (DEFAULT, 'move_primary')
										   RETURNING id INTO c_id;
	INSERT INTO @extschema@.cmd_opts VALUES (DEFAULT, c_id, part_name);
	INSERT INTO @extschema@.cmd_opts VALUES (DEFAULT, c_id, dest);
	RETURN c_id;
END $$ LANGUAGE plpgsql;

-- Internal functions

-- Called on shardmaster bgw start. Add itself to nodes table, set id, create
-- publication.
CREATE FUNCTION master_boot() RETURNS void AS $$
DECLARE
	-- If we have never booted as a master before, we have a work to do
	init_master bool DEFAULT false;
	master_connstring text;
	master_id int;
BEGIN
	raise INFO 'Booting master';
	PERFORM shardman.create_meta_pub();

	master_id := shardman.get_node_id();
	IF master_id IS NULL THEN
		SELECT pg_settings.setting into master_connstring from pg_settings
			WHERE NAME = 'shardman.master_connstring';
		EXECUTE format(
			'INSERT INTO @extschema@.nodes VALUES (DEFAULT, %L, NULL, false, true)
			RETURNING id', master_connstring) INTO master_id;
		PERFORM shardman.set_node_id(master_id);
		init_master := true;
	ELSE
		EXECUTE 'SELECT NOT (SELECT master FROM shardman.nodes WHERE id = $1)'
			INTO init_master USING master_id;
		EXECUTE 'UPDATE shardman.nodes SET master = true WHERE id = $1' USING master_id;
	END IF;
	IF init_master THEN
		-- TODO: set up lr channels
	END IF;
END $$ LANGUAGE plpgsql;

-- These tables will be replicated to worker nodes, notifying them about changes.
-- Called on master.
CREATE FUNCTION create_meta_pub() RETURNS void AS $$
BEGIN
	IF NOT EXISTS (SELECT * FROM pg_publication WHERE pubname = 'shardman_meta_pub') THEN
		CREATE PUBLICATION shardman_meta_pub FOR TABLE
			shardman.nodes, shardman.tables, shardman.partitions;
	END IF;
END;
$$ LANGUAGE plpgsql;

-- Recreate logical pgoutput replication slot. Drops existing slot.
CREATE FUNCTION create_repslot(slot_name text) RETURNS void AS $$
BEGIN
	PERFORM shardman.drop_repslot(slot_name);
	EXECUTE format('SELECT pg_create_logical_replication_slot(%L, %L)',
				   slot_name, 'pgoutput');
END
$$ LANGUAGE plpgsql;

-- Drop replication slot, if it exists.
-- About 'hard' option: we can't just drop replication slots because
-- pg_drop_replication_slot will bail out with ERROR if connection is active.
-- Therefore the caller must either ensure that the connection is dead (e.g.
-- drop subscription on far end) or pass 'true' to 'with_fire' option, which does
-- the following dirty hack. It kills twice active walsender with 1 second
-- interval. After the first kill, replica will immediately try to reconnect,
-- so the connection resurrects instantly. However, if we kill it second time,
-- replica won't try to reconnect until wal_retrieve_retry_interval after its
-- first reaction passes, which is 5 secs by default. Of course, this is not
-- reliable and should be redesigned.
CREATE FUNCTION drop_repslot(slot_name text, with_fire bool DEFAULT false)
	RETURNS void AS $$
DECLARE
	slot_exists bool;
BEGIN
	RAISE DEBUG '[SHARDMAN] Dropping repslot %', slot_name;
	EXECUTE format('SELECT EXISTS (SELECT * FROM pg_replication_slots
				   WHERE slot_name = %L)', slot_name) INTO slot_exists;
	IF slot_exists THEN
		IF with_fire THEN -- kill walsender twice
			RAISE DEBUG '[SHARDMAN] Killing repslot % with fire', slot_name;
			PERFORM shardman.terminate_repslot_walsender(slot_name);
			PERFORM pg_sleep(1);
			PERFORM shardman.terminate_repslot_walsender(slot_name);
		END IF;
		EXECUTE format('SELECT pg_drop_replication_slot(%L)', slot_name);
	END IF;
END
$$ LANGUAGE plpgsql STRICT;
CREATE FUNCTION terminate_repslot_walsender(slot_name text) RETURNS void AS $$
BEGIN
	EXECUTE format('SELECT pg_terminate_backend(active_pid) FROM
				   pg_replication_slots WHERE slot_name = %L', slot_name);
END $$ LANGUAGE plpgsql STRICT;

-- Drop with fire repslot and publication with the same name. Useful for 1-to-1
-- pub-sub.
CREATE FUNCTION drop_repslot_and_pub(pub name) RETURNS void AS $$
BEGIN
	EXECUTE format('DROP PUBLICATION IF EXISTS %I', pub);
	PERFORM shardman.drop_repslot(pub, true);
END $$ LANGUAGE plpgsql STRICT;

-- If sub exists, disable it, detach repslot from it and possibly drop. We
-- manage repslots ourselves, so it is essential to detach rs before dropping
-- sub, and repslots can't be detached while subscription is active.
CREATE FUNCTION eliminate_sub(subname name, drop_sub bool DEFAULT true)
	RETURNS void AS $$
DECLARE
	sub_exists bool;
BEGIN
	EXECUTE format('SELECT count(*) > 0 FROM pg_subscription WHERE subname
				   = %L', subname) INTO sub_exists;
	IF sub_exists THEN
		EXECUTE format('ALTER SUBSCRIPTION %I DISABLE', subname);
		EXECUTE format('ALTER SUBSCRIPTION %I SET (slot_name = NONE)', subname);
		IF drop_sub THEN
			EXECUTE format('DROP SUBSCRIPTION %I', subname);
		END IF;
	END IF;
END $$ LANGUAGE plpgsql STRICT;

-- Remove all our logical replication stuff in case of drop extension.
-- Dropping extension cleanup is not that easy:
--  - pg offers event triggers sql_drop, dd_command_end and ddl_command_start
--  - sql_drop looks like what we need, but we we can't do it from deleting
--    extension itself -- the trigger will be already deleted at the moment we
--    need it.
--  - same with dd_command_end
--  - ddl_command_start apparently doesn't provide us with info what exactly
--    is happening, I mean its impossible to learn with plpgsql what extension
--    is deleting.
--  - because of that I resort to C function which examines parse tree and if
--    it is our extension is being deleted, it calls plpgsql cleanup func
CREATE OR REPLACE FUNCTION pg_shardman_cleanup(drop_subs bool DEFAULT true)
	RETURNS void AS $$
DECLARE
	pub record;
	sub record;
	rs record;
BEGIN
	FOR pub IN SELECT pubname FROM pg_publication WHERE pubname LIKE 'shardman_%' LOOP
		EXECUTE format('DROP PUBLICATION %I', pub.pubname);
	END LOOP;
	FOR sub IN SELECT subname FROM pg_subscription WHERE subname LIKE 'shardman_%' LOOP
		PERFORM shardman.eliminate_sub(sub.subname, drop_subs);
	END LOOP;
	-- TODO: drop repslots gracefully? For that we should iterate over all active
	-- subscribers and turn off subscriptions first.
	FOR rs IN SELECT slot_name FROM pg_replication_slots
		WHERE slot_name LIKE 'shardman_%' AND slot_type = 'logical' LOOP
		PERFORM shardman.drop_repslot(rs.slot_name, true);
	END LOOP;

	PERFORM shardman.reset_node_id();
END;
$$ LANGUAGE plpgsql;
CREATE FUNCTION pg_shardman_cleanup_c() RETURNS event_trigger
    AS 'pg_shardman' LANGUAGE C;
CREATE EVENT TRIGGER cleanup_lr_trigger ON ddl_command_start
	WHEN TAG in ('DROP EXTENSION')
	EXECUTE PROCEDURE pg_shardman_cleanup_c();

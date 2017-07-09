-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_shardman" to load this file. \quit

-- Functions here use some gucs defined in .so, so we have to ensure that the
-- library is actually loaded.
DO $$
BEGIN
-- Yes, malicious user might have another extension containing 'pg_shardman'...
-- Probably better just call no-op func from the library
    IF strpos(current_setting('shared_preload_libraries'), 'pg_shardman') = 0 THEN
        RAISE EXCEPTION 'pg_shardman must be loaded via shared_preload_libraries. Refusing to proceed.';
    END IF;
END
$$;

-- active is the normal mode, others needed only for proper node add and removal
CREATE TYPE node_status AS ENUM ('active', 'add_in_progress', 'rm_in_progress');

-- list of nodes present in the cluster
CREATE TABLE nodes (
	id serial PRIMARY KEY,
	connstring text,
	status node_status NOT NULL
);

-- Master is removing us, so reset our state, removing all subscriptions. A bit
-- tricky part: we can't DROP SUBSCRIPTION here, because that would mean
-- shooting (sending SIGTERM) ourselvers (to replication apply worker) in the
-- leg.  So for now we just disable subscription, worker will stop after the end
-- of transaction. Later we should delete subscriptions fully.
CREATE FUNCTION rm_node_worker_side() RETURNS TRIGGER AS $$
BEGIN
	PERFORM shardman.pg_shardman_cleanup(false);
	RETURN NULL;
END
$$ language plpgsql;
CREATE TRIGGER rm_node_worker_side AFTER UPDATE ON shardman.nodes
	FOR EACH ROW WHEN (OLD.status = 'active' AND NEW.status = 'rm_in_progress')
	EXECUTE PROCEDURE rm_node_worker_side();
-- fire trigger only on worker nodes
ALTER TABLE shardman.nodes ENABLE REPLICA TRIGGER rm_node_worker_side;

-- Currently it is used just to store node id, in general we can keep any local
-- node metadata here. If is ever used extensively, probably hstore suits better.
CREATE TABLE local_meta (
	k text NOT NULL, -- key
	v text -- value
);
INSERT INTO @extschema@.local_meta VALUES ('node_id', NULL);

-- available commands
CREATE TYPE cmd AS ENUM ('add_node', 'rm_node');
-- command status
CREATE TYPE cmd_status AS ENUM ('waiting', 'canceled', 'failed', 'in progress', 'success');

CREATE TABLE cmd_log (
	id bigserial PRIMARY KEY,
	cmd_type cmd NOT NULL,
	status cmd_status DEFAULT 'waiting' NOT NULL,
	-- only for add_node cmd -- generated id for newly added node. Exists only
	-- when node adding is in progress or node is active. Cleaner to keep this
	-- in separate table...
	node_id int REFERENCES nodes(id)
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
	FOR EACH STATEMENT
	EXECUTE PROCEDURE notify_shardmaster();


-- probably better to keep opts in an array field, but working with arrays from
-- libpq is not very handy
-- opts must be inserted sequentially, we order by them by id
CREATE TABLE cmd_opts (
	id bigserial PRIMARY KEY,
	cmd_id bigint REFERENCES cmd_log(id),
	opt text NOT NULL
);


-- Internal functions

-- These tables will be replicated to worker nodes, notifying them about changes.
-- Called on master.
CREATE FUNCTION create_meta_pub() RETURNS void AS $$
BEGIN
	IF NOT EXISTS (SELECT * FROM pg_publication WHERE pubname = 'shardman_meta_pub') THEN
		CREATE PUBLICATION shardman_meta_pub FOR TABLE shardman.nodes;
	END IF;
END;
$$ LANGUAGE plpgsql;

-- These tables will be replicated to worker nodes, notifying them about changes.
-- Called on worker nodes.
CREATE FUNCTION create_meta_sub() RETURNS void AS $$
DECLARE
	master_connstring text;
BEGIN
	SELECT pg_settings.setting into master_connstring from pg_settings
		WHERE NAME = 'shardman.master_connstring';
	-- Note that 'CONNECTION $1...' USING master_connstring won't work here
	EXECUTE format('CREATE SUBSCRIPTION shardman_meta_sub CONNECTION %L PUBLICATION shardman_meta_pub', master_connstring);
END;
$$ LANGUAGE plpgsql;

-- If for cmd cmd_id we haven't yet inserted new node, do that; mark it as passive
-- for now, we still need to setup lr and set its id on the node itself
-- Return generated or existing node id
CREATE FUNCTION insert_node(connstring text, cmd_id bigint) RETURNS int AS $$
DECLARE
	n_id int;
BEGIN
	SELECT node_id FROM @extschema@.cmd_log INTO n_id WHERE id = cmd_id;
	IF n_id IS NULL THEN
		INSERT INTO @extschema@.nodes
			VALUES (DEFAULT, quote_literal(connstring), 'add_in_progress')
			RETURNING id INTO n_id;
		UPDATE @extschema@.cmd_log SET node_id = n_id WHERE id = cmd_id;
	END IF;
	RETURN n_id;
END
$$ LANGUAGE plpgsql;

-- Create logical pgoutput replication slot, if not exists
CREATE FUNCTION create_repslot(slot_name text) RETURNS void AS $$
DECLARE
	slot_exists bool;
BEGIN
	EXECUTE format('SELECT EXISTS (SELECT * FROM pg_replication_slots
				   WHERE slot_name=%L)', slot_name) INTO slot_exists;
	IF NOT slot_exists THEN
		EXECUTE format('SELECT pg_create_logical_replication_slot(%L, %L)',
					   slot_name, 'pgoutput');
	END IF;
END
$$ LANGUAGE plpgsql;

-- Drop replication slot, if it exists
CREATE FUNCTION drop_repslot(slot_name text) RETURNS void AS $$
DECLARE
	slot_exists bool;
BEGIN
	EXECUTE format('SELECT EXISTS (SELECT * FROM pg_replication_slots
				   WHERE slot_name=%L)', slot_name) INTO slot_exists;
	IF slot_exists THEN
		EXECUTE format('SELECT pg_drop_replication_slot(%L)', slot_name);
	END IF;
END
$$ LANGUAGE plpgsql;

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
--    it is our extension is deleting, it calls plpgsql cleanup func
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
		-- we are managing rep slots manually, so we need to detach it beforehand
		EXECUTE format('ALTER SUBSCRIPTION %I DISABLE', sub.subname);
		EXECUTE format('ALTER SUBSCRIPTION %I SET (slot_name = NONE)', sub.subname);
		IF drop_subs THEN
			EXECUTE format('DROP SUBSCRIPTION %I', sub.subname);
		END IF;
	END LOOP;
	FOR rs IN SELECT slot_name FROM pg_replication_slots
		WHERE slot_name LIKE 'shardman_%' AND slot_type = 'logical' LOOP
		EXECUTE format('SELECT pg_drop_replication_slot(%L)', rs.slot_name);
	END LOOP;
END;
$$ LANGUAGE plpgsql;
CREATE FUNCTION pg_shardman_cleanup_c() RETURNS event_trigger
    AS 'pg_shardman' LANGUAGE C;
CREATE EVENT TRIGGER cleanup_lr_trigger ON ddl_command_start
	WHEN TAG in ('DROP EXTENSION')
	EXECUTE PROCEDURE pg_shardman_cleanup_c();

-- Get local node id. NULL means node is not in the cluster yet.
CREATE FUNCTION get_node_id() RETURNS int AS $$
	SELECT v::int FROM @extschema@.local_meta WHERE k = 'node_id';
$$ LANGUAGE sql;

-- Exclude node from the cluster
CREATE FUNCTION reset_node_id() RETURNS void AS $$
	UPDATE @extschema@.local_meta SET v = NULL WHERE k = 'node_id';
$$ LANGUAGE sql;

-- Set local node id.
CREATE FUNCTION set_node_id(node_id int) RETURNS void AS $$
	UPDATE @extschema@.local_meta SET v = node_id WHERE k = 'node_id';
$$ LANGUAGE sql;


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

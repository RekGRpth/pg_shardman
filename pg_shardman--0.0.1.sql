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

-- list of nodes present in the cluster
CREATE TABLE nodes (
	id serial PRIMARY KEY,
	connstring text,
	active bool NOT NULL -- if false, we haven't yet finished adding it
);

-- Currently it is used just to store node id, in general we can keep any local
-- node metadata here. If is ever used extensively, probably hstore suits better.
CREATE TABLE local_meta (
	k text NOT NULL, -- key
	v text -- value
);
INSERT INTO @extschema@.local_meta VALUES ('node_id', NULL);

-- available commands
CREATE TYPE cmd AS ENUM ('add_node', 'remove_node');
-- command status
CREATE TYPE cmd_status AS ENUM ('waiting', 'canceled', 'failed', 'in progress', 'success');

CREATE TABLE cmd_log (
	id bigserial PRIMARY KEY,
	cmd_type cmd NOT NULL,
	status cmd_status DEFAULT 'waiting' NOT NULL,
	-- only for add_node cmd -- generated id for newly added node. Cleaner
	-- to keep that is separate table...
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
		INSERT INTO @extschema@.nodes VALUES (DEFAULT, quote_literal(connstring), false)
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
		EXECUTE format('SELECT * FROM pg_create_logical_replication_slot(%L, %L)',
					   slot_name, 'pgoutput');
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
CREATE OR REPLACE FUNCTION pg_shardman_cleanup() RETURNS void  AS $$
DECLARE
	pub record;
	sub record;
BEGIN
	FOR pub IN SELECT pubname FROM pg_publication WHERE pubname LIKE 'shardman_%' LOOP
		EXECUTE format('DROP PUBLICATION %I', pub.pubname);
	END LOOP;
	FOR sub IN SELECT subname FROM pg_subscription WHERE subname LIKE 'shardman_%' LOOP
		-- we are managing rep slots manually, so we need to detach it beforehand
		EXECUTE format('ALTER SUBSCRIPTION %I DISABLE', sub.subname);
		EXECUTE format('ALTER SUBSCRIPTION %I SET (slot_name = NONE)', sub.subname);
		EXECUTE format('DROP SUBSCRIPTION %I', sub.subname);
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

-- TODO: during the initial connection, ensure that nodes id (if any) is not
-- present in the cluster
CREATE FUNCTION add_node(connstring text) RETURNS void AS $$
DECLARE
	c_id int;
BEGIN
	INSERT INTO @extschema@.cmd_log VALUES (DEFAULT, 'add_node')
										   RETURNING id INTO c_id;
	INSERT INTO @extschema@.cmd_opts VALUES (DEFAULT, c_id, connstring);
END
$$ LANGUAGE plpgsql;

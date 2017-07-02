-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_shardman" to load this file. \quit

-- available commands
CREATE TYPE cmd AS ENUM ('add_node', 'remove_node');
-- command status
CREATE TYPE cmd_status AS ENUM ('waiting', 'canceled', 'failed', 'in progress', 'success');

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

-- list of nodes present in the cluster
CREATE TABLE nodes (
	id serial PRIMARY KEY,
	connstring text
);

-- Currently it is used just to store node id, in general we can keep any local
-- node metadata here. If is ever used extensively, probably hstore suits better.
CREATE TABLE local_meta (
	k text NOT NULL, -- key
	v text -- value
);
INSERT INTO @extschema@.local_meta VALUES ('node_id', NULL);

-- Internal functions

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

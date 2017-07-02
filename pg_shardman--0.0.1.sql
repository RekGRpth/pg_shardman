-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_shardman" to load this file. \quit

-- available commands
CREATE TYPE cmd AS ENUM ('add_node', 'remove_node');
-- command status
CREATE TYPE cmd_status AS ENUM ('waiting', 'canceled', 'in progress', 'success');

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

CREATE FUNCTION pg_shardman_hello() RETURNS VOID AS	$$
BEGIN
	RAISE NOTICE 'Aye, hi from shardman!';
END;
$$ LANGUAGE plpgsql;

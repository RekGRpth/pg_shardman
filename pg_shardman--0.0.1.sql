-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_shardman" to load this file. \quit

-- types & tables

-- available commands
CREATE TYPE cmd AS ENUM ('add_node', 'remove_node');
-- command status
CREATE TYPE cmd_status AS ENUM ('waiting', 'failed', 'success');

CREATE TABLE cmd_log (
	id serial PRIMARY KEY,
	cmd_type cmd NOT NULL,
	cmd_status cmd_status DEFAULT 'waiting' NOT NULL
);

-- probably better to keep opts in an array field, but working with arrays from
-- libpq is not very handy
CREATE TABLE cmd_opts (
	cmd_id int REFERENCES cmd_log(id),
	opt text NOT NULL
);

-- Probably later we should provide as flexible interface as libpq does for
-- specifying connstrings
CREATE TABLE nodes (
	id serial PRIMARY KEY,
	connstring text
);

-- functions

-- TODO: try to connect immediately and ensure that its id (if any) is not
-- present in the cluster
CREATE FUNCTION add_node(connstring text) RETURNS void AS $$
DECLARE
	c_id int;
BEGIN
	INSERT INTO @extschema@.cmd_log VALUES (DEFAULT, 'add_node')
										   RETURNING id INTO c_id;
	INSERT INTO @extschema@.cmd_opts VALUES (c_id, connstring);
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION pg_shardman_hello() RETURNS VOID AS	$$
BEGIN
	RAISE NOTICE 'Aye, hi from shardman!';
END;
$$ LANGUAGE plpgsql;

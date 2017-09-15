/* ------------------------------------------------------------------------
 *
 * init.sql
 *		Handling nodes addition and removal.
 *
 * Copyright (c) 2017, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

-- active is the normal mode, removed means node removed, others needed only
-- for proper node add and removal
CREATE TYPE worker_node_status AS ENUM (
	'active', 'add_in_progress', 'rm_in_progress', 'removed');

-- list of nodes present in the cluster
CREATE TABLE nodes (
	id serial PRIMARY KEY,
	connstring text NOT NULL,
	-- While currently we don't support lord and worker roles on one node,
	-- potentially node can be either worker, lord or both.
	worker_status worker_node_status, -- NULL if this node is not a worker
	-- is this node shardlord?
	shardlord bool NOT NULL DEFAULT false,
	-- cmd by which node was added
	added_by bigint REFERENCES shardman.cmd_log(id)
);
CREATE UNIQUE INDEX unique_node_connstr ON shardman.nodes (connstring)
	WHERE (worker_status = 'active' OR shardlord);

-- Lord is removing us, so reset our state, removing all subscriptions. A bit
-- tricky part: we can't DROP SUBSCRIPTION here, because that would mean
-- shooting (sending SIGTERM) ourselvers (to replication apply worker) in the
-- leg.  So for now we just disable subscription, worker will stop after the end
-- of transaction. Later we should delete subscriptions fully.
CREATE FUNCTION rm_node_worker_side() RETURNS TRIGGER AS $$
BEGIN
	IF OLD.id = (SELECT shardman.my_id()) THEN
		RAISE DEBUG '[SHMN] rm_node_worker_side called';
		PERFORM shardman.pg_shardman_cleanup(false);
	END IF;
	RETURN NULL;
END
$$ LANGUAGE plpgsql;
CREATE TRIGGER rm_node_worker_side AFTER UPDATE ON shardman.nodes
	FOR EACH ROW WHEN (OLD.worker_status = 'active' AND NEW.worker_status = 'rm_in_progress')
	EXECUTE PROCEDURE rm_node_worker_side();
-- fire trigger only on worker nodes
ALTER TABLE shardman.nodes ENABLE REPLICA TRIGGER rm_node_worker_side;

-- Currently it is used just to store node id, in general we can keep any local
-- node metadata here. If is ever used extensively, probably hstore suits better.
CREATE TABLE local_meta (
	k text NOT NULL, -- key
	v text -- value
);
INSERT INTO shardman.local_meta VALUES ('my_id', NULL);

-- Get local node id. NULL means node is not in the cluster yet.
CREATE FUNCTION my_id() RETURNS int AS $$
	SELECT v::int FROM shardman.local_meta WHERE k = 'my_id';
$$ LANGUAGE sql;

-- Exclude node from the cluster
CREATE FUNCTION reset_my_id() RETURNS void AS $$
BEGIN
	UPDATE shardman.local_meta SET v = NULL WHERE k = 'my_id';
	PERFORM shardman.reset_my_id_c();
END $$ LANGUAGE plpgsql STRICT;
CREATE FUNCTION reset_my_id_c() RETURNS void
	AS 'pg_shardman' LANGUAGE C;

-- Set local node id.
CREATE FUNCTION set_my_id(my_id int) RETURNS void AS $$
BEGIN
	UPDATE shardman.local_meta SET v = my_id WHERE k = 'my_id';
	PERFORM shardman.set_my_id_c(my_id);
END $$ LANGUAGE plpgsql STRICT;
CREATE FUNCTION set_my_id_c(my_id int) RETURNS void
	AS 'pg_shardman' LANGUAGE C;

-- If for cmd cmd_id we haven't yet inserted new node, do that; mark it as
-- passive for now, we still need to setup lr and set its id on the node itself.
-- Return generated or existing node id.
CREATE FUNCTION insert_node(connstring text, cmd_id bigint) RETURNS int AS $$
DECLARE
	n_id int;
BEGIN
	SELECT nodes.id
		FROM shardman.cmd_log c_log, shardman.nodes nodes
	 	WHERE c_log.id = cmd_id AND cmd_id = nodes.added_by
	  	INTO n_id;
	IF n_id IS NULL THEN
		INSERT INTO shardman.nodes
			VALUES (DEFAULT, connstring, 'add_in_progress', false, cmd_id)
			RETURNING id INTO n_id;
	END IF;
	RETURN n_id;
END
$$ LANGUAGE plpgsql;

-- Get local node connstr regardless of its state. Returns NULL if node is not
-- in cluster and never was in one.
CREATE FUNCTION my_connstr() RETURNS text AS $$
BEGIN
	RETURN connstring FROM shardman.nodes WHERE id = shardman.my_id();
END $$ LANGUAGE plpgsql;
-- Same, but throw ERROR there is no connstring
CREATE FUNCTION my_connstr_strict() RETURNS text AS $$
DECLARE
	connstr text := shardman.my_connstr();
BEGIN
	IF connstr IS NULL THEN
		RAISE EXCEPTION '[SHMN] Node id % is not in cluster', shardman.my_id();
	END IF;
	RETURN connstr;
END $$ LANGUAGE plpgsql;


-- Get connstr of worker node with id node_id. ERROR is raised if there isn't
-- one.
CREATE FUNCTION get_worker_node_connstr(node_id int) RETURNS text AS $$
DECLARE
	connstr text := connstring FROM shardman.nodes WHERE id = node_id AND
				worker_status IS NOT NULL;
BEGIN
	IF connstr IS NULL THEN
		RAISE EXCEPTION '[SHMN] Worker node with id % not found', node_id;
	END IF;
	RETURN connstr;
END $$ LANGUAGE plpgsql STRICT;

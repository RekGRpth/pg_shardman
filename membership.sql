/* ------------------------------------------------------------------------
 *
 * init.sql
 *		Handling nodes addition and removal.
 *
 * ------------------------------------------------------------------------
 */

-- active is the normal mode, others needed only for proper node add and removal
CREATE TYPE worker_node_status AS ENUM ('active', 'add_in_progress', 'rm_in_progress');

-- list of nodes present in the cluster
CREATE TABLE nodes (
	id serial PRIMARY KEY,
	connstring text NOT NULL UNIQUE,
	worker_status worker_node_status,
	-- While currently we don't support master and worker roles on one node,
	-- potentially node can be either worker, master or both, so we need 2 bits.
	-- One bool with NULL might be fine, but it seems a bit counter-intuitive.
	worker bool NOT NULL DEFAULT true,
	master bool NOT NULL DEFAULT false,
	-- cmd by which node was added
	added_by bigint REFERENCES shardman.cmd_log(id)
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
INSERT INTO shardman.local_meta VALUES ('node_id', NULL);

-- Get local node id. NULL means node is not in the cluster yet.
CREATE FUNCTION get_node_id() RETURNS int AS $$
	SELECT v::int FROM shardman.local_meta WHERE k = 'node_id';
$$ LANGUAGE sql;

-- Exclude node from the cluster
CREATE FUNCTION reset_node_id() RETURNS void AS $$
BEGIN
	UPDATE shardman.local_meta SET v = NULL WHERE k = 'node_id';
	PERFORM shardman.reset_node_id_c();
END $$ LANGUAGE plpgsql STRICT;
CREATE FUNCTION reset_node_id_c() RETURNS void
	AS 'pg_shardman' LANGUAGE C;

-- Set local node id.
CREATE FUNCTION set_node_id(node_id int) RETURNS void AS $$
BEGIN
	UPDATE shardman.local_meta SET v = node_id WHERE k = 'node_id';
	PERFORM shardman.set_node_id_c(node_id);
END $$ LANGUAGE plpgsql STRICT;
CREATE FUNCTION set_node_id_c(node_id int) RETURNS void
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
			VALUES (DEFAULT, connstring, 'add_in_progress', true, false, cmd_id)
			RETURNING id INTO n_id;
	END IF;
	RETURN n_id;
END
$$ LANGUAGE plpgsql;

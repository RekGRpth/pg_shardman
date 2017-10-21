/* ------------------------------------------------------------------------
 *
 * init.sql
 *   Commands infrastructure, interface funcs, common utility funcs.
 *
 * Copyright (c) 2017, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_shardman" to load this file. \quit



-- Interface functions




-- Add a node. Its state will be reset, all shardman data lost.
CREATE FUNCTION add_node(conn_string text, repl_group text) RETURNS void AS $$
DECLARE
	new_node_id integer;
    node    nodes;
	lord_connstr text;
	pubs text := '';
	subs text := '';
	sync text := '';
	lord_id integer;
BEGIN
	INSERT INTO nodes (connstring,worker_status,replication_group) values (conn_string, 'active',repl_group) returning id into new_node_id;

	SELECT string_agg('node_'||id, ',') INTO sync_stanbys from nodes WHERE NOT shardlord;

	FOR node IN SELECT * FROM nodes WHERE replication_group=repl_group AND id<>new_node_id AND NOT shardlord
	LOOP
		pubs := format('%s%s:CREATE PUBLICATION node_%s;
			 			  %s:CREATE PUBLICATION node_%s;',
			pubs, node.id, new_node_id,
				  new_node_id, node.id);
		subs := format('%s%s:CREATE SUBSCRIPTION node_%s CONNECTION %L PUBLICATION node_%s with (synchronous_commit = local);
			 			  %s:CREATE SUBSCRIPTION node_%s CONNECTION %L PUBLICATION node_%s with (synchronous_commit = local);',
			 subs, node.id, new_node_id, conn_string, node.id);
			 	   new_node_id, node.id, node.connstring, new_node_id);
		sync := format('%s%s:SELECT shardman_set_sync_standbys(%L);', sync, sync_stanbys);
	END LOOP;

	sync := format('%s%s:SELECT shardman_set_sync_standbys(%L);', sync, new_node_id, sync_stanbys);

	lord_id := shardman.my_id();
	SELECT connstring INTO lord_connstr FROM nodes WHERE id=lord_id;
	subs := format('%s%s:CREATE SUBSCRIPTION shardman_meta_sub CONNECTION %L PUBLICATION shardman_meta_pub with (synchronous_commit = local);',
		 subs, new_node_id, connstring);

    PERFORM sharman.broadcast(pubs);
	PERFORM sharman.broadcast(subs);
	PERFORM sharman.broadcast(sync);
END
$$ LANGUAGE plpgsql;


-- Remove node. Its state will be reset, all shardman data lost.
CREATE FUNCTION rm_node(rm_node_id int, force bool DEFAULT false) RETURNS void AS $$
DECLARE
	n_parts integer;
	part partitions;
	pubs text := '';
	subs text := '';
	fdws text := '';
    new_master_id integer;
BEGIN
	IF NOT force THEN
	   SELECT count(*) INFO n_parts FROM partitions WHERE node=rm_node_id;
	   IF (n_parts <> 0) THEN
	   	  RAISE ERROR 'Use forse=true to remove non-empty node';
	   END IF;
    END IF;

	SELECT string_agg('node_'||id, ',') INTO sync_stanbys from nodes WHERE NOT shardlord AND is<>rm_node_id;

	-- remove all subscriptions of this node
    FOR node IN SELECT * FROM nodes WHERE replication_group=repl_group AND id<>rm_node_id AND NOT shardlord
	LOOP
		pubs := format('%s%s:DROP PUBLICATION node_%s;
			 			  %s:DROP PUBLICATION node_%s;',
			 pubs, node.id, rm_node_id,
			 	   rm_node_id, node.id);
		subs := format('%s%s:DROP SUBSCRIPTION node_%s;
			 			  %s:DROP SUBSCRIPTION node_%s',
			 subs, rm_node_id, node.id,
			 	   node.id, rm_node_id);
		sync := format('%s%s:SELECT shardman_set_sync_standbys(%L);', sync, sync_stanbys);
	END LOOP;

    subs := format('%s%s:DROP SUBSCRIPTION shardman_meta_sub;',
		 subs, rm_node_id, connstring);

	PERFORM sharman.broadcast(subs, ignore_errors:=true);
    PERFORM sharman.broadcast(pubs, ignore_errors:=true);
	PERFORM sharman.broadcast(sync, ignore_errors:=true);

	FOR part in SELECT * from shardman.partitions where node=rm_node_id
	LOOP
		SELECT node INTO new_master_id FROM shardman.replicas WHERE part_name=part.part_name;
		IF new_master_id IS NOT NULL
		THEN -- exists some replica for this node: redirect foreign table to this replica and refresh LR channels for this replication group
		    UPDATE shardman.partitions SET node=new_master_id WHERE part_name=part.part_name;
			DELETE FROM sharaman.replicas WHERE part_name=part.part_name AND node=new_master_id;

			pubs := '';
			subs := '';
			FOR repl in SELECT * FROM sharman.replicas WHERE part_name=part.part_name
			LOOP
			    pubs := format('%s%s:ALTER PUBLICATION node_%s ADD TABLE %I;',
				    pubs, new_master_id, repl.node, part.part_name);
				subs := format('%s%s:ALTER SUBSCRIPTION node_%s REFRESH PUBLICATION',
					 subs, repl.node, new_master_id);
			END LOOP;

			PERFORM sharman.broadcast(subs);
			PERFORM sharman.broadcast(pubs);
		ELSE -- there is no replica: we have to create new empty partition and redirect all FDWs to it
			SELECT id INTO new_master_id FROM nodes WHERE NOT shardlord AND id<>rm_node_id ORDER BY random() LIMIT 1;
		    INSERT INTO shardman.partitions (part_name,node,relation) VALUES (part.part.name,new_master_id,part.relation);
		END IF;

		FOR node IN SELECT * FROM nodes WHERE id<>rm_node_id AND NOT shardlord
		LOOP
			IF id=new_master_id THEN
			    fdws := format('%s%d:SELECT replace_hash_partition(''%s_fdw'',''%s'');',
			 		fdws, node.id, part.part_name, part.part_name);
			ELSE
				fdws := format('%s%s:UPDATE pg_foreign_table SET ftserver = (SELECT oid FROM pg_foreign_server WHERE srvname = ''node_%s'') WHERE ftrelid = (SELECT oid FROM pg_class WHERE relname=''%s_fdw'');',
		   			fdws, node.id, new_master_id, part.part_name);
			END IF;
		END LOOP;
	END LOOP;

	PERFORM sharman.broadcast(fdws, ignore_errors:=true);

	DELETE from shardman.nodes where id=rm_node_id CASCADE;
END
$$ LANGUAGE plpgsql;

-- Shard table with hash partitions. Params as in pathman, except for relation
-- (lord doesn't know oid of the table)
CREATE FUNCTION create_hash_partitions(
	node_id int, relation text, expr text, partitions_count int,
	rebalance bool DEFAULT true)
	RETURNS int AS $$
DECLARE
	cmd_id	int;
	cmd		text;
	opts	text[];
BEGIN
	cmd = 'create_hash_partitions';
	opts = ARRAY[node_id::text,
				 relation::text,
				 expr::text,
				 partitions_count::text,
				 rebalance::text];

	cmd_id = @extschema@.register_cmd(cmd, opts);

	-- additional steps must check node's type
	IF @extschema@.me_lord() AND rebalance THEN
		cmd_id = @extschema@.rebalance(relation);
	END IF;

	-- return last command's id
	RETURN cmd_id;
END
$$ LANGUAGE plpgsql;

-- Move primary or replica partition to another node. Params:
-- 'part_name' is name of the partition to move
-- 'dst' is id of the destination node
-- 'src' is id of the node with partition. If NULL, primary partition is moved.
CREATE FUNCTION move_part(part_name text, dst int, src int DEFAULT NULL)
	RETURNS int AS $$
DECLARE
	cmd		text;
	opts	text[];
BEGIN
	cmd = 'move_part';
	opts = ARRAY[part_name::text, dst::text, src::text];

	RETURN @extschema@.register_cmd(cmd, opts);
END
$$ LANGUAGE plpgsql;

-- Create replica partition. Params:
-- 'part_name' is name of the partition to replicate
-- 'dst' is id of the node on which part will be created
CREATE FUNCTION create_replica(part_name text, dst int) RETURNS int AS $$
DECLARE
	cmd		text;
	opts	text[];
BEGIN
	cmd = 'create_replica';
	opts = ARRAY[part_name::text, dst::text];

	RETURN @extschema@.register_cmd(cmd, opts);
END
$$ LANGUAGE plpgsql;

-- Evenly distribute partitions of table 'relation' across all nodes.
CREATE FUNCTION rebalance(relation text) RETURNS int AS $$
DECLARE
	cmd		text;
	opts	text[];
BEGIN
	cmd = 'rebalance';
	opts = ARRAY[relation::text];

	RETURN @extschema@.register_cmd(cmd, opts);
END
$$ LANGUAGE plpgsql;

-- Add replicas to partitions of table 'relation' until we reach replevel
-- replicas for each one. Note that it is pointless to set replevel to more than
-- number of active workers - 1. Replica deletions is not implemented yet.
CREATE FUNCTION set_replevel(relation text, replevel int) RETURNS int AS $$
DECLARE
	cmd		text;
	opts	text[];
BEGIN
	cmd = 'set_replevel';
	opts = ARRAY[relation::text, replevel::text];

	RETURN @extschema@.register_cmd(cmd, opts);
END
$$ LANGUAGE plpgsql STRICT;


-- Internal functions


-- Register command cmd_type for execution on shardlord.
CREATE FUNCTION register_cmd(cmd_type text, cmd_opts text[]) RETURNS int AS $$
DECLARE
	cmd_id int;
BEGIN
	IF NOT @extschema@.me_lord() THEN
		RETURN @extschema@.execute_on_lord_c(cmd_type, cmd_opts);
	END IF;

	INSERT INTO @extschema@.cmd_log
		VALUES (DEFAULT, cmd_type, cmd_opts)
		RETURNING id INTO cmd_id;

	NOTIFY shardman_cmd_log_update; -- Notify bgw about the job

	RETURN cmd_id;
END
$$ LANGUAGE plpgsql STRICT;

-- Called on shardlord bgw start. Add itself to nodes table, set id, create
-- publication.
CREATE FUNCTION lord_boot() RETURNS void AS $$
DECLARE
	-- If we have never booted as a lord before, we have a work to do
	init_lord bool DEFAULT false;
	lord_connstring text;
	lord_id int;
BEGIN
	RAISE INFO '[SHMN] Booting lord';
	PERFORM shardman.create_meta_pub();

	lord_id := shardman.my_id();
	IF lord_id IS NULL THEN
		SELECT pg_settings.setting INTO lord_connstring FROM pg_settings
			WHERE NAME = 'shardman.shardlord_connstring';
		EXECUTE format(
			'INSERT INTO shardman.nodes VALUES (DEFAULT, %L, NULL, true)
			RETURNING id', lord_connstring) INTO lord_id;
		PERFORM shardman.set_my_id(lord_id);
		PERFORM shardman.alter_system_c('shardman.my_id', lord_id::text);
		PERFORM pg_reload_conf();
		init_lord := true;
	ELSE
		EXECUTE 'SELECT NOT (SELECT shardlord FROM shardman.nodes WHERE id = $1)'
			INTO init_lord USING lord_id;
		EXECUTE 'UPDATE shardman.nodes SET shardlord = true WHERE id = $1'
			USING lord_id;
	END IF;
	IF init_lord THEN
		-- TODO: set up lr channels
	END IF;
END
$$ LANGUAGE plpgsql;

-- These tables will be replicated to worker nodes, notifying them about changes.
-- Called on lord.
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
-- drop subscription on far end) or pass 'true' to 'with_fire' option, which
-- does the following dirty hack. It kills several times active walsender with 1
-- second interval. After the first kill, replica will immediately try to
-- reconnect, so the connection resurrects instantly. However, if we kill it
-- second time, replica won't try to reconnect until wal_retrieve_retry_interval
-- after its first reaction passes, which is 5 secs by default. Of course, this
-- is not reliable and should be redesigned.
CREATE FUNCTION drop_repslot(slot_name text, with_fire bool DEFAULT false)
	RETURNS void AS $$
DECLARE
	slot_exists bool;
	kill_ws_times int := 2;
BEGIN
	RAISE DEBUG '[SHMN] Dropping repslot %', slot_name;
	EXECUTE format('SELECT EXISTS (SELECT * FROM pg_replication_slots
				   WHERE slot_name = %L)', slot_name) INTO slot_exists;
	IF slot_exists THEN
		IF with_fire THEN -- kill walsender several times
			RAISE DEBUG '[SHMN] Killing repslot % with fire', slot_name;
			FOR i IN 1..kill_ws_times LOOP
				RAISE DEBUG '[SHMN] Killing walsender for slot %', slot_name;
				PERFORM shardman.terminate_repslot_walsender(slot_name);
				IF i != kill_ws_times THEN
					PERFORM pg_sleep(1);
				END IF;
			END LOOP;
		END IF;
		EXECUTE format('SELECT pg_drop_replication_slot(%L)', slot_name);
	END IF;
END
$$ LANGUAGE plpgsql STRICT;
CREATE FUNCTION terminate_repslot_walsender(slot_name text) RETURNS void AS $$
BEGIN
	EXECUTE format('SELECT pg_terminate_backend(active_pid) FROM
				   pg_replication_slots WHERE slot_name = %L', slot_name);
END
$$ LANGUAGE plpgsql STRICT;

-- Drop with fire repslot and publication with the same name. Useful for 1-to-1
-- pub-sub.
CREATE FUNCTION drop_repslot_and_pub(pub name) RETURNS void AS $$
BEGIN
	EXECUTE format('DROP PUBLICATION IF EXISTS %I', pub);
	PERFORM shardman.drop_repslot(pub, true);
END
$$ LANGUAGE plpgsql STRICT;

-- If sub exists, disable it, detach repslot from it and possibly drop. We
-- manage repslots ourselves, so it is essential to detach rs before dropping
-- sub, and repslots can't be detached while subscription is active.
CREATE FUNCTION eliminate_sub(subname name, drop_sub bool DEFAULT true)
	RETURNS void AS $$
DECLARE
	sub_exists bool;
BEGIN
	RAISE DEBUG '[SHMN] eliminating sub %, drop_sub %', subname, drop_sub;
	EXECUTE format('SELECT EXISTS (SELECT 1 FROM pg_subscription WHERE subname
				   = %L)', subname) INTO sub_exists;
	IF sub_exists THEN
		EXECUTE format('ALTER SUBSCRIPTION %I DISABLE', subname);
		EXECUTE format('ALTER SUBSCRIPTION %I SET (slot_name = NONE)', subname);
		IF drop_sub THEN
			EXECUTE format('DROP SUBSCRIPTION %I', subname);
		END IF;
	END IF;
END
$$ LANGUAGE plpgsql STRICT;

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
	RAISE DEBUG '[SHMN] pg_shardman is dropping, cleaning up';

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
		-- TODO: remove only shardman's standbys
		-- If we never were in the cluster, we didn't touch sync_standby_names
	IF shardman.my_connstr() IS NOT NULL THEN
		-- otherwise we will hang
		SET LOCAL synchronous_commit TO LOCAL;
		PERFORM shardman.set_sync_standbys('');
	END IF;

	PERFORM shardman.reset_my_id();
END;
$$ LANGUAGE plpgsql;
CREATE FUNCTION pg_shardman_cleanup_c() RETURNS event_trigger
    AS 'pg_shardman' LANGUAGE C;
CREATE EVENT TRIGGER cleanup_lr_trigger ON ddl_command_start
	WHEN TAG IN ('DROP EXTENSION')
	EXECUTE PROCEDURE pg_shardman_cleanup_c();

CREATE FUNCTION alter_system_c(opt text, val text) RETURNS void
	AS 'pg_shardman' LANGUAGE C STRICT;

-- Ask shardlord to perform a command if we're but a worker node.
CREATE FUNCTION execute_on_lord_c(cmd_type text, cmd_opts text[]) RETURNS text
	AS 'pg_shardman' LANGUAGE C STRICT;

CREATE FUNCTION broadcast(commands cstring, ignore_errors boolean default false, two_phase boolean default false) RETURNS text
	AS 'broadcast' LANGUAGE C STRICT;



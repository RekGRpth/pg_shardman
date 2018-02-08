/* ------------------------------------------------------------------------
 *
 * pg_shardman.sql
 *   Sharding and replication using pg_pathman, postgres_fdw and logical replication
 *
 * Copyright (c) 2017, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_shardman" to load this file. \quit

-- We define several GUCs (though user can see them in SHOW if she sets it
-- explicitly even without loaded lib) and have to inform pathman that we want
-- shardman's COPY FROM, so it makes sense to load the lib on server start.
DO $$
BEGIN
-- -- Yes, malicious user might have another extension containing 'pg_shardman'...
-- -- Probably better just call no-op func from the library
	IF strpos(current_setting('shared_preload_libraries'), 'pg_shardman') = 0 THEN
		RAISE EXCEPTION 'pg_shardman must be loaded via shared_preload_libraries. Refusing to proceed.';
	END IF;
END
$$;

-- Shardman tables

-- List of nodes present in the cluster
CREATE TABLE nodes (
	id serial PRIMARY KEY,
	system_id bigint NOT NULL UNIQUE,
    super_connection_string text UNIQUE NOT NULL,
	connection_string text UNIQUE NOT NULL,
	-- group of nodes within which shard replicas are allocated
	replication_group text NOT NULL
);

-- List of sharded tables
CREATE TABLE tables (
	relation text PRIMARY KEY,     -- table name
	sharding_key text,             -- expression by which table is sharded
	master_node integer REFERENCES nodes(id) ON DELETE CASCADE,
	partitions_count int,          -- number of partitions
	create_sql text NOT NULL,      -- sql to create the table
	create_rules_sql text          -- sql to create rules for shared table
);

-- Main partitions
CREATE TABLE partitions (
	part_name text PRIMARY KEY,
	 -- Node on which partition lies. NULL, if node was removed and replica is
	 -- not yet promoted.
	node_id int REFERENCES nodes(id),
	relation text NOT NULL REFERENCES tables(relation) ON DELETE CASCADE
);

-- Partition replicas
CREATE TABLE replicas (
	part_name text NOT NULL REFERENCES partitions(part_name) ON DELETE CASCADE,
	-- node on which partition lies
	node_id int NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
	relation text NOT NULL REFERENCES tables(relation) ON DELETE CASCADE,
	PRIMARY KEY (part_name,node_id)
);

-- Shardman interface functions

-- Add a node: adjust logical replication channels in replication group and
-- create foreign servers.
-- 'super_conn_string' is connection string to the node which allows to login to
-- the node as superuser, and 'conn_string' can be some other connstring.
-- The former is used for configuring logical replication, the latter for DDL
-- and for setting up FDW. This separation serves two purposes:
-- * It allows to access data without requiring superuser privileges;
-- * It allows to set up pgbouncer, as replication can't go through it.
-- If conn_string is null, super_conn_string is used everywhere.
CREATE FUNCTION add_node(super_conn_string text, conn_string text = NULL,
						 repl_group text = NULL) RETURNS int AS $$
DECLARE
	new_node_id int;
    node shardman.nodes;
    part shardman.partitions;
	t shardman.tables;
	pubs text = '';
	subs text = '';
	sync text = '';
	conf text = '';
	fdws text = '';
	usms text = '';
	server_opts text;
	um_opts text;
	new_server_opts text;
	new_um_opts text;
	sync_standbys text[];
	create_table text;
	create_tables text = '';
	create_partitions text = '';
	create_fdws text = '';
	create_rules text = '';
	replace_parts text = '';
	fdw_part_name text;
	table_attrs text;
	srv_name text;
	rules text = '';
	master_node_id int;
	sys_id bigint;
	conn_string_effective text = COALESCE(conn_string, super_conn_string);
	conn_string_effective_user text;
BEGIN
	IF NOT shardman.is_shardlord()
	THEN
		RETURN shardman.broadcast(
			format('0:SELECT shardman.add_node(%L, %L, %L)',
				   super_conn_string, conn_string, repl_group))::int;
	END IF;

	-- Insert new node in nodes table.
	-- We have to update system_id (and repl_group, for which we probably need
	-- system_id) only after insert, because broadcast fetches connstring from
	-- shardman.nodes itself.
	INSERT INTO shardman.nodes (system_id, super_connection_string,
								connection_string, replication_group)
	VALUES (0, super_conn_string, conn_string_effective, '')
		   RETURNING id INTO new_node_id;

	sys_id := shardman.broadcast(
		format('%s:SELECT shardman.get_system_identifier();',
			   new_node_id), super_connstr => true)::bigint;
	IF EXISTS(SELECT 1 FROM shardman.nodes WHERE system_id = sys_id) THEN
		RAISE EXCEPTION 'Node with system id % is already in the cluster', sys_id;
	END IF;
	UPDATE shardman.nodes SET system_id = sys_id WHERE id = new_node_id;
	-- By default, use system id as repl group name
	UPDATE shardman.nodes SET replication_group =
		(CASE WHEN repl_group IS NULL THEN sys_id::text ELSE repl_group END)
		WHERE id = new_node_id;

	-- If conn_string is provided, make sure effective user has permissions on
	-- shardman schema and postgres_fdw.
	IF conn_string IS NOT NULL THEN
		conn_string_effective_user := shardman.broadcast(
			format('%s:SELECT current_user;', new_node_id),
			super_connstr => false);
		PERFORM shardman.broadcast(
			format('{%s:GRANT USAGE ON FOREIGN DATA WRAPPER postgres_fdw TO %s;
				   GRANT USAGE ON SCHEMA shardman TO %s;}',
				   new_node_id, conn_string_effective_user, conn_string_effective_user),
			super_connstr => true);
	END IF;

	-- Adjust replication channels within replication group.
	-- We need all-to-all replication channels between all group members.
	FOR node IN SELECT * FROM shardman.nodes WHERE replication_group = repl_group
		AND id  <> new_node_id
	LOOP
		-- Add to new node publications for all existing nodes and add
		-- publication for new node to all existing nodes
		pubs := format('%s%s:CREATE PUBLICATION node_%s;
			 			  %s:CREATE PUBLICATION node_%s;
						  %s:SELECT pg_create_logical_replication_slot(''node_%s'', ''pgoutput'');
						  %s:SELECT pg_create_logical_replication_slot(''node_%s'', ''pgoutput'');',
						  pubs, node.id, new_node_id,
						  new_node_id, node.id,
						  node.id, new_node_id,
						  new_node_id, node.id);
		-- Add to new node subscriptions to existing nodes and add subscription
		-- to new node to all existing nodes
		-- sub name is sub_$subnodeid_pubnodeid to avoid application_name collision
		subs := format('%s%s:CREATE SUBSCRIPTION sub_%s_%s CONNECTION %L PUBLICATION node_%s with (create_slot=false, slot_name=''node_%s'', synchronous_commit=local);
			 			  %s:CREATE SUBSCRIPTION sub_%s_%s CONNECTION %L PUBLICATION node_%s with (create_slot=false, slot_name=''node_%s'', synchronous_commit=local);',
						  subs,
						  node.id, node.id, new_node_id, super_conn_string, node.id, node.id,
			 			  new_node_id, new_node_id, node.id, node.super_connection_string, new_node_id, new_node_id);
	END LOOP;

	-- Broadcast create publication commands
    PERFORM shardman.broadcast(pubs, super_connstr => true);
	-- Broadcast create subscription commands
	PERFORM shardman.broadcast(subs, super_connstr => true);

	-- In case of synchronous replication broadcast update synchronous standby
	-- list commands
	IF shardman.synchronous_replication() AND
		(SELECT COUNT(*) FROM shardman.nodes WHERE replication_group = repl_group) > 1
	THEN
		FOR node IN SELECT * FROM shardman.nodes WHERE replication_group = repl_group LOOP
			sync := format('%s%s:ALTER SYSTEM SET synchronous_standby_names to %L;',
				 sync, node.id, shardman.construct_ssnames(node.id));
			conf := format('%s%s:SELECT pg_reload_conf();', conf, node.id);
		END LOOP;

	    PERFORM shardman.broadcast(sync, sync_commit_on => true, super_connstr => true);
	    PERFORM shardman.broadcast(conf, super_connstr => true);
	END IF;

	-- Add foreign servers for connection to the new node and backward
	-- Construct foreign server options from connection string of new node
	SELECT * FROM shardman.conninfo_to_postgres_fdw_opts(conn_string_effective) INTO new_server_opts, new_um_opts;
	FOR node IN SELECT * FROM shardman.nodes WHERE id<>new_node_id
	LOOP
	    -- Construct foreign server options from connection string of this node
		SELECT * FROM shardman.conninfo_to_postgres_fdw_opts(node.connection_string) INTO server_opts, um_opts;

		-- Create foreign server for new node at all other nodes and servers at new node for all other nodes
		fdws := format('%s%s:CREATE SERVER node_%s FOREIGN DATA WRAPPER postgres_fdw %s;
			 			  %s:CREATE SERVER node_%s FOREIGN DATA WRAPPER postgres_fdw %s;',
			 fdws, new_node_id, node.id, server_opts,
			 	   node.id, new_node_id, new_server_opts);

		-- Create user mapping for this servers
		usms := format('%s%s:CREATE USER MAPPING FOR CURRENT_USER SERVER node_%s %s;
			 			  %s:CREATE USER MAPPING FOR CURRENT_USER SERVER node_%s %s;',
			 usms, new_node_id, node.id, um_opts,
			      node.id, new_node_id, new_um_opts);
	END LOOP;

	-- Broadcast command for creating foreign servers
	PERFORM shardman.broadcast(fdws);
	-- Broadcast command for creating user mapping for this servers
	PERFORM shardman.broadcast(usms);

	-- Create FDWs at new node for all existing partitions
	FOR t IN SELECT * from shardman.tables WHERE sharding_key IS NOT NULL
	LOOP
		create_tables := format('%s{%s:%s}',
			create_tables, new_node_id, t.create_sql);
		create_partitions := format('%s%s:SELECT create_hash_partitions(%L,%L,%L);',
			create_partitions, new_node_id, t.relation, t.sharding_key, t.partitions_count);
		SELECT shardman.reconstruct_table_attrs(t.relation) INTO table_attrs;
		FOR part IN SELECT * FROM shardman.partitions WHERE relation=t.relation
	    LOOP
			create_fdws := format(
				'%s%s:SELECT shardman.replace_real_with_foreign(%s, %L, %L);',
				create_fdws, new_node_id, part.node_id, part.part_name, table_attrs);
		END LOOP;
	END LOOP;

	-- Create at new node FDWs for all shared tables
	FOR t IN SELECT * from shardman.tables WHERE master_node IS NOT NULL
	LOOP
		SELECT connection_string INTO conn_string from shardman.nodes WHERE id=t.master_node;
		create_tables := format('%s{%s:%s}',
			create_tables, new_node_id, t.create_sql);
		SELECT shardman.reconstruct_table_attrs(t.relation) INTO table_attrs;
		srv_name := format('node_%s', t.master_node);
		fdw_part_name := format('%s_fdw', t.relation);
		create_fdws := format('%s%s:CREATE FOREIGN TABLE %I %s SERVER %s OPTIONS (table_name %L);',
			create_fdws, new_node_id, fdw_part_name, table_attrs, srv_name, t.relation);
		create_rules :=  format('%s{%s:%s}',
			 create_rules, new_node_id, t.create_rules_sql);
	END LOOP;

	-- Create subscriptions for all shared tables
	subs := '';
	FOR master_node_id IN SELECT DISTINCT master_node FROM shardman.tables WHERE master_node IS NOT NULL
	LOOP
		subs := format('%s%s:CREATE SUBSCRIPTION share_%s_%s CONNECTION %L PUBLICATION shared_tables with (synchronous_commit=local);',
			 subs, new_node_id, new_node_id, t.master_node, conn_string);
	END LOOP;

    -- Broadcast create table commands
	PERFORM shardman.broadcast(create_tables);
	-- Broadcast create hash partitions command
	PERFORM shardman.broadcast(create_partitions, iso_level => 'read committed');
	-- Broadcast create foreign table commands
	PERFORM shardman.broadcast(create_fdws);
	-- Broadcast replace hash partition commands
	PERFORM shardman.broadcast(replace_parts);
	-- Broadcast create rules for shared tables
	PERFORM shardman.broadcast(create_rules);
	-- Broadcast create subscriptions for shared tables
	PERFORM shardman.broadcast(subs, super_connstr => true);

	RETURN new_node_id;
END
$$ LANGUAGE plpgsql;

-- Replace real partition with foreign one. Real partition is locked to avoid
-- stale writes.
CREATE FUNCTION replace_real_with_foreign(target_srv int, part_name name, table_attrs text)
	RETURNS void AS $$
DECLARE
	srv_name name :=  format('node_%s', target_srv);
	fdw_part_name name := format('%s_fdw', part_name);
BEGIN
	RAISE DEBUG '[SHMN] replace table % with foreign %', part_name, fdw_part_name;
	EXECUTE format('CREATE FOREIGN TABLE %I %s SERVER %s OPTIONS (table_name %L);',
				   fdw_part_name, table_attrs, srv_name, part_name);
	PERFORM replace_hash_partition(part_name::regclass, fdw_part_name::regclass);
	EXECUTE format('TRUNCATE TABLE %I', part_name);
	PERFORM shardman.write_protection_on(part_name::regclass);
END
$$ LANGUAGE plpgsql;

-- Replace foreign partition with real one. The latter must exist.
CREATE FUNCTION replace_foreign_with_real(part_name name) RETURNS void AS $$
DECLARE
	fdw_part_name name := format('%s_fdw', part_name);
BEGIN
	RAISE DEBUG '[SHMN] replace foreign table % with %', fdw_part_name, part_name;
	PERFORM replace_hash_partition(fdw_part_name::regclass, part_name::regclass);
	PERFORM shardman.write_protection_off(part_name::regclass);
	EXECUTE format('DROP FOREIGN TABLE %I', fdw_part_name);
END
$$ LANGUAGE plpgsql;

-- Construct synchronous_standby_names for node 'src_node'
CREATE FUNCTION construct_ssnames(src_node int) RETURNS text AS $$
DECLARE
	repl_group text := replication_group FROM shardman.nodes WHERE id = src_node;
	sync_standbys text[];
BEGIN
	sync_standbys :=
		-- format is sub_$dst_$src
		ARRAY(SELECT format('sub_%s_%s', id, src_node) ssn FROM shardman.nodes
			  WHERE replication_group = repl_group AND id <> src_node
			  ORDER BY ssn); -- to be able compare it
	RETURN CASE WHEN sync_standbys = '{}'::text[] THEN
		''
		ELSE
		format('FIRST %s (%s)', array_length(sync_standbys, 1),
			   array_to_string(sync_standbys, ','))
		END;
END
$$ LANGUAGE plpgsql;

-- Remove node: try to choose alternative from one of replicas of this nodes,
-- exclude node from replication channels and remove foreign servers.
-- To remove node with existing partitions use force=true parameter.
CREATE FUNCTION rm_node(rm_node_id int, force bool = false) RETURNS void AS $$
DECLARE
	node shardman.nodes;
	part shardman.partitions;
	repl shardman.replicas;
	pubs text = '';
	subs text = '';
	prts text = '';
	sync text = '';
	conf text = '';
	alts text = '';
    new_master_id int;
	sync_standbys text[];
	sync_standby_names text;
	repl_group text;
	master_node_id int;
	err text;
BEGIN
	IF shardman.redirect_to_shardlord(format('rm_node(%L, %L)', rm_node_id, force))
	THEN
		RETURN;
	END IF;

	IF NOT EXISTS(SELECT * FROM shardman.nodes WHERE id=rm_node_id)
	THEN
	   	RAISE EXCEPTION 'Node % does not exist', rm_node_id;
 	END IF;

	-- If it is not forced remove, check if there are no partitions at this node
	IF NOT force THEN
	    IF EXISTS (SELECT * FROM shardman.partitions WHERE node_id = rm_node_id)
	    THEN
	   	    RAISE EXCEPTION 'Use force=true to remove non-empty node';
	    END IF;
    END IF;

	SELECT replication_group INTO repl_group FROM shardman.nodes WHERE id=rm_node_id;

	-- Clean removed node, if it is reachable. Better to do that before removing
	-- pubs on other nodes to avoid 'with_force' pub removal. However, it is also
	-- would be good to do that *after* removing the node from metadata to avoid
	-- recover() run if rm_node fails without touching metadata. We currently
	-- can't do that, though, because broadcast would not know where to find
	-- conn string.
	PERFORM shardman.broadcast(format('%s:SELECT shardman.wipe_state();',
									  rm_node_id),
									  ignore_errors := true);

	-- We set node_id of node's parts to NULL, meaning they are waiting for
	-- promotion. Replicas are removed with cascade.
	UPDATE shardman.partitions SET node_id = NULL WHERE node_id=rm_node_id;
	DELETE FROM shardman.nodes WHERE id = rm_node_id;


	-- Remove all subscriptions and publications related to removed node
    FOR node IN SELECT * FROM shardman.nodes WHERE replication_group=repl_group
		LOOP
		-- We don't remove pubs on removed node; wipe_state will handle that.
		-- on other members of replication group
		pubs := format('%s%s:DROP PUBLICATION node_%s;
						  %s:SELECT pg_drop_replication_slot(''node_%s'');',
			 pubs, node.id, rm_node_id,
				   node.id, rm_node_id);

		subs := format('%s%s:SELECT shardman.eliminate_sub(''sub_%s_%s'');',
						subs, node.id, node.id, rm_node_id);

		-- Construct new synchronous standby list
		sync := format('%s%s:ALTER SYSTEM SET synchronous_standby_names to %L;',
					   sync, node.id, shardman.construct_ssnames(node.id));
		conf := format('%s%s:SELECT pg_reload_conf();', conf, node.id);
	END LOOP;

	-- Broadcast drop subscription commands, ignore errors because removed node
	-- might be not available
	err := shardman.broadcast(subs, ignore_errors => true, super_connstr => true);
	IF position('<error>' IN err) <> 0 THEN
		RAISE WARNING 'Failed to rm subs taking changes from removed node % on some nodes',
		  rm_node_id;
	END IF;
	err := shardman.broadcast(pubs, ignore_errors => true, super_connstr => true);
	IF position('<error>' IN err) <> 0 THEN
		RAISE WARNING 'Failed to rm pubs publishing changes to removed node % on some nodes',
		  rm_node_id;
	END IF;

	-- In case of synchronous replication update synchronous standbys list
	IF shardman.synchronous_replication()
	THEN
	    PERFORM shardman.broadcast(sync,
								   ignore_errors => true,
								   sync_commit_on => true,
								   super_connstr => true);
	    PERFORM shardman.broadcast(conf, ignore_errors:=true, super_connstr => true);
	END IF;

	-- Exclude partitions of removed node, promote them on replicas, if any
	FOR part IN SELECT * FROM shardman.partitions prts WHERE prts.node_id IS NULL
	LOOP
		-- Is there some replica of this part?
		SELECT node_id INTO new_master_id FROM shardman.replicas
		  WHERE part_name=part.part_name ORDER BY random() LIMIT 1;
		IF new_master_id IS NOT NULL
		THEN -- exists some replica for this part, promote it
			-- If there are more than one replica of this partition, we need to
			-- synchronize them
			IF shardman.get_redundancy_of_partition(part.part_name) > 1
			THEN
				BEGIN
					PERFORM shardman.synchronize_replicas(part.part_name);
				EXCEPTION
					WHEN external_routine_invocation_exception THEN
						RAISE WARNING 'Failed to promote replicas after node % removal: couldn''t synchronize replicas',
						rm_node_id USING DETAIL = SQLERRM, HINT = 'You should run recover() after resolving the problem';
						EXIT;
				END;
			END IF;

			pubs := '';
			subs := '';
			-- Refresh LR channels for this replication group
			FOR repl IN SELECT * FROM shardman.replicas
				WHERE part_name=part.part_name AND node_id != new_master_id
			LOOP
				-- Publish this partition at new master
			    pubs := format('%s%s:ALTER PUBLICATION node_%s ADD TABLE %I;',
				     pubs, new_master_id, repl.node_id, part.part_name);
				-- And refresh subscriptions and replicas
				subs := format('%s%s:ALTER SUBSCRIPTION sub_%s_%s REFRESH PUBLICATION WITH (copy_data=false);',
					 subs, repl.node_id, repl.node_id, new_master_id);
			END LOOP;

			BEGIN
				-- Broadcast alter publication commands
				PERFORM shardman.broadcast(pubs, super_connstr => true);
				-- Broadcast refresh alter subscription commands
				PERFORM shardman.broadcast(subs, super_connstr => true);
			EXCEPTION
				WHEN external_routine_invocation_exception THEN
					RAISE WARNING 'Failed to promote replicas after node % removal: couldn''t update LR channels',
					rm_node_id USING DETAIL = SQLERRM, HINT = 'You should run recover() after resolving the problem';
					EXIT;
			END;
		ELSE -- there is no replica: we have to create new empty partition at
			 -- random mode and redirect all FDWs to it
			RAISE WARNING 'Data of partition % was lost, creating empty partition.',
				part.part_name;
			SELECT id INTO new_master_id FROM shardman.nodes
			  WHERE id<>rm_node_id ORDER BY random() LIMIT 1;
		END IF;

		-- Partition is successfully promoted, update metadata. XXX: we should
		-- commit that before sending new mappings, because otherwise if we fail
		-- during the latter, news about promoted replica will be lost; next
		-- time we might choose another replica to promote with some new data
		-- already written to previously promoted replica. Syncing replicas
		-- doesn't help us much here if we don't lock tables.
		UPDATE shardman.partitions SET node_id = new_master_id
		 WHERE part_name = part.part_name;
		DELETE FROM shardman.replicas WHERE part_name = part.part_name AND
											node_id = new_master_id;

		-- Update pathman partition map at all nodes
		FOR node IN SELECT * FROM shardman.nodes WHERE id<>rm_node_id
		LOOP
			IF node.id=new_master_id THEN
			    -- At new master node replace foreign link with local partition
				prts := format('%s%s:SELECT shardman.replace_foreign_with_real(%L);',
			 				   prts, node.id, part.part_name);
			ELSE
				-- At all other nodes adjust foreign server for foreign table to
				-- refer to new master node.
				prts := format(
					'%s%s:SELECT shardman.alter_ftable_set_server(%L, ''node_%s'', true);',
		   			prts, node.id, part.part_name || '_fdw', new_master_id);
			END IF;
		END LOOP;
	END LOOP;

	-- Broadcast changes of pathman mapping
	BEGIN
		PERFORM shardman.broadcast(prts);
	EXCEPTION
		WHEN external_routine_invocation_exception THEN
			RAISE WARNING 'Failed to update FDW mappings after node % removal',
			rm_node_id USING DETAIL = SQLERRM, HINT = 'You should run recover() after resolving the problem';
	END;
END
$$ LANGUAGE plpgsql;

-- Since PG doesn't support it, mess with catalogs directly. If asked and no one
-- uses this server, drop it.
CREATE FUNCTION alter_ftable_set_server(ftable name, new_fserver name,
										server_not_needed bool DEFAULT false) RETURNS void AS $$
DECLARE
	new_fserver_oid oid := oid FROM pg_foreign_server WHERE srvname = new_fserver;
	old_fserver name := srvname FROM pg_foreign_server
		WHERE oid = (SELECT ftserver FROM pg_foreign_table WHERE ftrelid = ftable::regclass);
	old_fserver_oid oid := oid FROM pg_foreign_server WHERE srvname = old_fserver;
BEGIN
	UPDATE pg_foreign_table SET ftserver = new_fserver_oid WHERE ftrelid = ftable::regclass;
	UPDATE pg_depend SET refobjid = new_fserver_oid
		WHERE objid = ftable::regclass AND refobjid = old_fserver_oid;
	IF server_not_needed AND
	   ((SELECT count(*) FROM pg_foreign_table WHERE ftserver = old_fserver_oid) = 0)
	THEN
		EXECUTE format('DROP SERVER %s CASCADE', old_fserver);
	END IF;
END
$$ LANGUAGE plpgsql;

-- Bail out with ERROR if some replication group doesn't have 'redundancy'
-- replicas
CREATE FUNCTION check_max_replicas(redundancy int) RETURNS void AS $$
DECLARE
	rg record;
BEGIN
	FOR rg IN SELECT count(*), replication_group FROM shardman.nodes
		GROUP BY replication_group LOOP
		IF rg.count < redundancy + 1 THEN
			RAISE EXCEPTION 'Requested redundancy % is too high: replication group % has % members', redundancy, rg.replication_group, rg.count;
		END IF;
	END LOOP;
END
$$ LANGUAGE plpgsql;

-- Shard table with hash partitions. Parameters are the same as in pathman.
-- It also scatter partitions through all nodes.
-- This function expects that empty table is created at shardlord.
-- It can be executed only at shardlord and there is no need to redirect this
-- function to shardlord.
CREATE FUNCTION create_hash_partitions(rel_name name, expr text, part_count int,
									   redundancy int = 0)
RETURNS void AS $$
DECLARE
	create_table text;
	node shardman.nodes;
	node_ids int[];
	node_id int;
	part_name text;
	fdw_part_name text;
	table_attrs text;
	srv_name text;
	create_tables text = '';
	create_partitions text = '';
	create_fdws text = '';
	replace_parts text = '';
	i int;
	n_nodes int;
BEGIN
	IF EXISTS(SELECT relation FROM shardman.tables WHERE relation = rel_name)
	THEN
		RAISE EXCEPTION 'Table % is already sharded', rel_name;
	END IF;

	IF (SELECT count(*) FROM shardman.nodes) = 0 THEN
		RAISE EXCEPTION 'Please add some nodes first';
	END IF;

	-- Check right away to avoid unneccessary recover()
	PERFORM shardman.check_max_replicas(redundancy);

	-- Generate SQL statement creating this table
	SELECT shardman.gen_create_table_sql(rel_name) INTO create_table;

	INSERT INTO shardman.tables (relation,sharding_key,partitions_count,create_sql) values (rel_name,expr,part_count,create_table);

	-- Create parent table and partitions at all nodes
	FOR node IN SELECT * FROM shardman.nodes
	LOOP
		-- Create parent table at all nodes
		create_tables := format('%s{%s:%s}',
			create_tables, node.id, create_table);
		create_partitions := format('%s%s:select create_hash_partitions(%L,%L,%L);',
			create_partitions, node.id, rel_name, expr, part_count);
	END LOOP;

	-- Broadcast create table commands
	PERFORM shardman.broadcast(create_tables);
	-- Broadcast create hash partitions command
	PERFORM shardman.broadcast(create_partitions, iso_level => 'read committed');

	-- Get list of nodes in random order
	SELECT ARRAY(SELECT id from shardman.nodes ORDER BY random()) INTO node_ids;
	n_nodes := array_length(node_ids, 1);

	-- Reconstruct table attributes from parent table
	SELECT shardman.reconstruct_table_attrs(rel_name::regclass) INTO table_attrs;

	FOR i IN 0..part_count-1
	LOOP
		-- Choose location of new partition
		node_id := node_ids[1 + (i % n_nodes)]; -- round robin
		part_name := format('%s_%s', rel_name, i);
		fdw_part_name := format('%s_fdw', part_name);
		-- Insert information about new partition in partitions table
		INSERT INTO shardman.partitions (part_name, node_id, relation) VALUES (part_name, node_id, rel_name);
		-- Construct name of the server where partition will be located
		srv_name := format('node_%s', node_id);

		-- Replace local partition with foreign table at all nodes except owner
		FOR node IN SELECT * FROM shardman.nodes WHERE id<>node_id
		LOOP
			-- Create foreign table for this partition
			create_fdws := format(
				'%s%s:SELECT shardman.replace_real_with_foreign(%s, %L, %L);',
				create_fdws, node.id, node_id, part_name, table_attrs);
		END LOOP;
	END LOOP;

	-- Broadcast create foreign table commands
	PERFORM shardman.broadcast(create_fdws, iso_level => 'read committed');
	-- Broadcast replace hash partition commands
	PERFORM shardman.broadcast(replace_parts);

	IF redundancy <> 0
	THEN
		PERFORM shardman.set_redundancy(rel_name, redundancy, copy_data => false);
	END IF;
END
$$ LANGUAGE plpgsql;

-- Provide requested level of redundancy. 0 means no redundancy.
-- If existing level of redundancy is greater than specified, then right now this
-- function does nothing.
CREATE FUNCTION set_redundancy(rel_name name, redundancy int, copy_data bool = true)
RETURNS void AS $$
DECLARE
	part shardman.partitions;
	n_replicas int;
	repl_node int;
	repl_group text;
	pubs text = '';
	subs text = '';
	sub text = '';
	sub_options text = '';
BEGIN
	IF shardman.redirect_to_shardlord(format('set_redundancy(%L, %L)', rel_name,
											 redundancy))
	THEN
		RETURN;
	END IF;

	PERFORM shardman.check_max_replicas(redundancy);

	IF NOT copy_data THEN
	    sub_options := ' WITH (copy_data=false)';
	END IF;

	-- Loop through all partitions of this table
	FOR part IN SELECT * FROM shardman.partitions WHERE relation=rel_name
	LOOP
		-- Count number of replicas of this partition
		SELECT count(*) INTO n_replicas FROM shardman.replicas
			WHERE part_name=part.part_name;
		IF n_replicas < redundancy
		THEN -- If it is smaller than requested...
			SELECT replication_group INTO repl_group FROM shardman.nodes
				WHERE id=part.node_id;
			-- ...then add requested number of replicas in corresponding replication group
			FOR repl_node IN SELECT id FROM shardman.nodes
				WHERE replication_group=repl_group AND id<>part.node_id AND NOT EXISTS
					(SELECT * FROM shardman.replicas WHERE node_id=id AND part_name=part.part_name)
				ORDER by random() LIMIT redundancy-n_replicas
			LOOP
				-- Insert information about new replica in replicas table
				INSERT INTO shardman.replicas (part_name, node_id, relation)
					VALUES (part.part_name, repl_node, rel_name);
				-- Establish publications and subscriptions for this partition
				pubs := format('%s%s:ALTER PUBLICATION node_%s ADD TABLE %I;',
					 pubs, part.node_id, repl_node, part.part_name);
				sub := format('%s:ALTER SUBSCRIPTION sub_%s_%s REFRESH PUBLICATION%s;',
							  repl_node, repl_node, part.node_id, sub_options);
				-- ignore duplicates
				IF position(sub in subs) = 0 THEN
					subs := subs || sub;
				END IF;
			END LOOP;
		END IF;
	END LOOP;

	-- Broadcast alter publication commands
	PERFORM shardman.broadcast(pubs, super_connstr => true);
	-- Broadcast alter subscription commands
	-- Initial tablesync creates temporary repslots, which wait until all xacts
	-- started before snapshot creation end; because of that we must alter subs
	-- sequentially, or deadlocks are possible.
	PERFORM shardman.broadcast(subs, sequential => copy_data, super_connstr => true);

	-- Maintain change log to be able to synchronize replicas after primary node failure
	IF redundancy > 1
	THEN
		PERFORM shardman.generate_on_change_triggers(rel_name);
	END IF;

	-- This function doesn't wait completion of replication sync.
	-- Use wait ensure_redundancy function to wait until sync is completed
END
$$ LANGUAGE plpgsql;

-- Wait completion of initial table sync for all replication subscriptions.
-- This function can be used after set_redundancy to ensure that partitions are
-- copied to replicas.
CREATE FUNCTION ensure_redundancy() RETURNS void AS $$
DECLARE
	src_node_id int;
	dst_node_id int;
	timeout_sec int = 1;
	sub_name text;
	poll text;
	response text;
BEGIN
	IF shardman.redirect_to_shardlord('ensure_redundancy()')
	THEN
		RETURN;
	END IF;

	-- Wait until all subscriptions switch to ready state
	LOOP
		poll := '';
		FOR src_node_id IN SELECT id FROM shardman.nodes
		LOOP
			FOR dst_node_id IN SELECT id FROM shardman.nodes WHERE id<>src_node_id
			LOOP
				sub_name := format('sub_%s_%s', dst_node_id, src_node_id);
		    	poll := format('%s%s:SELECT shardman.is_subscription_ready(%L);',
					 poll, dst_node_id, sub_name);
			END LOOP;
		END LOOP;

		-- Poll subscription statuses at all nodes
		response := shardman.broadcast(poll);

		-- Check if all are ready
		EXIT WHEN POSITION('f' IN response)=0;

		PERFORM pg_sleep(timeout_sec);
	END LOOP;
END
$$ LANGUAGE plpgsql;


-- Remove table from all nodes.
CREATE FUNCTION rm_table(rel_name name)
RETURNS void AS $$
DECLARE
	node_id int;
	pname text;
	drop1 text = '';
	drop2 text = '';
BEGIN
	IF shardman.redirect_to_shardlord(format('rm_table(%L)', rel_name))
	THEN
		RETURN;
	END IF;

	-- Drop table at all nodes
	FOR node_id IN SELECT id FROM shardman.nodes
	LOOP
		-- Drop parent table. It will also delete all its partitions.
		drop1 := format('%s%s:DROP TABLE %I CASCADE;',
			  drop1, node_id, rel_name);
		-- Drop replicas and stub tables (which are replaced with foreign tables)
		FOR pname IN SELECT part_name FROM shardman.partitions WHERE
			relation = rel_name
		LOOP
			drop2 := format('%s%s:DROP TABLE IF EXISTS %I CASCADE;',
			  	  drop2, node_id, pname);
		END LOOP;
	END LOOP;

	-- Broadcast drop table commands
	PERFORM shardman.broadcast(drop1);
	PERFORM shardman.broadcast(drop2);

	-- Update metadata
	DELETE FROM shardman.tables WHERE relation=rel_name;
END
$$ LANGUAGE plpgsql;

-- Move partition to other node. This function can move partition only within
-- replication group. It creates temporary logical replication channel to copy
-- partition to new location. Until logical replication almost caught-up access
-- to old partition is denied. Then we revoke all access to this table until
-- copy is completed and all FDWs are updated.
CREATE FUNCTION mv_partition(mv_part_name text, dst_node_id int)
RETURNS void AS $$
DECLARE
	node shardman.nodes;
	src_repl_group text;
	dst_repl_group text;
	conn_string text;
	part shardman.partitions;
	replace_parts text = '';
	fdw_part_name text = format('%s_fdw', mv_part_name);
	table_attrs text;
	srv_name text = format('node_%s', dst_node_id);
	pubs text = '';
	subs text = '';
	src_node_id int;
	repl_node_id int;
	drop_slots text = '';
	err text;
BEGIN
	IF shardman.redirect_to_shardlord(format('mv_partition(%L, %L)', mv_part_name,
											 dst_node_id))
	THEN
		RETURN;
	END IF;

	-- Check if there is partition with specified name
	SELECT * INTO part FROM shardman.partitions WHERE part_name = mv_part_name;
	IF part IS NULL THEN
	    RAISE EXCEPTION 'Partition % does not exist', mv_part_name;
	END IF;
	src_node_id := part.node_id;

	SELECT replication_group, super_connection_string
	  INTO src_repl_group, conn_string
	  FROM shardman.nodes WHERE id=src_node_id;
	SELECT replication_group INTO dst_repl_group FROM shardman.nodes WHERE id=dst_node_id;

	IF src_node_id = dst_node_id THEN
	    -- Nothing to do: partition is already here
		RAISE NOTICE 'Partition % is already located at node %',mv_part_name,dst_node_id;
		RETURN;
	END IF;

	-- Check if destination belongs to the same replication group as source
	IF dst_repl_group<>src_repl_group AND shardman.get_redundancy_of_partition(mv_part_name)>0
	THEN
	    RAISE EXCEPTION 'Unable to move partition % to different replication group', mv_part_name;
	END IF;

	IF EXISTS(SELECT * FROM shardman.replicas WHERE part_name=mv_part_name AND node_id=dst_node_id)
	THEN
	    RAISE EXCEPTION 'Unable to move partition % to node % with existing replica', mv_part_name, dst_node_id;
	END IF;

	-- Copy partition data to new location
	pubs := format('%s:CREATE PUBLICATION shardman_copy_%s FOR TABLE %I;
		 			%s:SELECT pg_create_logical_replication_slot(''shardman_copy_%s'', ''pgoutput'');',
		 src_node_id, mv_part_name, mv_part_name,
		 src_node_id, mv_part_name);
	subs := format('%s:CREATE SUBSCRIPTION shardman_copy_%s CONNECTION %L PUBLICATION shardman_copy_%s with (create_slot=false, slot_name=''shardman_copy_%s'', synchronous_commit=local);',
		 dst_node_id, mv_part_name, conn_string, mv_part_name, mv_part_name);

	-- Create publication and slot for copying
	PERFORM shardman.broadcast(pubs, super_connstr => true);
	PERFORM shardman.broadcast(subs, super_connstr => true);

	-- Wait completion of partition copy and prohibit access to this partition
	PERFORM shardman.wait_copy_completion(src_node_id, dst_node_id, mv_part_name);

	RAISE NOTICE 'Copy of partition % from node % to % is completed',
		 mv_part_name, src_node_id, dst_node_id;

	pubs := '';
	subs := '';

	-- Drop temporary LR channel
	PERFORM shardman.broadcast(format('%s:DROP PUBLICATION shardman_copy_%s;',
									  src_node_id, mv_part_name),
									  super_connstr => true);
	-- drop sub cannot be executed in multi-command string, so don't set
	-- synchronous_commit to local
	PERFORM shardman.broadcast(format(
		'%s:DROP SUBSCRIPTION shardman_copy_%s;',
		dst_node_id, mv_part_name), super_connstr => true, sync_commit_on => true);

	-- Drop old channels and establish new ones. We don't care much about the
	-- order of actions: if recover() initially fixes LR channels and only then
	-- repairs mappings, we will be fine anyway: all nodes currently see old
	-- location which is locked for writes, and will be unlocked (if needed)
	-- only after fixing channels and mappings. Ideally we should also block
	-- reads of old partition to prevent returning stale data.
	pubs := '';
	subs := '';
	FOR repl_node_id IN SELECT node_id FROM shardman.replicas WHERE part_name=mv_part_name
	LOOP
		pubs := format('%s%s:ALTER PUBLICATION node_%s DROP TABLE %I;
			 			  %s:ALTER PUBLICATION node_%s ADD TABLE %I;',
			 pubs, src_node_id, repl_node_id, mv_part_name,
			 	   dst_node_id, repl_node_id, mv_part_name);
		subs := format('%s%s:ALTER SUBSCRIPTION sub_%s_%s REFRESH PUBLICATION WITH (copy_data=false);
			 			  %s:ALTER SUBSCRIPTION sub_%s_%s REFRESH PUBLICATION WITH (copy_data=false);',
			 subs, repl_node_id, repl_node_id, src_node_id,
			 	   repl_node_id, repl_node_id, dst_node_id);
	END LOOP;

	-- Broadcast alter publication commands
	PERFORM shardman.broadcast(pubs, super_connstr => true);
	-- Broadcast alter subscription commands
	PERFORM shardman.broadcast(subs, super_connstr => true);

    -- Now, with source part locked and dst part fully copied, update owner of
    -- this partition: we consider move as completed at this point. We must make
    -- this change persistent before reconfiguring mappings, otherwise recover()
    -- will still think partition was not moved after some nodes probably wrote
    -- something to part at new location.
	-- NB: if you want to see the update in this xact, make sure we are at READ
    -- COMMITTED here.
	PERFORM shardman.broadcast(format(
		'0: UPDATE shardman.partitions SET node_id=%s WHERE part_name=%L;',
		dst_node_id, mv_part_name));

	-- Update FDWs at all nodes
	SELECT shardman.reconstruct_table_attrs(part.relation) INTO table_attrs;
	FOR node IN SELECT * FROM shardman.nodes
	LOOP
		IF node.id = src_node_id
		THEN
			replace_parts := format(
				'%s%s:SELECT shardman.replace_real_with_foreign(%s, %L, %L);',
				replace_parts, node.id, dst_node_id, mv_part_name, table_attrs);
		ELSIF node.id = dst_node_id THEN
			replace_parts := format(
				'%s%s:SELECT shardman.replace_foreign_with_real(%L);',
				replace_parts, node.id, mv_part_name);
		ELSE
			replace_parts := format(
				'%s%s:SELECT shardman.alter_ftable_set_server(%L, ''node_%s'');',
		   		replace_parts, node.id, fdw_part_name, dst_node_id);
		END IF;
	END LOOP;

	-- Broadcast replace hash partition commands
	err := shardman.broadcast(replace_parts, ignore_errors => true);
	IF position('<error>' IN err) <> 0 THEN
		RAISE WARNING 'Partition % was successfully moved from % to %, but FDW mappings update failed on some nodes.',
		mv_part_name, src_node_id, dst_node_id
		USING HINT = 'You should run recover() after resolving the problem';
	END IF;
END
$$ LANGUAGE plpgsql;

-- Get redundancy of the particular partition
-- This command can be executed only at shardlord.
CREATE FUNCTION get_redundancy_of_partition(part_name text) returns bigint AS $$
	SELECT count(*) FROM shardman.replicas r where r.part_name=part_name;
$$ LANGUAGE sql;

-- Get minimal redundancy of the specified relation.
-- This command can be executed only at shardlord.
CREATE FUNCTION get_min_redundancy(rel_name name) RETURNS bigint AS $$
	SELECT min(redundancy) FROM (SELECT count(*) redundancy FROM shardman.replicas WHERE relation=rel_name GROUP BY part_name) s;
$$ LANGUAGE sql;

-- Execute command at all shardman nodes.
-- It can be used to perform DDL at all nodes.
CREATE FUNCTION forall(sql text, use_2pc bool = false, including_shardlord bool = false)
returns void AS $$
DECLARE
	node_id integer;
	cmds text = '';
BEGIN
	IF shardman.redirect_to_shardlord(format('forall(%L, %L, %L)', sql, use_2pc, including_shardlord))
	THEN
		RETURN;
	END IF;

	-- Loop through all nodes
	FOR node_id IN SELECT * from shardman.nodes
	LOOP
		cmds := format('%s%s:%s;', cmds, node_id, sql);
	END LOOP;

	-- Execute command also at shardlord
	IF including_shardlord
	THEN
		cmds := format('%s0:%s;', cmds, sql);
	END IF;

	PERFORM shardman.broadcast(cmds, two_phase => use_2pc);
END
$$ LANGUAGE plpgsql;

-- Count number of replicas at particular node.
-- This command can be executed only at shardlord.
CREATE FUNCTION get_node_replicas_count(node_id int) returns bigint AS $$
   SELECT count(*) from shardman.replicas r WHERE r.node_id=node_id;
$$ LANGUAGE sql;

-- Count number of partitions at particular node.
-- This command can be executed only at shardlord.
CREATE FUNCTION get_node_partitions_count(node_id int) returns bigint AS $$
   SELECT count(*) from shardman.partitions p WHERE p.node_id=node_id;
$$ LANGUAGE sql;

-- Rebalance partitions between nodes. This function tries to evenly
-- redistribute partitions of tables which names match LIKE 'pattern'
-- between all nodes of replication groups.
-- It is not able to move partition between replication groups.
-- This function intentionally moves one partition per time to minimize
-- influence on system performance.
CREATE FUNCTION rebalance(part_pattern text = '%') RETURNS void AS $$
DECLARE
	dst_node int;
	src_node int;
	min_count bigint;
	max_count bigint;
	mv_part_name text;
	repl_group text;
	done bool;
BEGIN
	IF shardman.redirect_to_shardlord(format('rebalance(%L)', part_pattern))
	THEN
		RETURN;
	END IF;

	LOOP
		done := true;
		-- Repeat for all replication groups
		FOR repl_group IN SELECT DISTINCT replication_group FROM shardman.nodes
		LOOP
			-- Select node in this group with minimal number of partitions
			SELECT node_id, count(*) n_parts INTO dst_node, min_count
				FROM shardman.partitions p JOIN shardman.nodes n ON p.node_id=n.id
			    WHERE n.replication_group=repl_group AND p.relation LIKE part_pattern
				GROUP BY node_id
				ORDER BY n_parts ASC LIMIT 1;
			-- Select node in this group with maximal number of partitions
			SELECT node_id, count(*) n_parts INTO src_node,max_count
			    FROM shardman.partitions p JOIN shardman.nodes n ON p.node_id=n.id
				WHERE n.replication_group=repl_group AND p.relation LIKE part_pattern
				GROUP BY node_id
				ORDER BY n_parts DESC LIMIT 1;
			-- If difference of number of partitions on this nodes is greater
			-- than 1, then move random partition
			IF max_count - min_count > 1 THEN
			    SELECT p.part_name INTO mv_part_name
				FROM shardman.partitions p
				WHERE p.node_id=src_node AND p.relation LIKE part_pattern AND
				    NOT EXISTS(SELECT * from shardman.replicas r
							   WHERE r.node_id=dst_node AND r.part_name=p.part_name)
				ORDER BY random() LIMIT 1;
				PERFORM shardman.mv_partition(mv_part_name, dst_node);
				done := false;
			END IF;
		END LOOP;

		EXIT WHEN done;
	END LOOP;
END
$$ LANGUAGE plpgsql;

-- Share table between all nodes. This function should be executed at
-- shardlord. The empty table should be present at shardlord, but not at nodes.
CREATE FUNCTION create_shared_table(rel regclass, master_node_id int = 1) RETURNS void AS $$
DECLARE
	node shardman.nodes;
	pubs text = '';
	subs text = '';
	fdws text = '';
	rules text = '';
	conn_string text;
	create_table text;
	create_tables text;
	create_rules text;
	table_attrs text;
	rel_name text = rel::text;
	fdw_name text = format('%s_fdw', rel_name);
	srv_name text = format('node_%s', master_node_id);
	new_master bool;
BEGIN
	-- Check if valid node ID is passed and get connection string for this node
	SELECT connection_string INTO conn_string FROM shardman.nodes WHERE id=master_node_id;
	IF conn_string IS NULL THEN
	    RAISE EXCEPTION 'There is no node with ID % in the cluster', master_node_id;
	END IF;

    -- Generate SQL statement creating this table
	SELECT shardman.gen_create_table_sql(rel_name) INTO create_table;

	-- Construct table attributes for create foreign table
	SELECT shardman.reconstruct_table_attrs(rel) INTO table_attrs;

	-- Generate SQL statements creating  instead rules for updates
	SELECT shardman.gen_create_rules_sql(rel_name) INTO create_rules;

	-- Create table at all nodes
	FOR node IN SELECT * FROM shardman.nodes
	LOOP
		create_tables := format('%s{%s:%s}',
			create_tables, node.id, create_table);
	END LOOP;

	-- Create publication at master node
	IF EXISTS(SELECT * from shardman.tables WHERE master_node=master_node_id)
	THEN
		new_master := false;
		pubs := format('%s:ALTER PUBLICATION shared_tables ADD TABLE %I;',
			 master_node_id, rel_name);
	ELSE
		new_master := true;
		pubs := format('%s:CREATE PUBLICATION shared_tables FOR TABLE %I;',
	         master_node_id, rel_name);
	END IF;

	-- Insert information about new table in shardman.tables
	INSERT INTO shardman.tables (relation,master_node,create_sql,create_rules_sql) values (rel_name,master_node_id,create_table,create_rules);

	-- Create subscriptions, foreign tables and rules at all nodes
	FOR node IN SELECT * FROM shardman.nodes WHERE id<>master_node_id
	LOOP
		IF new_master THEN
		    subs := format('%s%s:CREATE SUBSCRIPTION share_%s_%s CONNECTION %L PUBLICATION shared_tables WITH (copy_data=false, synchronous_commit=local);',
				 subs, node.id, node.id, master_node_id, conn_string);
		ELSE
			subs := format('%s%s:ALTER SUBSCRIPTION share_%s_%s REFRESH PUBLICATIONS WITH (copy_data=false);',
				 subs, node.id, node.id, master_node_id);
		END IF;
		fdws := format('%s%s:CREATE FOREIGN TABLE %I %s SERVER %s OPTIONS (table_name %L);',
			 fdws, node.id, fdw_name, table_attrs, srv_name, rel_name);
		rules := format('%s{%s:%s}',
			 rules, node.id, create_rules);
	END LOOP;

	-- Broadcast create table command
	PERFORM shardman.broadcast(create_tables);
	-- Create or alter publication at master node
	PERFORM shardman.broadcast(pubs);
	-- Create subscriptions at all nodes
	PERFORM shardman.broadcast(subs, sync_commit_on => true, super_connstr => true);
	-- Create foreign tables at all nodes
	PERFORM shardman.broadcast(fdws);
	-- Create redirect rules at all nodes
	PERFORM shardman.broadcast(rules);
END
$$ LANGUAGE plpgsql;


-- Move replica to other node. This function is able to move replica only within replication group.
-- It initiates copying data to new replica, disables logical replication to original replica,
-- waits completion of initial table sync and then removes old replica.
CREATE FUNCTION mv_replica(mv_part_name text, src_node_id int, dst_node_id int)
RETURNS void AS $$
DECLARE
	src_repl_group text;
	dst_repl_group text;
	master_node_id int;
	rel_name text;
BEGIN
	IF shardman.redirect_to_shardlord(format('mv_replica(%L, %L, %L)', mv_part_name, src_node_id, dst_node_id))
	THEN
		RETURN;
	END IF;

	IF src_node_id = dst_node_id
    THEN
	    -- Nothing to do: replica is already here
		RAISE NOTICE 'Replica % is already located at node %', mv_part_name,dst_node_id;
		RETURN;
	END IF;

	-- Check if there is such replica at source node
	IF NOT EXISTS(SELECT * FROM shardman.replicas WHERE part_name=mv_part_name AND node_id=src_node_id)
	THEN
	    RAISE EXCEPTION 'Replica of % does not exist on node %', mv_part_name, src_node_id;
	END IF;

	-- Check if destination belongs to the same replication group as source
	SELECT replication_group INTO src_repl_group FROM shardman.nodes WHERE id=src_node_id;
	SELECT replication_group INTO dst_repl_group FROM shardman.nodes WHERE id=dst_node_id;
	IF dst_repl_group<>src_repl_group
	THEN
	    RAISE EXCEPTION 'Can not move replica % from replication group % to %', mv_part_name, src_repl_group, dst_repl_group;
	END IF;

	-- Check if there is no replica of this partition at the destination node
	IF EXISTS(SELECT * FROM shardman.replicas WHERE part_name=mv_part_name AND node_id=dst_node_id)
	THEN
	    RAISE EXCEPTION 'Can not move replica % to node % with existed replica', mv_part_name, dst_node_id;
	END IF;

	-- Get node ID of primary partition
	SELECT node_id,relation INTO master_node_id,rel_name FROM shardman.partitions WHERE part_name=mv_part_name;

	IF master_node_id=dst_node_id
	THEN
		RAISE EXCEPTION 'Can not move replica of partition % to primary node %', mv_part_name, dst_node_id;
	END IF;

	-- Alter publications at master node
	PERFORM shardman.broadcast(format('%s:ALTER PUBLICATION node_%s ADD TABLE %I;%s:ALTER PUBLICATION node_%s DROP TABLE %I;',
		master_node_id, dst_node_id, mv_part_name, master_node_id, src_node_id, mv_part_name));

	-- Refresh subscriptions
	PERFORM shardman.broadcast(format('%s:ALTER SUBSCRIPTION sub_%s_%s REFRESH PUBLICATION WITH (copy_data=false);'
									  '%s:ALTER SUBSCRIPTION sub_%s_%s REFRESH PUBLICATION;',
									  src_node_id, src_node_id, master_node_id,
									  dst_node_id, dst_node_id, master_node_id),
							   super_connstr => true);

	-- Wait completion of initial table sync
	PERFORM shardman.wait_sync_completion(master_node_id, dst_node_id);

	-- Update metadata
	UPDATE shardman.replicas SET node_id=dst_node_id WHERE node_id=src_node_id AND part_name=mv_part_name;

	-- Truncate original table
	PERFORM shardman.broadcast(format('%s:TRUNCATE TABLE %I;', src_node_id, mv_part_name));

	-- If there are more than one replica, we need to maintain change_log table for it
	IF shardman.get_redundancy_of_partition(mv_part_name) > 1
	THEN
		PERFORM shardman.broadcast(format('{%s:%s}{%s:%s}',
										   dst_node_id, shardman.create_on_change_triggers(rel_name, mv_part_name),
										   src_node_id, shardman.drop_on_change_triggers(mv_part_name)));
	END IF;
END
$$ LANGUAGE plpgsql;


-- Rebalance replicas between nodes. This function tries to evenly
-- redistribute replicas of partitions of tables which names match LIKE 'pattern'
-- between all nodes of replication groups.
-- It is not able to move replica between replication groups.
-- This function intentionally moves one replica per time to minimize
-- influence on system performance.
CREATE FUNCTION rebalance_replicas(replica_pattern text = '%') RETURNS void AS $$
DECLARE
	dst_node int;
	src_node int;
	min_count bigint;
	max_count bigint;
	mv_part_name text;
	repl_group text;
	done bool;
BEGIN
	IF shardman.redirect_to_shardlord(format('rebalance_replicas(%L)', replica_pattern))
	THEN
		RETURN;
	END IF;

	LOOP
		done := true;
		-- Repeat for all replication groups
		FOR repl_group IN SELECT DISTINCT replication_group FROM shardman.nodes
		LOOP
			-- Select node in this group with minimal number of replicas
			SELECT node_id, count(*) n_parts INTO dst_node, min_count
				FROM shardman.replicas r JOIN shardman.nodes n ON r.node_id=n.id
			    WHERE n.replication_group=repl_group AND r.relation LIKE replica_pattern
				GROUP BY node_id
				ORDER BY n_parts ASC LIMIT 1;
			-- Select node in this group with maximal number of partitions
			SELECT node_id, count(*) n_parts INTO src_node,max_count
			    FROM shardman.replicas r JOIN shardman.nodes n ON r.node_id=n.id
				WHERE n.replication_group=repl_group AND r.relation LIKE replica_pattern
				GROUP BY node_id
				ORDER BY n_parts DESC LIMIT 1;
			-- If difference of number of replicas on this nodes is greater
			-- than 1, then move random partition
			IF max_count - min_count > 1 THEN
			    SELECT src.part_name INTO mv_part_name
				FROM shardman.replicas src
				WHERE src.node_id=src_node AND src.relation LIKE replica_pattern
				    AND NOT EXISTS(SELECT * FROM shardman.replicas dst
							   WHERE dst.node_id=dst_node AND dst.part_name=src.part_name)
				    AND NOT EXISTS(SELECT * FROM shardman.partitions p
							   WHERE p.node_id=dst_node AND p.part_name=src.part_name)
				ORDER BY random() LIMIT 1;
				RAISE NOTICE 'Move replica of % from node % to %', mv_part_name, src_node, dst_node;
				PERFORM shardman.mv_replica(mv_part_name, src_node, dst_node);
				done := false;
			END IF;
		END LOOP;

		EXIT WHEN done;
	END LOOP;
END
$$ LANGUAGE plpgsql;

-- Map system identifier to node identifier.
CREATE FUNCTION get_node_by_sysid(sysid bigint) RETURNS int AS $$
DECLARE
    node_id int;
BEGIN
	SELECT shardman.broadcast(format('0:SELECT id FROM shardman.nodes WHERE system_id=%s;', sysid))::int INTO node_id;
	RETURN node_id;
END
$$ LANGUAGE plpgsql;

-- Get self node identifier.
CREATE FUNCTION get_my_id() RETURNS int AS $$
BEGIN
    RETURN shardman.get_node_by_sysid(shardman.get_system_identifier());
END
$$ LANGUAGE plpgsql;

-- Check consistency of cluster against metadata and perform recovery. All nodes
-- must be up for successfull completion. In general, at first we repair LR,
-- i.e. make sure tables with data are replicated properly, and only then fix
-- FDW mappings.
CREATE FUNCTION recover() RETURNS void AS $$
DECLARE
	dst_node shardman.nodes;
	src_node shardman.nodes;
	part shardman.partitions;
	repl shardman.replicas;
	t shardman.tables;
	repl_group text;
	server_opts text;
	um_opts text;
	table_attrs text;
	fdw_part_name text;
	srv_name text;
	create_table text;
	conn_string text;
	pub_name text;
	sub_name text;
	pubs text = '';
	subs text = '';
	sync text = '';
	conf text = '';
	old_replicated_tables text;
	new_replicated_tables text;
	replicated_tables text;
	subscribed_tables text;
	node_id int;
	sync_standbys text[];
	old_sync_policy text;
	new_sync_policy text;
	node record;
	prim name;
	foreign_part name;
	new_master_id int;
BEGIN
	IF shardman.redirect_to_shardlord('recover()')
	THEN
		RETURN;
	END IF;

	-- Remove potentially hanged temporary pub and sub used for mv_partition,
	-- truncate & forbid writes to not used partitions, unlock used partitions
	-- to fix up things after suddenly failed mv_partition. Yeah, since
	-- currently we don't log executed commands, we have to do that everywhere.
	FOR node IN SELECT n.id,
		ARRAY(SELECT prims.part_name FROM shardman.partitions prims WHERE n.id = prims.node_id) primary_parts,
		ARRAY(SELECT part_name FROM shardman.partitions
			   WHERE part_name NOT IN
					 (SELECT prims.part_name FROM shardman.partitions prims WHERE n.id = prims.node_id)
				 AND part_name NOT IN
					 (SELECT repls.part_name FROM shardman.replicas repls WHERE n.id = repls.node_id)) foreign_parts
		FROM shardman.nodes n
	LOOP
		subs := format('%s{%s:SELECT shardman.drop_copy_sub();', subs, node.id);
		FOREACH prim IN ARRAY node.primary_parts LOOP -- unlock local parts
			subs := format('%s SELECT shardman.write_protection_off(%L::regclass);',
						   subs, prim);
		END LOOP;
		FOREACH foreign_part IN ARRAY node.foreign_parts LOOP -- lock foreign parts
			subs := format('%s SELECT shardman.write_protection_on(%L::regclass);
						   TRUNCATE %I;',
						   subs, foreign_part, foreign_part);
		END LOOP;
		subs := subs || '}';

		pubs := format('%s%s:SELECT shardman.drop_copy_pub();', pubs, node.id);
	END LOOP;
	PERFORM shardman.broadcast(subs, super_connstr => true);
	PERFORM shardman.broadcast(pubs, super_connstr => true);

	-- Promote replica for parts without primary (or create new empty primary)
	-- To fix LR, we need to see parts as promoted,
	-- so make sure we are running in READ COMMITTED.
	ASSERT current_setting('transaction_isolation') = 'read committed',
		'recover must be executed with READ COMMITTED isolation level';
	FOR part IN SELECT * FROM shardman.partitions prts WHERE prts.node_id IS NULL
	LOOP
		-- Is there some replica of this part?
		SELECT r.node_id INTO new_master_id FROM shardman.replicas r
		  WHERE r.part_name=part.part_name ORDER BY random() LIMIT 1;
		IF new_master_id IS NOT NULL
		THEN -- exists some replica for this part, promote it
			RAISE DEBUG '[SHMN] Promoting part % on node %', part.part_name, new_master_id;
			-- If there are more than one replica of this partition, we need to
			-- synchronize them
			IF shardman.get_redundancy_of_partition(part.part_name) > 1
			THEN
				PERFORM shardman.synchronize_replicas(part.part_name);
			END IF;
			-- LR channels will be fixed below
		ELSE -- there is no replica: we have to create new empty partition at
			 -- random mode and redirect all FDWs to it
			RAISE WARNING 'Data of partition % was lost, creating empty partition.',
				part.part_name;
			SELECT id INTO new_master_id FROM shardman.nodes
				WHERE id<>rm_node_id ORDER BY random() LIMIT 1;
		END IF;

		-- Update metadata. XXX We should commit that before sending new
		-- mappings, because otherwise if we fail during the latter, news about
		-- promoted replica will be lost; next time we might choose another
		-- replica to promote with some new data already written to previously
		-- promoted replica. Syncing replicas doesn't help us much here if we
		-- don't lock tables.
		UPDATE shardman.partitions SET node_id=new_master_id
		 WHERE part_name = part.part_name;
		DELETE FROM shardman.replicas r WHERE r.part_name = part.part_name AND
											  r.node_id = new_master_id;
		-- PERFORM shardman.broadcast(format(
			-- '{0:UPDATE shardman.partitions SET node_id=%s WHERE part_name = %L;
			-- DELETE FROM shardman.replicas WHERE part_name = %L AND node_id = %s}',
			-- new_master_id, part.part_name, part.part_name, new_master_id));
	END LOOP;

	-- Fix replication channels
	pubs := '';
	subs := '';
	FOR repl_group IN SELECT DISTINCT replication_group FROM shardman.nodes
	LOOP
		FOR src_node IN SELECT * FROM shardman.nodes WHERE replication_group = repl_group
		LOOP
			FOR dst_node IN SELECT * FROM shardman.nodes
				WHERE replication_group = repl_group AND id <> src_node.id
			LOOP
				pub_name := format('node_%s', dst_node.id);
				sub_name := format('sub_%s_%s', dst_node.id, src_node.id);

				-- Construct list of partitions which need to be published from
				-- src node to dst node
				SELECT coalesce(string_agg(pname, ','), '') INTO replicated_tables FROM
					(SELECT p.part_name pname FROM shardman.partitions p, shardman.replicas r
					  WHERE p.node_id = src_node.id AND r.node_id = dst_node.id AND
							p.part_name = r.part_name ORDER BY p.part_name) parts;

				pubs := format('%s%s:SELECT shardman.recover_pub(%L, %L);',
							   pubs, src_node.id, pub_name, replicated_tables);

				-- Create subscription if not exists. Otherwise, if list of
				-- subscribed tables is not up-to-date, refresh it.
				IF shardman.not_exists(dst_node.id, format(
					'pg_subscription WHERE subname=%L', sub_name))
				THEN
					RAISE NOTICE 'Creating subscription % at node %', sub_name, dst_node.id;
					subs := format('%s%s:CREATE SUBSCRIPTION %I CONNECTION %L PUBLICATION %I WITH (copy_data=false, create_slot=false, slot_name=%L, synchronous_commit=local);',
						 subs, dst_node.id, sub_name, src_node.connection_string, pub_name, pub_name);
				ELSE
					subscribed_tables := shardman.broadcast(format(
						$subrels$%s:SELECT coalesce(string_agg(srrelid::regclass::name, ','
															   ORDER BY srrelid::regclass::name), '')
						FROM pg_subscription_rel sr, pg_subscription s
						WHERE sr.srsubid = s.oid AND s.subname = %L
						GROUP BY srrelid;$subrels$,
						dst_node.id, sub_name));
					IF subscribed_tables <> replicated_tables THEN
						subs := format('%s%s:ALTER SUBSCRIPTION %I REFRESH PUBLICATION WITH (copy_data=false);',
									   subs, dst_node.id, sub_name, pub_name);
					END IF;
				END IF;
			END LOOP;

			-- Restore synchronous standby list
			IF shardman.synchronous_replication()
			THEN
				new_sync_policy := shardman.construct_ssnames(src_node.id);

				SELECT shardman.broadcast(format(
					'%s:SELECT setting from pg_settings
					WHERE name=''synchronous_standby_names'';', src_node.id))
				INTO old_sync_policy;

				IF old_sync_policy <> new_sync_policy
				THEN
					RAISE NOTICE 'Alter synchronous_standby_names to ''%'' at node %', new_sync_policy, src_node.id;
					sync := format('%s%s:ALTER SYSTEM SET synchronous_standby_names to %L;',
								   sync, src_node.id, new_sync_policy);
					conf := format('%s%s:SELECT pg_reload_conf();', conf, src_node.id);
				END IF;
			END IF;
		END LOOP;
	END LOOP;

	-- Create missing publications and repslots
	PERFORM shardman.broadcast(pubs, super_connstr => true);
	-- Create missing subscriptions
	PERFORM shardman.broadcast(subs, super_connstr => true);

	IF sync <> ''
	THEN -- Alter synchronous_standby_names if needed
		-- alter system must be one-line command, don't set synchronous_commit
		PERFORM shardman.broadcast(sync, super_connstr => true, sync_commit_on => true);
    	PERFORM shardman.broadcast(conf, super_connstr => true);
	END IF;

	-- Restore FDWs
	FOR src_node IN SELECT * FROM shardman.nodes
	LOOP
		-- Restore foreign servers
		FOR dst_node IN SELECT * FROM shardman.nodes
		LOOP
			IF src_node.id<>dst_node.id
			THEN
				-- Create foreign server if not exists
				srv_name := format('node_%s', dst_node.id);
				IF shardman.not_exists(src_node.id, format(
					'pg_foreign_server WHERE srvname=%L', srv_name))
				THEN
					SELECT * FROM shardman.conninfo_to_postgres_fdw_opts(dst_node.connection_string)
						INTO server_opts, um_opts;
					RAISE NOTICE 'Creating foreign server % at node %',
						srv_name, src_node.id;
					PERFORM shardman.broadcast(format(
						'{%s:CREATE SERVER %I FOREIGN DATA WRAPPER postgres_fdw %s;
			 			CREATE USER MAPPING FOR CURRENT_USER SERVER %I %s}',
						src_node.id, srv_name, server_opts,
			 	   		srv_name, um_opts));
				END IF;
			END IF;
		END LOOP;

		-- Restore foreign tables
		FOR part IN SELECT * FROM shardman.partitions
		LOOP
			fdw_part_name := format('%s_fdw', part.part_name);
			-- Create parent table if not exists
			IF shardman.not_exists(src_node.id,
								   format('pg_class WHERE relname=%L', part.relation))
			THEN
				RAISE NOTICE 'Creating table % at node %', part.relation, src_node_id;
				SELECT create_sql INTO create_table FROM sharman.tables WHERE relation=part.relation;
				PERFORM shardman.broadcast(format('{%s:%s}', src_node.id, create_table));
			END IF;

			IF part.node_id <> src_node.id
			THEN -- part is foreign partition for src node
				srv_name := format('node_%s', part.node_id);

				-- Create foreign table if not exists
				IF shardman.not_exists(src_node.id, format(
					'pg_class c, pg_foreign_table f WHERE c.oid=f.ftrelid AND c.relname=%L',
					fdw_part_name))
				THEN
					RAISE NOTICE 'Creating foreign table % at node %', fdw_part_name, src_node.id;
					SELECT shardman.reconstruct_table_attrs(part.relation) INTO table_attrs;
					PERFORM shardman.broadcast(format(
						'%s:CREATE FOREIGN TABLE %I %s SERVER %s OPTIONS (table_name %L);',
						src_node.id, fdw_part_name, table_attrs, srv_name, part.part_name));
				ELSIF shardman.not_exists(src_node.id, format(
					'pg_class c, pg_foreign_table f, pg_foreign_server s
					 WHERE c.oid=f.ftrelid AND c.relname=%L AND
					f.ftserver=s.oid AND s.srvname = %L',
					fdw_part_name, srv_name))
				THEN
					RAISE NOTICE 'Binding foreign table % to server % at node %',
					fdw_part_name, srv_name, src_node.id;
					PERFORM shardman.broadcast(format(
						'%s:SELECT shardman.alter_ftable_set_server(%L, %L);',
		   				src_node.id, fdw_part_name, srv_name));
				END IF;

				-- Check if parent table contains foreign table as child
				IF shardman.not_exists(src_node.id, format(
					'pg_class p, pg_inherits i, pg_class c WHERE p.relname=%L AND
					p.oid=i.inhparent AND i.inhrelid=c.oid AND c.relname=%L',
				   	part.relation, fdw_part_name))
				THEN
					-- If parent table contains neither local nor foreign
					-- partitions, then assume that table was not partitioned at
					-- all
					IF shardman.not_exists(src_node.id, format(
						'pg_class p, pg_inherits i, pg_class c WHERE p.relname=%L AND
						p.oid=i.inhparent AND i.inhrelid=c.oid AND c.relname=%L',
				   		part.relation, part.part_name))
					THEN
						RAISE NOTICE 'Create hash partitions for table % at node %',
							part.relation, src_node.id;
						SELECT * INTO t FROM shardman.tables WHERE relation=part.relation;
						PERFORM shardman.broadcast(format(
							'%s:SELECT create_hash_partitions(%L,%L,%L);',
							src_node.id, t.relation, t.sharding_key, t.partitions_count),
							iso_level => 'read committed');
					END IF;
					RAISE NOTICE 'Replace % with % at node %',
						part.part_name, fdw_part_name, src_node.id;
					PERFORM shardman.broadcast(format(
						'%s:SELECT replace_hash_partition(%L,%L);',
						src_node.id, part.part_name, fdw_part_name));
				END IF;
			ELSE -- part is local partition for src node
				-- Check if parent table contains local partition as a child
				IF shardman.not_exists(src_node.id, format(
					'pg_class p, pg_inherits i, pg_class c
					WHERE p.relname=%L AND p.oid=i.inhparent AND i.inhrelid=c.oid AND c.relname=%L',
			   	   	part.relation, part.part_name))
				THEN
					-- If parent table contains neither local neither foreign
					-- partitions, then assume that table was not partitioned at
					-- all
					IF shardman.not_exists(src_node.id, format(
						'pg_class p,pg_inherits i,pg_class c WHERE p.relname=%L AND
						p.oid=i.inhparent AND i.inhrelid=c.oid AND c.relname=%L',
					   	part.relation, fdw_part_name))
				    THEN
						RAISE NOTICE 'Create hash partitions for table % at node %', part.relation, src_node.id;
						SELECT * INTO t FROM shardman.tables WHERE relation=part.relation;
						PERFORM shardman.broadcast(format(
							'%s:SELECT create_hash_partitions(%L, %L, %L);',
							src_node.id, t.relation, t.sharding_key, t.partitions_count),
							iso_level => 'read committed');
					ELSE
						RAISE NOTICE 'Replace % with % at node %',
							fdw_part_name, part.part_name, src_node.id;
						PERFORM shardman.broadcast(format(
							'%s:SELECT replace_hash_partition(%L, %L);',
							src_node.id, fdw_part_name, part.part_name));
					END IF;
				END IF;
			END IF;
		END LOOP;
	END LOOP;

	-- Restore shared tables
	pubs := '';
	subs := '';
	FOR t IN SELECT * FROM shardman.tables WHERE master_node IS NOT NULL
	LOOP
		-- Create table if not exists
		IF shardman.not_exists(t.master_node, format('pg_class WHERE relname=%L', t.relation))
		THEN
			RAISE NOTICE 'Create table % at node %', t.relation, t.master_node;
			PERFORM shardman.broadcast(format('{%s:%s}', t.master_node, t.create_sql));
		END IF;

		-- Construct list of shared tables at this node
		SELECT string_agg(pname, ',') INTO new_replicated_tables FROM
		(SELECT relation AS pname FROM shardman.tables WHERE master_node=t.master_node ORDER BY relation) shares;

		SELECT string_agg(pname, ',') INTO old_replicated_tables FROM
		(SELECT c.relname pname FROM pg_publication p,pg_publication_rel r,pg_class c WHERE p.pubname='shared_tables' AND p.oid=r.prpubid AND r.prrelid=c.oid ORDER BY c.relname) shares;

		-- Create publication if not exists
		IF shardman.not_exists(t.master_node, 'pg_publication WHERE pubname=''shared_tables''')
		THEN
			RAISE NOTICE 'Create publication shared_tables at node %', master_node_id;
			pubs := format('%s%s:CREATE PUBLICATION shared_tables FOR TABLE %s;',
				 pubs, t.master_node, new_replicated_tables);
		ELSIF new_replicated_tables<>old_replicated_tables
		THEN
			RAISE NOTICE 'Alter publication shared_tables at node %', master_node_id;
			pubs := format('%s%s:ALTER PUBLICATION shared_tables SET TABLE %s;',
				 pubs, t.master_node, new_replicated_tables);
		END IF;

		SELECT connection_string INTO conn_string FROM shardman.nodes WHERE id=t.master_node;
		srv_name := format('node_%s', t.master_node);

		-- Create replicas of shared table at all nodes if not exist
		FOR node_id IN SELECT id from shardman.nodes WHERE id<>t.master_node
		LOOP
			-- Create table if not exists
			IF shardman.not_exists(node_id, format('pg_class WHERE relname=%L', t.relation))
			THEN
				RAISE NOTICE 'Create table % at node %', t.relation, node_id;
				PERFORM shardman.broadcast(format('{%s:%s}', node_id, t.create_sql));
			END IF;

			-- Create foreign table if not exists
			fdw_part_name := format('%s_fdw', t.relation);
			IF shardman.not_exists(node_id, format('pg_class c,pg_foreign_table f WHERE c.oid=f.ftrelid AND c.relname=%L', fdw_part_name))
			THEN
				RAISE NOTICE 'Create foreign table %I at node %', fdw_part_name, node_id;
				SELECT shardman.reconstruct_table_attrs(t.relation) INTO table_attrs;
				PERFORM shardman.broadcast(format('%s:CREATE FOREIGN TABLE % %s SERVER %s OPTIONS (table_name %L);',
					node_id, fdw_part_name, table_attrs, srv_name, t.relation));
			END IF;

			-- Create rules if not exists
			IF shardman.not_exists(node_id, format('pg_rules WHERE tablename=%I AND rulename=''on_update''', t.relation))
			THEN
				RAISE NOTICE 'Create rules for table % at node %', t.relation, node_id;
				PERFORM shardman.broadcast(format('{%s:%s}', node_id, t.create_rules_sql));
			END IF;

			-- Create subscription to master if not exists
			sub_name := format('share_%s_%s', node_id, t.master_node);
			IF shardman.not_exists(node.id, format('pg_subscription WHERE slot_name=%L', sub_name))
			THEN
				RAISE NOTICE 'Create subscription % at node %', sub_name, node_id;
				subs := format('%s%s:CREATE SUBSCRIPTION %I CONNECTION %L PUBLICATION shared_tables with (copy_data=false, synchronous_commit=local);',
					 subs, node_id, sub_name, conn_string);
			END IF;
		END LOOP;
	END LOOP;

	-- Create not existed publications
	PERFORM shardman.broadcast(pubs, super_connstr => true);
	-- Create not existed subscriptions
	PERFORM shardman.broadcast(subs, super_connstr => true);

	-- Create not existed on_change triggers
	PERFORM shardman.generate_on_change_triggers();
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION drop_copy_sub() RETURNS void AS $$
DECLARE
	sub_name name;
BEGIN
	FOR sub_name IN SELECT subname FROM pg_subscription WHERE subname LIKE 'shardman_copy_%'
	LOOP
		PERFORM shardman.eliminate_sub(sub_name);
	END LOOP;
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION drop_copy_pub() RETURNS void AS $$
DECLARE
	pub_name name;
	slotname name;
BEGIN
	FOR pub_name IN SELECT pubname FROM pg_publication WHERE pubname LIKE 'shardman_copy_%'
	LOOP
		EXECUTE format('DROP PUBLICATION %I', pub_name);
	END LOOP;
	FOR slotname IN SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'shardman_copy_%'
	LOOP
		PERFORM pg_drop_replication_slot(slotname);
	END LOOP;
END
$$ LANGUAGE plpgsql;

-- Make sure pub & slot on the node exists and proper parts are published.
-- 'replicated_tables' is comma-separated list of tables to publish, probably
-- empty string. It is ordered for easier comparison.
-- If everything is ok, we don't touch it.
CREATE FUNCTION recover_pub(pub_name name, replicated_tables text) RETURNS void AS $$
DECLARE
	currently_replicated_tables text := COALESCE(string_agg(pname, ','), '') FROM
		(SELECT c.relname pname FROM pg_publication p, pg_publication_rel r, pg_class c
		  WHERE p.pubname = pub_name AND p.oid = r.prpubid AND r.prrelid=c.oid ORDER BY c.relname) parts;
BEGIN
	IF NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=pub_name) THEN
		PERFORM pg_create_logical_replication_slot(pub_name, 'pgoutput');
	END IF;

	-- in this case it is simpler to recreate the pub, 'alter pub set table to;'
	-- is not allowed
	IF replicated_tables = '' AND currently_replicated_tables <> replicated_tables THEN
		EXECUTE format('DROP PUBLICATION IF EXISTS %I', pub_name);
	END IF;
	IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = pub_name) THEN
		RAISE LOG '[SHMN] recovery: creating publication %s', pub_name;
		EXECUTE format('CREATE PUBLICATION %I', pub_name);
	END IF;
	IF replicated_tables <> '' AND currently_replicated_tables <> replicated_tables THEN
		RAISE LOG '[SHMN] recovery: alter publication %s set table %s', pub_name, replicated_tables;
		EXECUTE format('ALTER PUBLICATION %I SET TABLE %s', pub_name, replicated_tables);
	END IF;
END
$$ LANGUAGE plpgsql;


-- Alter table at shardlord and all nodes
CREATE FUNCTION alter_table(rel regclass, alter_clause text) RETURNS void AS $$
DECLARE
	rel_name text = rel::text;
	t shardman.tables;
	repl shardman.replicas;
	create_table text;
	create_rules text;
	rules text = '';
	alters text = '';
	node_id int;
BEGIN
	IF shardman.redirect_to_shardlord(format('alter_table(%L,%L)', rel_name, alter_clause))
	THEN
		RETURN;
	END IF;

	-- Alter root table everywhere
	PERFORM shardman.forall(format('ALTER TABLE %I %s', rel_name, alter_clause), including_shardlord=>true);

	SELECT * INTO t FROM shardman.tables WHERE relation=rel_name;
	SELECT shardman.gen_create_table_sql(t.relation) INTO create_table;

	-- Broadcast new rules
	IF t.master_node IS NOT NULL
	THEN
		SELECT shardman.gen_create_rules_sql(t.relation, format('%s_fdw', t.relation)) INTO create_rules;
		FOR node_id IN SELECT * FROM shardman.nodes WHERE id<>t.master_node
		LOOP
			rules :=  format('%s{%s:%s}',
				  rules, node_id, create_rules);
		END LOOP;
		PERFORM shardman.broadcast(rules);
	END IF;
	UPDATE shardman.tables SET create_sql=create_table, create_rules_sql=create_rules WHERE relation=t.relation;

	-- Alter all replicas
	FOR repl IN SELECT * FROM shardman.replicas
	LOOP
		alters := format('%s%s:ALTER TABLE %I %s;',
			   alters, repl.node_id, repl.part_name, alter_clause);
	END LOOP;
	PERFORM shardman.broadcast(alters);
END
$$ LANGUAGE plpgsql;


-- Commit or rollback not completed distributed transactions.
-- All nodes must be alive for this to do something.
-- If coordinator is still in the cluster, we just try asking it:
--   if xact committed on it, we commit it everywhere, if aborted, abort
--   everywhere.
-- If not, and there is only one participant, we simply commit the xact.
-- If n_participants > 1, and xact is prepared everywhere, commit it.
-- Otherwise, check WAL of every node; if COMMIT is found, COMMIT, if ABORT
-- is found, ABORT.
--
-- Currently this function is not too hasty because
-- * We make totally independent decisions for each 'prepare'.
-- * We never know the participants and poll all nodes in the cluster.
-- * If coordinator is excluded, we sequentially examine WAL of *all* nodes to
--   learn the outcome, even where xact is prepared.
CREATE FUNCTION recover_xacts() RETURNS void AS $$
DECLARE
	node_id int;
	xacts text[];
	xact_node_id int;
	xact text;
    cmds text = '';
	gid text;
	xid bigint;
	sysid bigint;
	counter text;
	coordinator int;
	status text;
	n_participants int;
	n_prepared int;
	resp text;
	do_commit bool;
	do_rollback bool;
	finish text = '';
BEGIN
	IF shardman.redirect_to_shardlord('recover_xacts()')
	THEN
		RETURN;
	END IF;

	FOR node_id IN SELECT id FROM shardman.nodes
	LOOP
		cmds := format($cmd$
			%s%s:SELECT coalesce(string_agg('%s=>' || gid, ','), '') FROM pg_prepared_xacts;$cmd$,
			cmds, node_id, node_id);
	END LOOP;

	-- Collected prepared xacts from all nodes. They arrive as comma-separated
	-- $node_id=>$gid
	xacts := string_to_array(shardman.broadcast(cmds), ',');
	-- empty string means no prepared xacts
	xacts := array_remove(xacts, '');

	FOREACH xact IN ARRAY xacts
	LOOP
		xact_node_id := split_part(xact, '=>', 1);
		gid := split_part(xact, '=>', 2);
		sysid := split_part(gid, ':', 3)::bigint;
		xid := split_part(gid, ':', 4)::bigint; -- coordinator's xid
		SELECT id INTO coordinator FROM shardman.nodes WHERE system_id=sysid;
		IF coordinator IS NULL
		THEN
			-- Coordinator node is not available
			RAISE NOTICE 'Coordinator of transaction % is not available', gid;
			n_participants := split_part(gid, ':', 5)::int;
			IF n_participants > 1
			THEN
				-- Poll all participants.
				-- First of all try portable way: get information from pg_prepared_xacts.
				cmds := '';
				FOR node_id IN SELECT id FROM shardman.nodes
				LOOP
					cmds := format('%s%s:SELECT COUNT(*) FROM pg_prepared_xacts WHERE gid=%L;', cmds, node_id, gid);
				END LOOP;
				SELECT shardman.broadcast(cmds) INTO resp;

				n_prepared := 0;
				FOREACH counter IN ARRAY string_to_array(resp, ',')
				LOOP
					n_prepared := n_prepared + counter::int;
				END LOOP;

				IF n_prepared = n_participants
				THEN
					RAISE NOTICE 'Commit distributed transaction % which is prepared at all participant nodes', gid;
					finish := format('%s%s:COMMIT PREPARED %L;', finish, xact_node_id, gid);
				ELSE
					RAISE NOTICE 'Distributed transaction % is prepared at % nodes from %',
						  gid, n_prepared, n_participants;

					IF EXISTS (SELECT * FROM pg_proc WHERE proname='pg_prepared_xact_status')
					THEN
						-- Without coordinator there is no standard way to get
						-- status of this distributed transaction. Use PGPRO-EE
						-- pg_prepared_xact_status() function if available
						cmds := '';
						FOR node_id IN SELECT id FROM shardman.nodes
						LOOP
							cmds := format('%s%s:SELECT pg_prepared_xact_status(%L);',
										   cmds, node_id, gid);
						END LOOP;
						SELECT shardman.broadcast(cmds) INTO resp;

						-- Collect information about distributed transaction
						-- status at all nodes
						do_commit := false;
						do_rollback := false;
						FOREACH status IN ARRAY string_to_array(resp, ',')
						LOOP
							IF status='committed'
							THEN
								do_commit := true;
							ELSIF status='aborted'
							THEN
								do_rollback := true;
							END IF;
						END LOOP;

						IF do_commit
						THEN
							IF do_rollack
							THEN
								RAISE WARNING 'Inconsistent state of transaction %',
								gid;
							ELSE
								RAISE NOTICE 'Committing transaction %s at node % because it was committed at one of participants',
									gid, xact_node_id;
								finish := format('%s%s:COMMIT PREPARED %L;', finish, xact_node_id, gid);
							END IF;
						ELSIF do_rollback
						THEN
							RAISE NOTICE 'Abort transaction %s at node % because if was aborted at one of participants',
								gid, xact_node_id;
							finish := format('%s%s:ROLLBACK PREPARED %L;', finish, xact_node_id, gid);
						ELSE
							RAISE NOTICE 'Can''t make any decision concerning distributed transaction %', gid;
						END IF;
					ELSE
						RAISE WARNING 'Can''t perform 2PC resolution of xact % at node % with vanilla PostgreSQL',
							gid, xact_node_id;
					END IF;
				END IF;
			ELSE
				RAISE NOTICE 'Committing transaction % with single participant %', gid, xact_node_id;
				finish := format('%s%s:COMMIT PREPARED %L;', finish, xact_node_id, gid);
			END IF;
		ELSE
			-- Check status of transaction at coordinator
			SELECT shardman.broadcast(format('%s:SELECT txid_status(%s);', coordinator, xid))
			  INTO status;
			RAISE NOTICE 'Status of distributed transaction % is % at coordinator %',
				gid, status, coordinator;
			IF status='committed'
			THEN
				finish := format('%s%s:COMMIT PREPARED %L;', finish, xact_node_id, gid);
			ELSIF status='aborted'
			THEN
				finish := format('%s%s:ROLLBACK PREPARED %L;', finish, xact_node_id, gid);
			ELSEIF status IS NULL
			THEN
				RAISE WARNING 'Transaction % at coordinator % is too old to perform 2PC resolution or still in progress',
				gid, coordinator;
			END IF;
		END IF;
	END LOOP;

	-- Finish all prepared transactions for which decision was made
	PERFORM shardman.broadcast(finish, sync_commit_on => true);
END
$$ LANGUAGE plpgsql;

-- Generate rules for redirecting updates for shared table
CREATE FUNCTION gen_create_rules_sql(rel_name text, fdw_name text) RETURNS text AS $$
DECLARE
	pk text;
	dst text;
	src text;
BEGIN
	-- Construct list of attributes of the table for update/insert
	SELECT INTO dst, src
          string_agg(quote_ident(attname), ', '),
		  string_agg('NEW.' || quote_ident(attname), ', ')
    FROM   pg_attribute
    WHERE  attrelid = rel_name::regclass
    AND    NOT attisdropped   -- no dropped (dead) columns
    AND    attnum > 0;

	-- Construct primary key condition for update
	SELECT INTO pk
          string_agg(quote_ident(a.attname) || '=OLD.'|| quote_ident(a.attname), ' AND ')
	FROM   pg_index i
	JOIN   pg_attribute a ON a.attrelid = i.indrelid
                     AND a.attnum = ANY(i.indkey)
    WHERE  i.indrelid = rel_name::regclass
    AND    i.indisprimary;

	RETURN format('CREATE OR REPLACE RULE on_update AS ON UPDATE TO %I DO INSTEAD UPDATE %I SET (%s) = (%s) WHERE %s;
		           CREATE OR REPLACE RULE on_insert AS ON INSERT TO %I DO INSTEAD INSERT INTO %I (%s) VALUES (%s);
		           CREATE OR REPLACE RULE on_delete AS ON DELETE TO %I DO INSTEAD DELETE FROM %I WHERE %s;',
        rel_name, fdw_name, dst, src, pk,
		rel_name, fdw_name, dst, src,
		rel_name, fdw_name, pk);
END
$$ LANGUAGE plpgsql;

-- Check if resource exists at remote node
CREATE FUNCTION not_exists(node_id int, what text) RETURNS bool AS $$
DECLARE
	req text;
	resp text;
BEGIN
	req := format('%s:SELECT count(*) FROM %s;', node_id, what);
	SELECT shardman.broadcast(req) INTO resp;
	return resp::bigint=0;
END
$$ LANGUAGE plpgsql;

-- Execute command at shardlord
CREATE FUNCTION redirect_to_shardlord(cmd text) RETURNS bool AS $$
BEGIN
	IF NOT shardman.is_shardlord()
	THEN
	    RAISE NOTICE 'Redirect command "%" to shardlord',cmd;
		RETURN true;
	ELSE
		RETURN false;
	END IF;
END
$$ LANGUAGE plpgsql;

-- Generate based on information from catalog SQL statement creating this table
CREATE FUNCTION gen_create_table_sql(relation text)
RETURNS text AS 'pg_shardman' LANGUAGE C STRICT;

-- Reconstruct table attributes for foreign table
CREATE FUNCTION reconstruct_table_attrs(relation regclass)
RETURNS text AS 'pg_shardman' LANGUAGE C STRICT;

-- Broadcast SQL commands to nodes and wait their completion.
-- cmds is list of SQL commands terminated by semi-columns with node
-- prefix: node-id:sql-statement;
-- To run multiple statements on node, wrap them in {}:
-- {node-id:statement; statement;}
-- All statements are run in one transaction.
-- Don't specify node id twice with 2pc, we use only one prepared_xact name.
-- Node id '0' means shardlord, shardlord_connstring guc is used.
-- No escaping is performed, so ';', '{' and '}' inside queries are not supported.
-- By default function throws error of type
-- 'external_routine_invocation_exception' if execution has failed at some of
-- the nodes. With ignore_errors=true errors are ignored and each error is
-- appended to the function result in form of "<error>${node_id}:Something
-- bad</error>"
-- In case of normal completion this function returns comma-separated results
-- for each run. A "result" is the first column of the first row of last
-- statement.
-- If two_phase parameter is true, then each statement is firstly prepared with
-- subsequent commit or rollback of prepared transaction at second phase of two
-- phase commit.
-- If sync_commit_on is false, we set session synchronous_commit to local.
-- If sequential is true, we send text cmd only when at least one statement for
-- previous was already executed.
-- If super_connstr is true, super connstring is used everywhere, usual
-- connstr otherwise.

-- If iso_level is specified, cmd is wrapped in BEGIN TRANSACTION ISOLATION
-- LEVEL iso_level;  ... END;
-- this allows to set isolation level; however you won't be able to get results
-- this way.
CREATE FUNCTION broadcast(cmds text,
						  ignore_errors bool = false,
						  two_phase bool = false,
						  sync_commit_on bool = false,
						  sequential bool = false,
						  super_connstr bool = false,
						  iso_level text = null)
RETURNS text AS 'pg_shardman' LANGUAGE C;

-- Options to postgres_fdw are specified in two places: user & password in user
-- mapping and everything else in create server. The problem is that we use
-- single conn_string, however user mapping and server doesn't understand this
-- format, i.e. we can't say create server ... options (dbname 'port=4848
-- host=blabla.org'). So we have to parse the opts and pass them manually. libpq
-- knows how to do it, but doesn't expose that. On the other hand, quote_literal
-- (which is necessary here) doesn't seem to have handy C API. I resorted to
-- have C function which parses the opts and returns them in two parallel
-- arrays, and this sql function joins them with quoting. TODO: of course,
-- quote_literal_cstr exists.
-- Returns two strings: one with opts ready to pass to CREATE FOREIGN SERVER
-- stmt, and one with opts ready to pass to CREATE USER MAPPING.
CREATE FUNCTION conninfo_to_postgres_fdw_opts(IN conn_string text,
	OUT server_opts text, OUT um_opts text) RETURNS record AS $$
DECLARE
	conn_string_keywords text[];
	conn_string_vals text[];
	server_opts_first_time_through bool = true;
	um_opts_first_time_through bool = true;
BEGIN
	server_opts := '';
	um_opts := '';
	SELECT * FROM shardman.pq_conninfo_parse(conn_string)
	  INTO conn_string_keywords, conn_string_vals;
	FOR i IN 1..array_upper(conn_string_keywords, 1) LOOP
		IF conn_string_keywords[i] = 'client_encoding' OR
			conn_string_keywords[i] = 'fallback_application_name' THEN
			CONTINUE; /* not allowed in postgres_fdw */
		ELSIF conn_string_keywords[i] = 'user' OR
			conn_string_keywords[i] = 'password' THEN -- user mapping option
			IF NOT um_opts_first_time_through THEN
				um_opts := um_opts || ', ';
			END IF;
			um_opts_first_time_through := false;
			um_opts := um_opts ||
				format('%s %L', conn_string_keywords[i], conn_string_vals[i]);
		ELSE -- server option
			IF NOT server_opts_first_time_through THEN
				server_opts := server_opts || ', ';
			END IF;
			server_opts_first_time_through := false;
			server_opts := server_opts ||
				format('%s %L', conn_string_keywords[i], conn_string_vals[i]);
		END IF;
	END LOOP;

	-- OPTIONS () is syntax error, so add OPTIONS only if we really have opts
	IF server_opts != '' THEN
		server_opts := format(' OPTIONS (%s)', server_opts);
	END IF;
	IF um_opts != '' THEN
		um_opts := format(' OPTIONS (%s)', um_opts);
	END IF;
END $$ LANGUAGE plpgsql STRICT;

-- Parse connection string. This function is used by
-- conninfo_to_postgres_fdw_opts to construct postgres_fdw options list.
CREATE FUNCTION pq_conninfo_parse(IN conninfo text, OUT keys text[], OUT vals text[])
	RETURNS record AS 'pg_shardman' LANGUAGE C STRICT;

-- Get shardlord connection string from configuration parameters
CREATE FUNCTION shardlord_connection_string()
	RETURNS text AS 'pg_shardman' LANGUAGE C STRICT;

-- Check from configuration parameters if synchronous replication mode was enabled
CREATE FUNCTION synchronous_replication()
	RETURNS bool AS 'pg_shardman' LANGUAGE C STRICT;

-- Check from configuration parameters if node plays role of shardlord
CREATE FUNCTION is_shardlord()
	RETURNS bool AS 'pg_shardman' LANGUAGE C STRICT;

-- Get subscription status
CREATE FUNCTION is_subscription_ready(sname text) RETURNS bool AS $$
DECLARE
	n_not_ready bigint;
BEGIN
	SELECT count(*) INTO n_not_ready FROM pg_subscription_rel srel
		JOIN pg_subscription s ON srel.srsubid = s.oid WHERE subname=sname AND srsubstate<>'r';
	RETURN n_not_ready=0;
END
$$ LANGUAGE plpgsql;

-- Wait initial sync completion
CREATE FUNCTION wait_sync_completion(src_node_id int, dst_node_id int) RETURNS void AS $$
DECLARE
	timeout_sec int = 1;
	response text;
BEGIN
	LOOP
	    response := shardman.broadcast(format('%s:SELECT shardman.is_subscription_ready(''sub_%s_%s'');',
			dst_node_id, dst_node_id, src_node_id));
		EXIT WHEN response::bool;
		PERFORM pg_sleep(timeout_sec);
	END LOOP;
END
$$ LANGUAGE plpgsql;


-- Wait completion of partition copy using LR. After successfull execution,
-- parts are fully synced and src part is locked.
CREATE FUNCTION wait_copy_completion(src_node_id int, dst_node_id int, part_name text) RETURNS void AS $$
DECLARE
	slot text = format('shardman_copy_%s', part_name);
	lag bigint;
	response text;
	caughtup_threshold bigint = 1024*1024;
	timeout_sec int = 1;
    locked bool = false;
	synced bool = false;
	wal_lsn text;
BEGIN
	LOOP
		IF NOT synced
		THEN
		    response := shardman.broadcast(format(
				'%s:SELECT shardman.is_subscription_ready(%L);',
				dst_node_id, slot));
			IF response::bool THEN
			    synced := true;
				RAISE DEBUG '[SHMN] Table % initial sync completed', part_name;
				CONTINUE;
			END IF;
	    ELSE
    	    IF locked THEN -- waiting for final sync
				response := shardman.broadcast(format(
					'%s:SELECT %L - confirmed_flush_lsn FROM pg_replication_slots
					WHERE slot_name=%L;', src_node_id, wal_lsn, slot));
			ELSE -- waiting for the lag < catchup threshold to lock
			    response := shardman.broadcast(format(
					'%s:SELECT pg_current_wal_lsn() - confirmed_flush_lsn
					FROM pg_replication_slots WHERE slot_name=%L;',
					src_node_id, slot));
			END IF;
			lag := response::bigint;

			RAISE DEBUG '[SHMN] wait_copy_completion %: replication lag %',
				part_name, lag;
			IF locked THEN
		        IF lag <= 0 THEN
			   	    RETURN;
			    END IF;
			ELSIF lag < caughtup_threshold THEN
	   	        PERFORM shardman.broadcast(format(
					'%s:CREATE TRIGGER write_protection BEFORE INSERT OR UPDATE OR DELETE ON
					%I FOR EACH STATEMENT EXECUTE PROCEDURE shardman.deny_access();',
					src_node_id, part_name));
				SELECT shardman.broadcast(format('%s:SELECT pg_current_wal_lsn();',
												 src_node_id)) INTO wal_lsn;
				locked := true;
				CONTINUE;
			END IF;
		END IF;
		PERFORM pg_sleep(timeout_sec);
	END LOOP;
END
$$ LANGUAGE plpgsql;

-- Disable writes to the partition, if we are not replica. This is handy because
-- we use replication to copy table.
CREATE FUNCTION write_protection_on(part regclass) RETURNS void AS $$
BEGIN
	IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'write_protection' AND
												  tgrelid = part) THEN
		EXECUTE format('CREATE TRIGGER write_protection BEFORE INSERT OR UPDATE OR DELETE ON
					   %I FOR EACH STATEMENT EXECUTE PROCEDURE shardman.deny_access();',
					   part::name);
	END IF;
END
$$ LANGUAGE plpgsql;

-- Enable writes to the partition back again
CREATE FUNCTION write_protection_off(part regclass) RETURNS void AS $$
BEGIN
	IF EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'write_protection' AND
											  tgrelid = part) THEN
		EXECUTE format('DROP TRIGGER write_protection ON %I', part::name);
	END IF;
END
$$ LANGUAGE plpgsql;

-- Trigger procedure prohibiting modification of the table
CREATE FUNCTION deny_access() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'This partition was moved to another node. Run shardman.recover(), if this error persists.';
END
$$ LANGUAGE plpgsql;

-- In case of primary node failure ensure that all replicas are identical using change_log table.
-- We check last seqno stored in change_log at all replicas and for each lagging replica (last_seqno < max_seqno) perform three actions:
-- 1. Copy missing part of change_log table
-- 2. Delete all records from partition which primary key=old_pk in change_log table with seqno>last_seqno
-- 3. Copy from advanced replica those records which primary key=new_pk in change_log table with seqno>last_seqno
CREATE FUNCTION synchronize_replicas(pname text) RETURNS void AS $$
DECLARE
	max_seqno bigint = 0;
	seqno bigint;
	advanced_node int;
	replica shardman.replicas;
BEGIN
	-- Select most advanced replica: replica with largest seqno
	FOR replica IN SELECT * FROM shardman.replicas WHERE part_name=pname
	LOOP
		SELECT shardman.broadcast(format('%s:SELECT max(seqno) FROM %s_change_log;',
			replica.node_id, replica.part_name))::bigint INTO seqno;
		IF seqno > max_seqno
		THEN
		   max_seqno := seqno;
		   advanced_node := replica.node_id;
		END IF;
	END LOOP;

	-- Synchronize all lagging replicas
	FOR replica IN SELECT * FROM shardman.replicas WHERE part_name=pname AND node_id<>advanced_node
	LOOP
		SELECT shardman.broadcast(format('%s:SELECT max(seqno) FROM %s_change_log;',
			replica.node_id, replica.part_name))::bigint INTO seqno;
		IF seqno <> max_seqno
		THEN
		   RAISE NOTICE 'Advance node % from %', replica.node_id, advanced_node;
		   PERFORM shardman.remote_copy(replica.relation, pname, replica.node_id,
										advanced_node, seqno);
		END IF;
	END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Get relation primary key. There can be table with no primary key or with
-- compound primary key. But logical replication and hash partitioning in any
-- case requires single primary key.
CREATE FUNCTION get_primary_key(rel regclass, out pk_name text, out pk_type text) AS $$
	SELECT a.attname::text, a.atttypid::regtype::text FROM pg_index i
	JOIN   pg_attribute a ON a.attrelid = i.indrelid
                     AND a.attnum = ANY(i.indkey)
    WHERE  i.indrelid = rel
    AND    i.indisprimary;
$$ LANGUAGE sql;

-- Copy missing data from one node to another. This function uses change_log
-- table to determine records which need to be copied.
-- See explanations in synchronize_replicas.
-- Parameters:
--   rel_name:   name of parent relation
--   part_name:  synchronized partition name
--   dst_node:   lagging node
--   src_node:   advanced node
--   last_seqno: maximal seqno at lagging node
CREATE FUNCTION remote_copy(rel_name text, part_name text, dst_node int,
							src_node int, last_seqno bigint) RETURNS void AS $$
DECLARE
	script text;
	conn_string text;
	pk_name text;
	pk_type text;
BEGIN
	SELECT * FROM shardman.get_primary_key(rel_name) INTO pk_name, pk_type;
	ASSERT pk_name IS NOT NULL, 'Can''t sync replicas without primary key';
	SELECT connection_string INTO conn_string FROM shardman.nodes WHERE id=src_node;
	-- We need to execute all this three statements in one transaction to
	-- exclude inconsistencies in case of failure
	script := format('{%s:COPY %s_change_log FROM PROGRAM ''psql "%s" -c "COPY (SELECT * FROM %s_change_log WHERE seqno>%s) TO stdout"'';
		   	  		      DELETE FROM %I USING %s_change_log cl WHERE cl.seqno>%s AND cl.old_pk=%I;
						  COPY %I FROM PROGRAM ''psql "%s" -c "COPY (SELECT DISTINCT ON (%I) %I.* FROM %I,%s_change_log cl WHERE cl.seqno>%s AND cl.new_pk=%I ORDER BY %I) TO stdout"''}',
					dst_node, part_name, conn_string, part_name, last_seqno,
					part_name, part_name, last_seqno, pk_name,
					part_name, conn_string, pk_name, part_name, part_name, part_name, last_seqno, pk_name, pk_name);
	PERFORM shardman.broadcast(script);
END;
$$ LANGUAGE plpgsql;

-- Drop on_change triggers when replica is moved
CREATE FUNCTION drop_on_change_triggers(part_name text) RETURNS text AS $$
BEGIN
	return format('DROP FUNCTION on_%s_insert CASCADE;
		   		   DROP FUNCTION on_%s_update CASCADE;
				   DROP FUNCTION on_%s_delete CASCADE;',
				   part_name, part_name, part_name);
END;
$$ LANGUAGE plpgsql;

-- Generate triggers which maintain change_log table for replica
CREATE FUNCTION create_on_change_triggers(rel_name text, part_name text) RETURNS text AS $$
DECLARE
	pk_name text;
	pk_type text;
	change_log_limit int = 32*1024;
BEGIN
	SELECT * FROM shardman.get_primary_key(rel_name) INTO pk_name,pk_type;
	RETURN format($triggers$
		CREATE TABLE IF NOT EXISTS %s_change_log(seqno bigserial primary key, new_pk %s, old_pk %s);
		CREATE OR REPLACE FUNCTION on_%s_update() RETURNS TRIGGER AS $func$
		DECLARE
			last_seqno bigint;
		BEGIN
			INSERT INTO %s_change_log (new_pk, old_pk) values (NEW.%I, OLD.%I) RETURNING seqno INTO last_seqno;
			IF last_seqno %% %s = 0 THEN
			    DELETE FROM %s_change_log WHERE seqno < last_seqno - %s;
			END IF;
			RETURN NEW;
		END; $func$ LANGUAGE plpgsql;
		CREATE OR REPLACE FUNCTION on_%s_insert() RETURNS TRIGGER AS $func$
		DECLARE
			last_seqno bigint;
		BEGIN
			INSERT INTO %s_change_log (new_pk) values (NEW.%I) RETURNING seqno INTO last_seqno;
			IF last_seqno %% %s = 0 THEN
			    DELETE FROM %s_change_log WHERE seqno < last_seqno - %s;
			END IF;
			RETURN NEW;
		END; $func$ LANGUAGE plpgsql;
		CREATE OR REPLACE FUNCTION on_%s_delete() RETURNS TRIGGER AS $func$
		DECLARE
			last_seqno bigint;
		BEGIN
			INSERT INTO %s_change_log (old_pk) values (OLD.%I) RETURNING seqno INTO last_seqno;
			IF last_seqno %% %s = 0 THEN
			    DELETE FROM %s_change_log WHERE seqno < last_seqno - %s;
			END IF;
		END; $func$ LANGUAGE plpgsql;
		CREATE TRIGGER on_insert AFTER INSERT ON %I FOR EACH ROW EXECUTE PROCEDURE on_%s_insert();
		CREATE TRIGGER on_update AFTER UPDATE ON %I FOR EACH ROW EXECUTE PROCEDURE on_%s_update();
		CREATE TRIGGER on_delete AFTER DELETE ON %I FOR EACH ROW EXECUTE PROCEDURE on_%s_delete();
		ALTER TABLE %I ENABLE REPLICA TRIGGER on_insert, ENABLE REPLICA TRIGGER on_update, ENABLE REPLICA TRIGGER on_delete;$triggers$,
		part_name, pk_type, pk_type,
		part_name, part_name, pk_name, pk_name, change_log_limit*2, part_name, change_log_limit,
		part_name, part_name, pk_name, change_log_limit*2, part_name, change_log_limit,
		part_name, part_name, pk_name, change_log_limit*2, part_name, change_log_limit,
		part_name, part_name,
		part_name, part_name,
		part_name, part_name,
		part_name);
END;
$$ LANGUAGE plpgsql;

-- Generate change_log triggers for partitions with more than one replica at nodes where this replicas are located
CREATE FUNCTION generate_on_change_triggers(table_pattern text = '%') RETURNS void AS $$
DECLARE
	replica shardman.replicas;
	create_triggers text = '';
BEGIN
	FOR replica IN SELECT * FROM shardman.replicas WHERE relation LIKE table_pattern
	LOOP
		IF shardman.get_redundancy_of_partition(replica.part_name) > 1
		   AND shardman.not_exists(replica.node_id, format('pg_trigger t, pg_class c WHERE tgname=''on_insert'' AND t.tgrelid=c.oid AND c.relname=%L', replica.part_name))
		THEN
			create_triggers := format('%s{%s:%s}', create_triggers, replica.node_id, shardman.create_on_change_triggers(replica.relation, replica.part_name));
		END IF;
	END LOOP;

	-- Create triggers at all nodes
	PERFORM shardman.broadcast(create_triggers);
END;
$$ LANGUAGE plpgsql;


-- Returns PostgreSQL system identifier (written in control file)
CREATE FUNCTION get_system_identifier()
    RETURNS bigint AS 'pg_shardman' LANGUAGE C STRICT;

-----------------------------------------------------------------------
-- Some useful views.
-----------------------------------------------------------------------

-- Type to represent vertex in lock graph
create type process as (node int, pid int);

-- View to build lock graph which can be used to detect global deadlock.
-- Application_name is assumed pgfdw:$system_id:$coord_pid
-- gid is assumed $pid:$count:$sys_id:$xid:$participants_count
-- Currently we are oblivious about lock modes and report any wait -> hold edge
-- on the same object and therefore might produce false loops. Furthermore,
-- we have not idea about locking queues here. Probably it is better to use
-- pg_blocking_pids, but it seems to ignore prepared xacts.
CREATE VIEW lock_graph(wait, hold) AS
	-- local dependencies
    -- If xact is already prepared, we take node and pid of the coordinator.
	SELECT
		ROW(shardman.get_my_id(),
			wait.pid)::shardman.process,
	 	CASE WHEN hold.pid IS NOT NULL THEN
		    ROW(shardman.get_my_id(), hold.pid)::shardman.process
		ELSE -- prepared
			ROW(shardman.get_node_by_sysid(split_part(gid, ':', 3)::bigint),
				split_part(gid, ':', 1)::int)::shardman.process
		END
     FROM pg_locks wait, pg_locks hold LEFT OUTER JOIN pg_prepared_xacts twopc
			  ON twopc.transaction=hold.transactionid
	WHERE
		NOT wait.granted AND wait.pid IS NOT NULL AND hold.granted AND
		-- waiter waits for the the object holder locks
		wait.database IS NOT DISTINCT FROM hold.database AND
		wait.relation IS NOT DISTINCT FROM hold.relation AND
		wait.page IS NOT DISTINCT FROM hold.page AND
		wait.tuple IS NOT DISTINCT FROM hold.tuple AND
		wait.virtualxid IS NOT DISTINCT FROM hold.virtualxid AND
		wait.transactionid IS NOT DISTINCT FROM hold.transactionid AND -- waiting on xid
		wait.classid IS NOT DISTINCT FROM hold.classid AND
		wait.objid IS NOT DISTINCT FROM hold.objid AND
		wait.objsubid IS NOT DISTINCT FROM hold.objsubid AND
		 -- this is most probably truism, but who knows
		(hold.pid IS NOT NULL OR twopc.gid IS NOT NULL)
	UNION ALL
	-- if this fdw backend is busy, potentially waiting, add edge coordinator -> fdw
	SELECT ROW(shardman.get_node_by_sysid(split_part(application_name, ':', 2)::bigint),
			   split_part(application_name,':',3)::int)::shardman.process,
		   ROW(shardman.get_my_id(),
			   pid)::shardman.process
	FROM pg_stat_activity WHERE application_name LIKE 'pgfdw:%' AND wait_event<>'ClientRead'
	UNION ALL
	-- otherwise, coordinator itself is busy, potentially waiting, so add fdw ->
	-- coordinator edge
	SELECT ROW(shardman.get_my_id(),
			   pid)::shardman.process,
		   ROW(shardman.get_node_by_sysid(split_part(application_name,':',2)::bigint),
			   split_part(application_name,':',3)::int)::shardman.process
	FROM pg_stat_activity WHERE application_name LIKE 'pgfdw:%' AND wait_event='ClientRead';

-- Pack lock graph into comma-separated string of edges like "2:17439->4:30046",
-- i.e. pid 17439 on node 2 waits for pid 30046 on node 4
CREATE FUNCTION serialize_lock_graph() RETURNS TEXT AS $$
	SELECT COALESCE(
		string_agg((wait).node || ':' || (wait).pid || '->' ||
				   (hold).node || ':' || (hold).pid,
				   ','),
		'')
	FROM shardman.lock_graph;
$$ LANGUAGE sql;

-- Unpack lock graph from string
CREATE FUNCTION deserialize_lock_graph(edges text) RETURNS SETOF shardman.lock_graph AS $$
	SELECT ROW(split_part(split_part(edge, '->', 1), ':', 1)::int,
		       split_part(split_part(edge, '->', 1), ':', 2)::int)::shardman.process AS wait,
	       ROW(split_part(split_part(edge, '->', 2), ':', 1)::int,
		       split_part(split_part(edge, '->', 2), ':', 2)::int)::shardman.process AS hold
	FROM regexp_split_to_table(edges, ',') edge WHERE edge <> '';
$$ LANGUAGE sql;

-- Collect lock graphs from all nodes
CREATE FUNCTION global_lock_graph() RETURNS text AS $$
DECLARE
	node_id int;
	poll text = '';
	graph text;
BEGIN
	IF NOT shardman.is_shardlord()
	THEN
		RETURN shardman.broadcast('0:SELECT shardman.global_lock_graph();');
	END IF;

	FOR node_id IN SELECT id FROM shardman.nodes
	LOOP
		poll := format('%s%s:SELECT shardman.serialize_lock_graph();', poll, node_id);
	END LOOP;
	SELECT shardman.broadcast(poll, ignore_errors => true) INTO graph;

	RETURN graph;
END;
$$ LANGUAGE plpgsql;

-- Find all distributed deadlocks and for random one return path in the lock
-- graph containing deadlock loop.
-- We go from each vertex in all directions until either there is nowhere to go
-- to or loop is discovered. It means that for each n-vertex-loop n paths
-- starting at different vertices are actually found, though we return only one.
-- Note that it doesn't neccessary returns path contatins ONLY the loop:
-- non-empty inital tail is perfectly possible.
CREATE FUNCTION detect_deadlock(lock_graph text) RETURNS shardman.process[] AS $$
	WITH RECURSIVE LinkTable AS (SELECT wait AS Parent, hold AS Child FROM shardman.deserialize_lock_graph(lock_graph)),
	cte AS (
		SELECT Child, Parent, ARRAY[Child] AS AllParents, false AS Loop
  	  	FROM LinkTable
	  	UNION ALL
	  	SELECT c.Child, c.Parent, p.AllParents || c.Child, c.Child = ANY(p.AllParents)
	 	FROM LinkTable c JOIN cte p	ON c.Parent = p.Child AND NOT p.Loop
	)
	SELECT AllParents FROM cte WHERE Loop LIMIT 1;
$$ LANGUAGE sql;

-- Monitor cluster for presence of distributed deadlocks and node failures.
-- Tries to cancel queries causing deadlock and exclude unavailable nodes from
-- the cluster.
CREATE FUNCTION monitor(check_timeout_sec int = 5,
						rm_node_timeout_sec int = 60) RETURNS void AS $$
DECLARE
	prev_deadlock_path shardman.process[];
	deadlock_path shardman.process[];
	victim shardman.process;
	loop_begin int;
	loop_end int;
	prev_loop_begin int;
	prev_loop_end int;
	sep int;
	resp text;
	error_begin int;
	error_end int;
	error_msg text;
	error_node_id int;
	failed_node_id int := null;
	failure_timestamp timestamp with time zone;
BEGIN
	IF shardman.redirect_to_shardlord(format('monitor(%s, %s)', check_timeout_sec, rm_node_timeout_sec))
	THEN
		RETURN;
	END IF;

	RAISE NOTICE 'Start cluster monitoring...';

	LOOP
		resp := shardman.global_lock_graph();
		error_begin := position('<error>' IN resp);
		IF error_begin <> 0
		THEN
			error_end := position('</error>' IN resp);
			sep := position(':' IN resp);
			error_node_id := substring(resp FROM error_begin+7 FOR sep-error_begin-7)::int;
			error_msg := substring(resp FROM sep+1 FOR error_end-sep-1);
			IF error_node_id = failed_node_id and rm_node_timeout_sec IS NOT NULL
			THEN
				IF clock_timestamp() > failure_timestamp + rm_node_timeout_sec * interval '1 sec'
				THEN
					RAISE NOTICE 'Removing node % because of % sec timeout expiration', failed_node_id, rm_node_timeout_sec;
					PERFORM shardman.broadcast(format('0:SELECT shardman.rm_node(%s, force=>true);', failed_node_id));
					PERFORM shardman.broadcast('0:SELECT shardman.recover_xacts();');
					failed_node_id := null;
				END IF;
			ELSE
				RAISE NOTICE 'Node % reports error message %', error_node_id, error_msg;
				failed_node_id := error_node_id;
				failure_timestamp := clock_timestamp();
			END IF;
			prev_deadlock_path := null;
		ELSE
			failed_node_id := null;
			deadlock_path := shardman.detect_deadlock(resp);
			-- pick out the loop itself
			loop_end := array_upper(deadlock_path, 1);
			loop_begin := array_position(deadlock_path, deadlock_path[loop_end]);
			-- Check if old and new lock graph contain the same subgraph.
			-- Because we can not make consistent distributed snapshot,
			-- collected global local graph can contain "false" loops.
			-- So we report deadlock only if detected loop persists during
			-- deadlock detection period.
			-- We count upon that not only the loop, but the sequence of visited
			-- nodes is the same
			IF prev_deadlock_path IS NOT NULL
			   AND loop_end - loop_begin = prev_loop_end - prev_loop_begin
			   AND deadlock_path[loop_begin:loop_end] = prev_deadlock_path[prev_loop_begin:prev_loop_end]
			THEN
				-- Try to cancel random node in loop.
				-- If the victim is not executing active query at the moment,
				-- pg_cancel_backend can't do anything with xact; because of that,
				-- we probably need to repeat it several times
				victim := deadlock_path[loop_begin + ((loop_end - loop_begin)*random())::integer];
				RAISE NOTICE 'Detect deadlock: cancel process % at node %', victim.pid, victim.node;
				PERFORM shardman.broadcast(format('%s:SELECT pg_cancel_backend(%s);',
					victim.node, victim.pid));
			END IF;
			prev_deadlock_path := deadlock_path;
			prev_loop_begin := loop_begin;
			prev_loop_end := loop_end;
		END IF;
		PERFORM pg_sleep(check_timeout_sec);
	END LOOP;
END;
$$ LANGUAGE plpgsql;


-- View for monitoring logical replication lag.
-- Can be used only at shardlord.
CREATE VIEW replication_lag(pubnode, subnode, lag) AS
	SELECT src.id AS srcnode, dst.id AS dstnode,
		shardman.broadcast(format('%s:SELECT pg_current_wal_lsn() - confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name=''node_%s'';',
						   src.id, dst.id))::bigint AS lag
    FROM shardman.nodes src, shardman.nodes dst WHERE src.id<>dst.id;

-- Yet another view for replication state based on change_log table.
-- Can be used only at shardlord only only if redundancy level is greater than 1.
-- This functions is polling state of all nodes, if some node is offline, then it should
-- be explicitly excluded by filter condition, otherwise error will be reported.
CREATE VIEW replication_state(part_name, node_id, last_seqno) AS
	SELECT part_name,node_id,shardman.broadcast(format('%s:SELECT max(seqno) FROM %s_change_log;',node_id,part_name))::bigint FROM shardman.replicas;


-- Drop replication slot, if it exists.
-- About 'with_force' option: we can't just drop replication slots because
-- pg_drop_replication_slot will bail out with ERROR if connection is active.
-- Therefore the caller must either ensure that the connection is dead (e.g.
-- drop subscription on far end) or pass 'true' to 'with_force' option, which
-- does the following dirty hack. It kills several times active walsender with
-- short interval. After the first kill, replica will immediately try to
-- reconnect, so the connection resurrects instantly. However, if we kill it
-- second time, replica won't try to reconnect until wal_retrieve_retry_interval
-- after its first reaction passes, which is 5 secs by default. Of course, this
-- is not reliable and should be redesigned.
CREATE FUNCTION drop_repslot(slot_name text, with_force bool DEFAULT true)
	RETURNS void AS $$
DECLARE
	slot_exists bool;
	kill_ws_times int := 3;
BEGIN
	RAISE DEBUG '[SHMN] Dropping repslot %', slot_name;
	EXECUTE format('SELECT EXISTS (SELECT 1 FROM pg_replication_slots
				   WHERE slot_name = %L)', slot_name) INTO slot_exists;
	IF slot_exists THEN
		IF with_force THEN -- kill walsender several times
			RAISE DEBUG '[SHMN] Killing repslot % with fire', slot_name;
			FOR i IN 1..kill_ws_times LOOP
				RAISE DEBUG '[SHMN] Killing walsender for slot %', slot_name;
				PERFORM shardman.terminate_repslot_walsender(slot_name);
				IF i != kill_ws_times THEN
					PERFORM pg_sleep(0.05);
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

-- Drop sub unilaterally: If sub exists, disable it, detach repslot from it and
-- drop.
CREATE FUNCTION eliminate_sub(subname name)
	RETURNS void AS $$
DECLARE
	sub_exists bool;
BEGIN
	EXECUTE format('SELECT EXISTS (SELECT 1 FROM pg_subscription WHERE subname
				   = %L)', subname) INTO sub_exists;
	IF sub_exists THEN
		EXECUTE format('ALTER SUBSCRIPTION %I DISABLE', subname);
		EXECUTE format('ALTER SUBSCRIPTION %I SET (slot_name = NONE)', subname);
		EXECUTE format('DROP SUBSCRIPTION %I', subname);
	END IF;
END
$$ LANGUAGE plpgsql STRICT;


-- Remove all shardman state (LR stuff, foreign servers). If
-- drop_slots_with_force is true, we will kill walsenders before dropping LR
-- slots.
-- We reset synchronous_standby_names to empty string after commit,
-- -- this is non-transactional action and might be not performed.
CREATE OR REPLACE FUNCTION wipe_state(drop_slots_with_force bool DEFAULT true)
	RETURNS void AS $$
DECLARE
	srv record;
	pub record;
	sub record;
	rs record;
	ftable_name name;
BEGIN
	-- otherwise we might hang
	SET LOCAL synchronous_commit TO LOCAL;

	-- It is a bad idea to drop foreign tables which are partitions, because
	-- currently there is no way in pathman to attach partition again later and
	-- we won't be able to recover the node. We would better create dummy
	-- foreign server and bind ftables to it.
	CREATE SERVER IF NOT EXISTS shardman_dummy FOREIGN DATA WRAPPER postgres_fdw
		OPTIONS (hostaddr '192.0.2.42'); -- invalid ip
	FOR ftable_name IN SELECT ftrelid::regclass::name FROM
		pg_foreign_table ft, pg_foreign_server fs WHERE srvname LIKE 'node_%' AND
		ft.ftserver = fs.oid
	LOOP
		PERFORM shardman.alter_ftable_set_server(ftable_name, 'shardman_dummy',
												 server_not_needed => true);
	END LOOP;

	FOR srv IN SELECT srvname FROM pg_foreign_server WHERE srvname LIKE 'node_%' LOOP
		EXECUTE format('DROP SERVER %I CASCADE', srv.srvname);
	END LOOP;

	FOR pub IN SELECT pubname FROM pg_publication WHERE pubname LIKE 'node_%' LOOP
		EXECUTE format('DROP PUBLICATION %I', pub.pubname);
	END LOOP;
	FOR sub IN SELECT subname FROM pg_subscription WHERE subname LIKE 'share_%' LOOP
		PERFORM shardman.eliminate_sub(sub.subname);
	END LOOP;
	FOR sub IN SELECT subname FROM pg_subscription WHERE subname LIKE 'sub_%' LOOP
		PERFORM shardman.eliminate_sub(sub.subname);
	END LOOP;
	FOR rs IN SELECT slot_name FROM pg_replication_slots
		WHERE slot_name LIKE 'node_%' AND slot_type = 'logical' LOOP
		PERFORM shardman.drop_repslot(rs.slot_name, drop_slots_with_force);
	END LOOP;
		-- TODO: remove only shardman's standbys
	PERFORM shardman.reset_synchronous_standby_names_on_commit();
END;
$$ LANGUAGE plpgsql;
CREATE FUNCTION reset_synchronous_standby_names_on_commit()
	RETURNS void AS 'pg_shardman' LANGUAGE C STRICT;

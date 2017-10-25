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

-- Shardman tables

-- List of nodes present in the cluster
CREATE TABLE nodes (
	id serial PRIMARY KEY,
	super_connection_string text UNIQUE NOT NULL,
	connection_string text UNIQUE NOT NULL,
	replication_group text NOT NULL -- group of nodes within which shard replicas are allocated
);

-- List of sharded tables
CREATE TABLE tables (
	relation text PRIMARY KEY,     -- table name
	sharding_key text NOT NULL,    -- expression by which table is sharded
	partitions_count int NOT NULL, -- number of partitions
	create_sql text NOT NULL       -- sql to create the table
);

-- Main partitions
CREATE TABLE partitions (
	part_name text PRIMARY KEY,
	node_id int NOT NULL REFERENCES nodes(id) ON DELETE CASCADE, -- node on which partition lies
	relation text NOT NULL
);

-- Partition replicas
CREATE TABLE replicas (
	part_name text NOT NULL REFERENCES partitions(part_name) ON DELETE CASCADE,
	node_id int NOT NULL REFERENCES nodes(id) ON DELETE CASCADE, -- node on which partition lies
	relation text NOT NULL,
	PRIMARY KEY (part_name,node_id)
);

-- Shardman interface functions

-- Add a node: adjust logical replication channels in replication group and
-- create foreign servers.
-- 'super_conn_string' is connection string to the node which allows to login to
-- the node as superuser, and 'conn_string' can be some other connstring.
-- The former is used for configuring logical replication, the latter for DDL
-- and for setting up FDW. This separation serves two purposes:
-- * It allows to access data without requiring superuser priviliges;
-- * It allows to set up pgbouncer, as replication can't go through it.
-- If conn_string is null, super_conn_string is used everywhere.
CREATE FUNCTION add_node(super_conn_string text, conn_string text = NULL,
						 repl_group text = 'all') RETURNS void AS $$
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
	replace_parts text = '';
	fdw_part_name text;
	table_attrs text;
	srv_name text;
	conn_string_effective text = COALESCE(conn_string, super_conn_string);
BEGIN
	IF shardman.redirect_to_shardlord(
		format('add_node(%L, %L, %L)', super_conn_string, conn_string, repl_group))
	THEN
		RETURN;
	END IF;

	-- Insert new node in nodes table
	INSERT INTO shardman.nodes (super_connection_string, connection_string, replication_group)
	VALUES (super_conn_string, conn_string_effective, repl_group)
		   RETURNING id INTO new_node_id;

	-- Adjust replication channels within replication group.
	-- We need all-to-all replication channels between all group members.
	FOR node IN SELECT * FROM shardman.nodes WHERE replication_group = repl_group AND id  <> new_node_id
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

	-- In case of synchronous replication broadcast update synchronous standby list commands
	IF shardman.synchronous_replication() AND
		(SELECT COUNT(*) FROM shardman.nodes WHERE replication_group = repl_group) > 1
	THEN
		-- Take all nodes in replicationg group excluding myself
		FOR node IN SELECT * FROM shardman.nodes WHERE replication_group = repl_group LOOP
			-- Construct list of synchronous standbyes=subscriptions to this node
			sync_standbys :=
				 coalesce(ARRAY(SELECT format('sub_%s_%s', id, node.id) FROM shardman.nodes
				   WHERE replication_group = repl_group AND id <> node.id), '{}'::text[]);
			sync := format('%s%s:ALTER SYSTEM SET synchronous_standby_names to ''FIRST %s (%s)'';',
				 sync, node.id, array_length(sync_standbys, 1),
				 array_to_string(sync_standbys, ','));
			conf := format('%s%s:SELECT pg_reload_conf();', conf, node.id);
		END LOOP;

	    PERFORM shardman.broadcast(sync, sync_commit_on => true, super_connstr => true);
	    PERFORM shardman.broadcast(conf, super_connstr => true);
	END IF;

	-- Add foreign servers for connection to the new node and backward
	-- Construct foreign server options from connection string of new node
	SELECT * FROM shardman.conninfo_to_postgres_fdw_opts(conn_string) INTO new_server_opts, new_um_opts;
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

	-- Create FDWs at new node for all existed partitions
	FOR t IN SELECT * from shardman.tables
	LOOP
		create_tables := format('%s{%s:%s}',
			create_tables, new_node_id, t.create_sql);
		create_partitions := format('%s%s:select create_hash_partitions(%L,%L,%L);',
			create_partitions, new_node_id, t.relation, t.sharding_key, t.partitions_count);
		SELECT shardman.reconstruct_table_attrs(t.relation) INTO table_attrs;
		FOR part IN SELECT * from shardman.partitions WHERE relation=t.relation
	    LOOP
			SELECT connection_string INTO conn_string from shardman.nodes WHERE id=part.node_id;
		    SELECT * FROM shardman.conninfo_to_postgres_fdw_opts(conn_str) INTO server_opts, um_opts;
			srv_name := format('node_%s', part.node_id);
			fdw_part_name := format('%s_fdw', part.part_name);
			create_fdws := format('%s%s:CREATE FOREIGN TABLE %I %s SERVER %s OPTIONS (table_name %L);',
				create_fdws, new_node_id, fdw_part_name, table_attrs, srv_name, part.part_name);
			replace_parts := format('%s%s:SELECT replace_hash_partition(%L, %L);',
				replace_parts, new_node_id, part.part_name, fdw_part_name);
		END LOOP;
	END LOOP;

	-- Broadcast create table commands
	PERFORM shardman.broadcast(create_tables);
	-- Broadcast create hash partitions command
	PERFORM shardman.broadcast(create_partitions);
	-- Broadcast create foreign table commands
	PERFORM shardman.broadcast(create_fdws);
	-- Broadcast replace hash partition commands
	PERFORM shardman.broadcast(replace_parts);
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
	fdws text = '';
	sync text = '';
	conf text = '';
	fdw_part_name text;
    new_master_id int;
	sync_standbys text[];
	repl_group text;
BEGIN
	IF shardman.redirect_to_shardlord(format('rm_node(%L, %L)', rm_node_id, force))
	THEN
		RETURN;
	END IF;

	IF NOT EXISTS(SELECT * from shardman.nodes WHERE id=rm_node_id)
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

	-- Remove all subscriptions and publications of this node
    FOR node IN SELECT * FROM shardman.nodes WHERE replication_group=repl_group AND id<>rm_node_id
	LOOP
		pubs := format('%s%s:DROP PUBLICATION node_%s;
			 			  %s:DROP PUBLICATION node_%s;',
			 pubs, node.id, rm_node_id,
			 	   rm_node_id, node.id);
		subs := format('%s%s:DROP SUBSCRIPTION sub_%s_%s;
			 			  %s:DROP SUBSCRIPTION sub_%s_%s',
			 subs, rm_node_id, rm_node_id, node.id,
			 node.id, node.id, rm_node_id);

		-- Construct new synchronous standby list
		sync_standbys :=
			coalesce(ARRAY(SELECT format('sub_%s_%s', id, node.id) FROM shardman.nodes
							WHERE replication_group = repl_group AND id <> node.id AND
								  id<>rm_node_id),
								  '{}'::text[]);
		sync := format('%s%s:ALTER SYSTEM SET synchronous_standby_names to ''FIRST %s (%s)'';',
					   sync, node.id, array_length(sync_standbys, 1),
					   array_to_string(sync_standbys, ','));
		conf := format('%s%s:SELECT pg_reload_conf();', conf, node.id);
	END LOOP;

	-- Broadcast drop subscription commands, ignore errors because removed node may be not available
	PERFORM shardman.broadcast(subs, ignore_errors:=true, sync_commit_on => true,
							   super_connst => true);
	-- Broadcast drop replication commands
    PERFORM shardman.broadcast(pubs, ignore_errors:=true, super_connstr => true);

	-- In case of synchronous replication update synchronous standbys list
	IF shardman.synchronous_replication()
	THEN
		-- On removed node, reset synchronous standbys list
		sync := format('%s%s:ALTER SYSTEM SET synchronous_standby_names to '''';',
			 sync, rm_node_id, sync_standbys);
	    PERFORM shardman.broadcast(sync, ignore_errors => true,
								   sync_commit_on => true, super_connstr => true);
	    PERFORM shardman.broadcast(conf, ignore_errors:=true, super_connstr => true);
	END IF;

	-- Remove foreign servers at all nodes for the removed node
    FOR node IN SELECT * FROM shardman.nodes WHERE id<>rm_node_id
	LOOP
		-- Drop server for all nodes at the removed node and drop server at all nodes for the removed node
		fdws := format('%s%s:DROP SERVER node_%s;
			 			  %s:DROP SERVER node_%s;',
			 fdws, node.id, rm_node_id,
			 	   rm_node_id, node.id);
	END LOOP;

	-- Broadcast drop server commands
    PERFORM shardman.broadcast(fdws, ignore_errors:=true);
	fdws := '';

	-- Exclude partitions of removed node
	FOR part in SELECT * from shardman.partitions where node=rm_node_id
	LOOP
		-- Is there some replica of this node?
		SELECT node_id INTO new_master_id FROM shardman.replicas WHERE part_name=part.part_name ORDER BY random() LIMIT 1;
		IF new_master_id IS NOT NULL
		THEN -- exists some replica for this node: redirect foreign table to this replica and refresh LR channels for this replication group
			-- Update partitions table: now replica is promoted to master...
		    UPDATE shardman.partitions SET node_id=new_master_id WHERE part_name=part.part_name;
			-- ... and is not a replica any more
			DELETE FROM sharaman.replicas WHERE part_name=part.part_name AND node_id=new_master_id;

			pubs := '';
			subs := '';
			-- Refresh LR channels for this replication group
			FOR repl in SELECT * FROM shardman.replicas WHERE part_name=part.part_name
			LOOP
				-- Publish this partition at new master
			    pubs := format('%s%s:ALTER PUBLICATION node_%s ADD TABLE %I;',
				     pubs, new_master_id, repl.node_id, part.part_name);
				-- And refresh subscriptions and replicas
				subs := format('%s%s:ALTER SUBSCRIPTION sub_%s_%s REFRESH PUBLICATION WITH (copy_data=false);',
					 subs, repl.node_id, repl.node_id, new_master_id);
			END LOOP;

			-- Broadcast alter publication commands
			PERFORM shardman.broadcast(pubs, super_connstr => true);
			-- Broadcast refresh alter subscription commands
			PERFORM shardman.broadcast(subs, super_connstr => true);
		ELSE -- there is no replica: we have to create new empty partition at random mode and redirect all FDWs to it
			SELECT id INTO new_master_id FROM shardman.nodes WHERE id<>rm_node_id ORDER BY random() LIMIT 1;
		    INSERT INTO shardman.partitions (part_name,node_id,relation) VALUES (part.part.name,new_master_id,part.relation);
		END IF;

		-- Update pathman partition map at all nodes
		FOR node IN SELECT * FROM shardman.nodes WHERE id<>rm_node_id
		LOOP
			fdw_part_name := format('%s_fdw', part.part_name);
			IF node.id=new_master_id THEN
			    -- At new master node replace foreign link with local partition
			    fdws := format('%s%d:SELECT replace_hash_partition(%L,%L);',
			 		fdws, node.id, fdw_part_name, part.part_name);
			ELSE
				-- At all other nodes adjust foreign server for foreign table to refer to new master node.
				-- It is not possible to alter foreign server for foreign table so we have to do it in such "hackers" way:
				fdws := format('%s%s:UPDATE pg_foreign_table SET ftserver = (SELECT oid FROM pg_foreign_server WHERE srvname = ''node_%s'') WHERE ftrelid = (SELECT oid FROM pg_class WHERE relname=%L);',
		   			fdws, node.id, new_master_id, fdw_part_name);
			END IF;
		END LOOP;
	END LOOP;

	-- Broadcast changes of pathman mapping
	PERFORM shardman.broadcast(fdws, ignore_errors:=true);

	-- Finally delete node from nodes table and all dependent tables
	DELETE from shardman.nodes WHERE id=rm_node_id;
END
$$ LANGUAGE plpgsql;



-- Shard table with hash partitions. Parameters are the same as in pathman.
-- It also scatter partitions through all nodes.
-- This function expects that empty table is created at shardlord.
-- So it can be executed only at shardlord and there is no need to redirect this function to shardlord.
CREATE FUNCTION create_hash_partitions(rel regclass, expr text, part_count int, redundancy int = 0)
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
	rel_name text = rel::text;
	i int;
	n_nodes int;
BEGIN
	IF EXISTS(SELECT relation FROM shardman.tables WHERE relation = rel_name)
	THEN
		RAISE EXCEPTION 'Table % is already sharded', rel_name;
	END IF;
	SELECT shardman.gen_create_table_sql(rel_name) INTO create_table;

	INSERT INTO shardman.tables (relation,sharding_key,partitions_count,create_sql) values (rel_name,expr,part_count,create_table);

	-- Create parent table and partitions at all nodes
	FOR node IN SELECT * FROM shardman.nodes
	LOOP
		-- Create parent table at all nodes
		create_tables := format('%s{%s:%s}',
			create_tables, node.id, create_table);
		-- Create partitions using pathman at all nodes
		create_partitions := format('%s%s:select create_hash_partitions(%L,%L,%L);',
			create_partitions, node.id, rel_name, expr, part_count);
	END LOOP;

	-- Broadcast create table commands
	PERFORM shardman.broadcast(create_tables);
	-- Broadcast create hash partitions command
	PERFORM shardman.broadcast(create_partitions);

	-- Get list of nodes in random order
	SELECT ARRAY(SELECT id from shardman.nodes ORDER BY random()) INTO node_ids;
	n_nodes := array_length(node_ids, 1);

	-- Reconstruct table attributes from parent table
	SELECT shardman.reconstruct_table_attrs(rel_name) INTO table_attrs;

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
		FOR node IN SELECT * from shardman.nodes WHERE id<>node_id
		LOOP
			-- Create foreign table for this partition
			create_fdws := format('%s%s:CREATE FOREIGN TABLE %I %s SERVER %s OPTIONS (table_name %L);',
				create_fdws, node.id, fdw_part_name, table_attrs, srv_name, part_name);
			replace_parts := format('%s%s:SELECT replace_hash_partition(%L, %L);',
				replace_parts, node.id, part_name, fdw_part_name);
		END LOOP;
	END LOOP;

	-- Broadcast create foreign table commands
	PERFORM shardman.broadcast(create_fdws);
	-- Broadcast replace hash partition commands
	PERFORM shardman.broadcast(replace_parts);

	IF redundancy <> 0
	THEN
		PERFORM shardman.set_redundancy(rel, redundancy, copy_data => false);
	END IF;
END
$$ LANGUAGE plpgsql;

-- Provide requested level of redundancy. 0 means no redundancy.
-- If existing level of redundancy is greater than specified, then right now this
-- function does nothing.
CREATE FUNCTION set_redundancy(rel regclass, redundancy int, copy_data bool = true)
RETURNS void AS $$
DECLARE
	part shardman.partitions;
	n_replicas int;
	repl_node int;
	repl_group text;
	pubs text = '';
	subs text = '';
	rel_name text = rel::text;
	sub_options text = '';
BEGIN
	IF shardman.redirect_to_shardlord(format('set_redundancy(%L, %L)', rel_name, redundancy))
	THEN
		RETURN;
	END IF;

	IF NOT copy_data THEN
	    sub_options := ' WITH (copy_data=false)';
	END IF;

	-- Loop through all partitions of this table
	FOR part IN SELECT * from shardman.partitions where relation=rel_name
	LOOP
		-- Count number of replicas of this partition
		SELECT count(*) INTO n_replicas FROM shardman.replicas WHERE part_name=part.part_name;
		IF n_replicas < redundancy
		THEN -- If it is smaller than requested...
			SELECT replication_group INTO repl_group FROM shardman.nodes where id=part.node_id;
			-- ...then add requested number of replicas in corresponding replication group
			FOR repl_node IN SELECT id FROM shardman.nodes
				WHERE replication_group=repl_group AND id<>part.node_id AND NOT EXISTS
					(SELECT * FROM shardman.replicas WHERE node_id=id AND part_name=part.part_name)
				ORDER by random() LIMIT redundancy-n_replicas
			LOOP
				-- Insert information about new replica in replicas table
				INSERT INTO shardman.replicas (part_name, node_id, relation) VALUES (part.part_name, repl_node, rel_name);
				-- Establish publications and subscriptions for this partition
				pubs := format('%s%s:ALTER PUBLICATION node_%s ADD TABLE %I;',
					 pubs, part.node_id, repl_node, part.part_name);
				subs := format('%s%s:ALTER SUBSCRIPTION sub_%s_%s REFRESH PUBLICATION%s;',
					 subs, repl_node, repl_node, part.node_id, sub_options);
			END LOOP;
		END IF;
	END LOOP;

	-- Broadcast alter publication commands
	PERFORM shardman.broadcast(pubs, super_connstr => true);
	-- Broadcast alter subscription commands
	PERFORM shardman.broadcast(subs, synchronous => copy_data, super_connstr => true);

	-- This function doesn't wait completion of replication sync
END
$$ LANGUAGE plpgsql;

-- Remove table from all nodes. All table partitions are removed, but replicas
-- and logical stuff not.
CREATE FUNCTION rm_table(rel regclass)
RETURNS void AS $$
DECLARE
	rel_name text = rel::text;
	node shardman.nodes;
	drops text = '';
BEGIN
	IF shardman.redirect_to_shardlord(format('rm_table(%L)', rel_name))
	THEN
		RETURN;
	END IF;

	-- Drop table at all nodes
	FOR node IN SELECT * FROM shardman.nodes
	LOOP
		drops := format('%s%s:DROP TABLE %I CASCADE;',
			drops, node.id, rel_name);
	END LOOP;

	-- Broadcast drop table commands
	PERFORM shardman.broadcast(drops);
END
$$ LANGUAGE plpgsql;

-- Move partition to other node. This function is able to move partition only within replication group.
-- It creates temporary logical replication channel to copy partition to new location.
-- Until logical replication almost caught-up access to old partition is now denied.
-- Then we revoke all access to this table until copy is completed and all FDWs are updated.
CREATE FUNCTION mv_partition(mv_part_name text, dst_node_id int)
RETURNS void AS $$
DECLARE
	node shardman.nodes;
	src_repl_group text;
	dst_repl_group text;
	conn_string text;
	part shardman.partitions;
	create_fdws text = '';
	replace_parts text = '';
	drop_fdws text = '';
	fdw_part_name text = format('%s_fdw', mv_part_name);
	table_attrs text;
	srv_name text = format('node_%s', dst_node_id);
	pubs text = '';
	subs text = '';
	src_node_id int;
	repl_node_id int;
	drop_slots text = '';
BEGIN
	IF shardman.redirect_to_shardlord(format('mv_partition(%L, %L)', mv_part_name, dst_node_id))
	THEN
		RETURN;
	END IF;

	-- Check if there is partition with specified name
	SELECT * INTO part FROM shardman.partitions WHERE part_name = mv_part_name;
	IF part IS NULL THEN
	    RAISE EXCEPTION 'Partition % does not exist', mv_part_name;
	END IF;
	src_node_id := part.node_id;

	SELECT replication_group, super_connection_string INTO src_repl_group, conn_string FROM shardman.nodes WHERE id=src_node_id;
	SELECT replication_group INTO dst_repl_group FROM shardman.nodes WHERE id=dst_node_id;

	IF src_node_id = dst_node_id THEN
	    -- Nothing to do: partition is already here
		RAISE NOTICE 'Partition % is already located at node %',mv_part_name,dst_node_id;
		RETURN;
	END IF;

	-- Check if destination belong to the same replication group as source
	IF dst_repl_group<>src_repl_group AND shardman.get_redundancy_of_partition(mv_part_name)>0
	THEN
	    RAISE EXCEPTION 'Can not move partition % to different replication group', mv_part_name;
	END IF;

	IF EXISTS(SELECT * FROM shardman.replicas WHERE part_name=mv_part_name AND node_id=dst_node_id)
	THEN
	    RAISE EXCEPTION 'Can not move partition % to node % with existed replica', mv_part_name, dst_node_id;
	END IF;

	-- Copy partition data to new location
	pubs := format('%s:CREATE PUBLICATION copy_%s FOR TABLE %I;
		 			%s:SELECT pg_create_logical_replication_slot(''copy_%s'', ''pgoutput'');',
		 src_node_id, mv_part_name, mv_part_name,
		 src_node_id, mv_part_name);
	subs := format('%s:CREATE SUBSCRIPTION copy_%s CONNECTION %L PUBLICATION copy_%s with (create_slot=false, slot_name=''copy_%s'', synchronous_commit=local);',
		 dst_node_id, mv_part_name, conn_string, mv_part_name, mv_part_name);

	-- Create publication and slot for copying
	PERFORM shardman.broadcast(pubs, super_connstr => true);
	PERFORM shardman.broadcast(subs, super_connstr => true);

	-- Wait completion of partition copy and prohibit access to this partition
	PERFORM shardman.wait_copy_completion(src_node_id, mv_part_name);

	RAISE NOTICE 'Copy of partition % from node % to % is completed',
		 mv_part_name, src_node_id, dst_node_id;

	pubs := format('%s:DROP PUBLICATION copy_%s;',
		 src_node_id, mv_part_name);
	subs := '';

	-- Update replication channels
	FOR repl_node_id IN SELECT node_id from shardman.replicas WHERE part_name=mv_part_name
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
	-- Drop copy subscription
	PERFORM shardman.broadcast(format('%s:DROP SUBSCRIPTION copy_%s;',
		 dst_node_id, mv_part_name), sync_commit_on => true, super_connstr => true);

    -- Update owner of this partition
	UPDATE shardman.partitions SET node_id=dst_node_id WHERE part_name=mv_part_name;

	-- Update FDWs at all nodes
	FOR node IN SELECT * FROM shardman.nodes
	LOOP
		IF node.id = src_node_id
		THEN
			SELECT shardman.reconstruct_table_attrs(part.relation) INTO table_attrs;
			create_fdws := format('%s%s:CREATE FOREIGN TABLE %I %s SERVER %s OPTIONS (table_name %L);',
				create_fdws, node.id, fdw_part_name, table_attrs, srv_name, mv_part_name);
			replace_parts := format('%s%s:SELECT replace_hash_partition(%L, %L);',
				replace_parts, node.id, mv_part_name, fdw_part_name);
		ELSIF node.id = dst_node_id THEN
			replace_parts := format('%s%s:SELECT replace_hash_partition(%L, %L);',
				replace_parts, node.id, fdw_part_name, mv_part_name);
			drop_fdws := format('%s%s:DROP FOREIGN TABLE %I;',
				drop_fdws, node.id, fdw_part_name);
		ELSE
			replace_parts := format('%s%s:UPDATE pg_foreign_table SET ftserver = (SELECT oid FROM pg_foreign_server WHERE srvname = ''node_%s'') WHERE ftrelid = (SELECT oid FROM pg_class WHERE relname=%L);',
		   		replace_parts, node.id, dst_node_id, fdw_part_name);
		END IF;
	END LOOP;

	-- Broadcast create foreign table commands
	PERFORM shardman.broadcast(create_fdws);
	-- Broadcast replace hash partition commands
	PERFORM shardman.broadcast(replace_parts);
	-- Broadcast drop foreign table commands
	PERFORM shardman.broadcast(drop_fdws);

	-- Truncate partition table and restore access to it at source node
	PERFORM shardman.complete_partition_move(src_node_id, mv_part_name);
END
$$ LANGUAGE plpgsql;

-- Get redundancy of the particular partition
CREATE FUNCTION get_redundancy_of_partition(pname text) returns bigint AS $$
	SELECT count(*) FROM shardman.replicas where part_name=pname;
$$ LANGUAGE sql;

-- Get minimal redundancy of the specified relation
CREATE FUNCTION get_min_redundancy(rel regclass) returns bigint AS $$
	SELECT min(redundancy) FROM (SELECT count(*) redundancy FROM shardman.replicas WHERE relation=rel::text GROUP BY part_name) s;
$$ LANGUAGE sql;

---------------------------------------------------------------------
-- Utility functions
---------------------------------------------------------------------

-- Execute command at shardlord
CREATE FUNCTION redirect_to_shardlord(cmd text) RETURNS bool AS $$
DECLARE
	am_shardlord bool;
BEGIN
	SELECT setting::bool INTO am_shardlord FROM pg_settings WHERE name = 'shardman.shardlord';
	IF NOT am_shardlord THEN
		PERFORM shardman.broadcast(format('0:SELECT %s;', cmd));
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
-- cmds is list of SQL commands separated by semi-columns with node
-- prefix: node-id:sql-statement;
-- To run multiple statements on node, wrap them in {}:
-- {node-id:statement; statement;}
-- Node id '0' means shardlord, shardlord_connstring guc is used.
-- Don't specify them separately with 2pc, we use only one prepared_xact name.
-- No escaping is performed, so ';', '{' and '}' inside queries are not supported.
-- By default functions throws error is execution is failed at some of the
-- nodes, with ignore_errors=true errors are ignored and function returns string
-- with "Error:" prefix containing list of errors separated by semicolons with
-- nodes prefixes.
-- In case of normal completion this function return list with node prefixes
-- separated by semi-columns with single result for select queries or number of
-- affected rows for other commands.
-- If two_phase parameter is true, then each statement is wrapped in blocked and
-- prepared with subsequent commit or rollback of prepared transaction at second
-- phase of two phase commit.
-- If sync_commit_on is false, we set session synchronous_commit to local.
-- If super_connstr is true, super connstring is used everywhere, usual
-- connstr otherwise.
CREATE FUNCTION broadcast(cmds text,
						  ignore_errors bool = false,
						  two_phase bool = false,
						  sync_commit_on bool = false,
						  synchronous bool = false,
						  super_connstr bool = false)
RETURNS text AS 'pg_shardman' LANGUAGE C STRICT;

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
	FOR i IN 1..(SELECT array_upper(conn_string_keywords, 1)) LOOP
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

-- Parse connection string. This function is used by conninfo_to_postgres_fdw_opts to construct postgres_fdw options list.
CREATE FUNCTION pq_conninfo_parse(IN conninfo text, OUT keys text[], OUT vals text[])
	RETURNS record AS 'pg_shardman' LANGUAGE C STRICT;

-- Get shardlord connection string from configuration parameters
CREATE FUNCTION shardlord_connection_string()
	RETURNS text AS 'pg_shardman' LANGUAGE C STRICT;

-- Check from configuration parameters is synchronous replication mode was enabled
CREATE FUNCTION synchronous_replication()
	RETURNS bool AS 'pg_shardman' LANGUAGE C STRICT;

-- Wait completion of partition copy using LR
CREATE FUNCTION wait_copy_completion(src_node_id int, part_name text) RETURNS void AS $$
DECLARE
	slot text = format('copy_%s', part_name);
	lag bigint;
	response text;
	caughtup_threshold bigint = 1024*1024;
	timeout_sec int = 1;
    locked bool = false;
BEGIN
	LOOP
		response := shardman.broadcast(format('%s:SELECT confirmed_flush_lsn - pg_current_wal_lsn() FROM pg_replication_slots WHERE slot_name=%L;', src_node_id, slot));
		lag := trim(trailing ';' from response)::bigint;

		RAISE NOTICE 'Replication lag %', lag;
		IF locked THEN
		    IF lag<=0 THEN
			    RETURN;
			END IF;
		ELSIF lag < caughtup_threshold THEN
	   	    PERFORM shardman.broadcast(format('%s:REVOKE SELECT,INSERT,UPDATE,DELETE ON %I FROM PUBLIC;',
				src_node_id, part_name));
			locked := true;
			CONTINUE;
		END IF;
		PERFORM pg_sleep(timeout_sec);
	END LOOP;
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION complete_partition_move(src_node_id int, part_name text) RETURNS void AS $$
BEGIN
	PERFORM shardman.broadcast(format('%s:TRUNCATE TABLE %I;',
		src_node_id, part_name));
	PERFORM shardman.broadcast(format('%s:GRANT SELECT,INSERT,UPDATE,DELETE ON %I TO PUBLIC;',
		src_node_id, part_name));
END
$$ LANGUAGE plpgsql;

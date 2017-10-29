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
	sharding_key text,             -- expression by which table is sharded
	master_node integer REFERENCES nodes(id) ON DELETE CASCADE,
	partitions_count int,          -- number of partitions
	create_sql text NOT NULL,      -- sql to create the table
	create_rules_sql text          -- sql to create rules for shared table
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

-- View for monitoring replication lag
CREATE VIEW replication_lag(pubnode, subnode, lag) AS
	SELECT src.id AS srcnode, dst.id AS dstnode,
		shardman.broadcast(format('%s:SELECT pg_current_wal_lsn() - confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name=''node_%s'';',
						   src.id, dst.id))::bigint AS lag
	FROM shardman.nodes src, shardman.nodes dst WHERE src.id<>dst.id;

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
						 repl_group text = 'default') RETURNS void AS $$
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
		-- Take all nodes in replication group excluding myself
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

	-- Create FDWs at new node for all existed partitions
	FOR t IN SELECT * from shardman.tables WHERE sharding_key IS NOT NULL
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
	PERFORM shardman.broadcast(create_partitions);
	-- Broadcast create foreign table commands
	PERFORM shardman.broadcast(create_fdws);
	-- Broadcast replace hash partition commands
	PERFORM shardman.broadcast(replace_parts);
	-- Broadcast create rules for shared tables
	PERFORM shardman.broadcast(create_rules);
	-- Broadcast create subscriptions for shared tables
	PERFORM shardman.broadcast(subs, super_connstr => true);
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
	prts text = '';
	sync text = '';
	conf text = '';
	alts text = '';
	fdw_part_name text;
    new_master_id int;
	sync_standbys text[];
	repl_group text;
	master_node_id int;
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
		-- Drop publication and subscriptions for replicas
		pubs := format('%s%s:DROP PUBLICATION node_%s;
			 			  %s:DROP PUBLICATION node_%s;
						  %s:SELECT pg_drop_replication_slot(''node_%s'');
						  %s:SELECT pg_drop_replication_slot(''node_%s'');',
			 pubs, node.id, rm_node_id,
			 	   rm_node_id, node.id,
				   node.id, rm_node_id,
				   rm_node_id, node.id);
		-- Subscription with associated slot can not be dropped inside block, but if we do not override synchronous_commit policy,
		-- then this command will be blocked waiting for sync replicas. So we need first do unbound slot from subscription.
		-- But it is possible only for disabled subscriptions. So we have to perform three steps: disable subscription, unbound slot, drop subscription.
		alts := format('%s{%s:ALTER SUBSCRIPTION sub_%s_%s DISABLE;ALTER SUBSCRIPTION sub_%s_%s SET (slot_name=NONE)}{%s:ALTER SUBSCRIPTION sub_%s_%s DISABLE;ALTER SUBSCRIPTION sub_%s_%s SET (slot_name=NONE)}',
			 alts, rm_node_id, rm_node_id, node.id, rm_node_id, node.id,
			       node.id, node.id, rm_node_id, node.id, rm_node_id);
		subs := format('%s%s:DROP SUBSCRIPTION sub_%s_%s;
			 			  %s:DROP SUBSCRIPTION sub_%s_%s;',
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

	-- Drop shared tables subscriptions
	FOR master_node_id IN SELECT DISTINCT master_node from shardman.tables WHERE master_node IS NOT NULL
	LOOP
		alts := format('%s{%s:ALTER SUBSCRIPTION share_%s_%s DISABLE;ALTER SUBSCRIPTION share_%s_%s SET (slot_name=NONE)}',
			 alts, rm_node_id, rm_node_id, master_node_id, rm_node_id, master_node_id);
		subs := format('%s%s:DROP SUBSCRIPTION share_%s_%s;',
			 subs, rm_node_id, rm_node_id, master_node_id);
		pubs := format('%s%s:SELECT pg_drop_replication_slot(''share_%s_%s'');',
			 pubs, master_node_id, rm_node_id, master_node_id);
	END LOOP;

	-- Broadcast alter subscription commands, ignore errors because removed node may be not available
	PERFORM shardman.broadcast(alts,
							   ignore_errors => true,
							   super_connstr => true);
	-- Broadcast drop subscription commands, ignore errors because removed node may be not available
	PERFORM shardman.broadcast(subs,
							   ignore_errors => true,
							   super_connstr => true);

    -- Broadcast drop replication commands
    PERFORM shardman.broadcast(pubs, ignore_errors => true, super_connstr => true);

	-- In case of synchronous replication update synchronous standbys list
	IF shardman.synchronous_replication()
	THEN
		-- On removed node, reset synchronous standbys list
		sync := format('%s%s:ALTER SYSTEM SET synchronous_standby_names to '''';',
			 sync, rm_node_id, sync_standbys);
	    PERFORM shardman.broadcast(sync,
								   ignore_errors => true,
								   sync_commit_on => true,
								   super_connstr => true);
	    PERFORM shardman.broadcast(conf, ignore_errors:=true, super_connstr => true);
	END IF;
/* To correctly remove foreign servers we need to update pf_depend table, otherwise
 * our hack with direct update pg_foreign_table leaves deteriorated dependencies 
	-- Remove foreign servers at all nodes for the removed node
    FOR node IN SELECT * FROM shardman.nodes WHERE id<>rm_node_id
	LOOP
		-- Drop server for all nodes at the removed node and drop server at all nodes for the removed node
		fdws := format('%s%s:DROP SERVER node_%s;
			 			  %s:DROP SERVER node_%s;',
			 fdws, node.id, rm_node_id,
			 	   rm_node_id, node.id);
		drps := format('%s%s:DROP USER MAPPING FOR CURRENT_USER SERVER node_%s;
			 			  %s:DROP USER MAPPING FOR CURRENT_USER SERVER node_%s;',
			 drps, node.id, rm_node_id,
			 	   rm_node_id, node.id);
	END LOOP;
*/
	-- Exclude partitions of removed node
	FOR part in SELECT * from shardman.partitions where node_id=rm_node_id
	LOOP
		-- Is there some replica of this node?
		SELECT node_id INTO new_master_id FROM shardman.replicas WHERE part_name=part.part_name ORDER BY random() LIMIT 1;
		IF new_master_id IS NOT NULL
		THEN -- exists some replica for this node: redirect foreign table to this replica and refresh LR channels for this replication group
			-- Update partitions table: now replica is promoted to master...
		    UPDATE shardman.partitions SET node_id=new_master_id WHERE part_name=part.part_name;
			-- ... and is not a replica any more
			DELETE FROM shardman.replicas WHERE part_name=part.part_name AND node_id=new_master_id;

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
			    prts := format('%s%s:SELECT replace_hash_partition(%L,%L);',
			 		prts, node.id, fdw_part_name, part.part_name);
				fdws := format('%s%s:DROP FOREIGN TABLE %I;',
					fdws, node.id, fdw_part_name);
			ELSE
				-- At all other nodes adjust foreign server for foreign table to refer to new master node.
				-- It is not possible to alter foreign server for foreign table so we have to do it in such "hackers" way:
				prts := format('%s%s:UPDATE pg_foreign_table SET ftserver = (SELECT oid FROM pg_foreign_server WHERE srvname = ''node_%s'') WHERE ftrelid = (SELECT oid FROM pg_class WHERE relname=%L);',
		   			prts, node.id, new_master_id, fdw_part_name);
			END IF;
		END LOOP;
	END LOOP;

	-- Broadcast changes of pathman mapping
	PERFORM shardman.broadcast(prts, ignore_errors:=true);
	-- Broadcast drop server commands
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

	-- Generate SQL statement creating this table
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

	-- Check if destination belongs to the same replication group as source
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
	PERFORM shardman.wait_copy_completion(src_node_id, dst_node_id, mv_part_name);

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
	PERFORM shardman.complete_partition_move(src_node_id, dst_node_id, mv_part_name);
END
$$ LANGUAGE plpgsql;

-- Get redundancy of the particular partition
-- This command can be executed only at shardlord.
CREATE FUNCTION get_redundancy_of_partition(pname text) returns bigint AS $$
	SELECT count(*) FROM shardman.replicas where part_name=pname;
$$ LANGUAGE sql;

-- Get minimal redundancy of the specified relation.
-- This command can be executed only at shardlord.
CREATE FUNCTION get_min_redundancy(rel regclass) returns bigint AS $$
	SELECT min(redundancy) FROM (SELECT count(*) redundancy FROM shardman.replicas WHERE relation=rel::text GROUP BY part_name) s;
$$ LANGUAGE sql;

-- Execute command at all shardman nodes.
-- It can be used to perform DDL at all nodes.
CREATE FUNCTION forall(sql text, use_2pc bool = false) returns void AS $$
DECLARE
	node_id integer;
	cmds text = '';
BEGIN
	IF shardman.redirect_to_shardlord(format('forall(%L, %L)', sql, use_2pc))
	THEN
		RETURN;
	END IF;

	-- Loop through all nodes
	FOR node_id IN SELECT * from shardman.nodes
	LOOP
		cmds := format('%s%s:%s;', cmds, node_id, sql);
	END LOOP;
	PERFORM shardman.broadcast(cmds, two_phase => use_2pc);
END
$$ LANGUAGE plpgsql;

-- Count number of partitions at particular node.
-- This command can be executed only at shardlord.
CREATE FUNCTION get_node_partitions_count(node int) returns bigint AS $$
   SELECT count(*) from shardman.partitions WHERE node_id=node;
$$ LANGUAGE sql;

-- Rebalance partitions between nodes. This function tries to evenly
-- redistribute partitions of tables which names match LIKE 'pattern'
-- between all nodes of replication groups.
-- It is not able to move partition between replication groups.
-- This function intentionally moves one partition per time to minimize
-- influence on system performance.
CREATE FUNCTION rebalance(table_pattern text = '%') RETURNS void AS $$
DECLARE
	dst_node int;
	src_node int;
	min_count bigint;
	max_count bigint;
	mv_part_name text;
	repl_group text;
	done bool;
BEGIN
	IF shardman.redirect_to_shardlord(format('rebalance(%L)', table_pattern))
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
			    WHERE n.replication_group=repl_group AND p.relation LIKE table_pattern
				GROUP BY node_id
				ORDER BY n_parts ASC LIMIT 1;
			-- Select node in this group with maximal number of partitions
			SELECT node_id, count(*) n_parts INTO src_node,max_count
			    FROM shardman.partitions p JOIN shardman.nodes n ON p.node_id=n.id
				WHERE n.replication_group=repl_group AND p.relation LIKE table_pattern
				GROUP BY node_id
				ORDER BY n_parts DESC LIMIT 1;
			-- If difference of number of partitions on this nodes is greater
			-- than 1, then move random partition
			IF max_count - min_count > 1 THEN
			    SELECT p.part_name INTO mv_part_name
				FROM shardman.partitions p
				WHERE p.node_id=src_node AND p.relation LIKE table_pattern AND
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

-- Share table between all nodes. This function should be executed at shardlord. The empty table should be present at shardlord,
-- but not at nodes.
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
	pk text;
	dst text;
	src text;
	new_master bool;
BEGIN
	-- Check if valid node ID is passed and get connection string for this node
	SELECT connection_string INTO conn_string FROM shardman.nodes WHERE id=master_node_id;
	IF conn_string IS NULL THEN
	    RAISE EXCEPTION 'There is no node with ID % in the cluster', master_node_id;
	END IF;

	-- Construct list of attributes of the table for update/insert
	SELECT INTO dst, src
          string_agg(quote_ident(attname), ', '),
		  string_agg('NEW.' || quote_ident(attname), ', ')
    FROM   pg_attribute
    WHERE  attrelid = rel
    AND    NOT attisdropped   -- no dropped (dead) columns
    AND    attnum > 0;

	-- Construct primary key condition for update
	SELECT INTO pk
          string_agg(quote_ident(a.attname) || '=OLD.'|| quote_ident(a.attname), ' AND ')
	FROM   pg_index i
	JOIN   pg_attribute a ON a.attrelid = i.indrelid
                     AND a.attnum = ANY(i.indkey)
    WHERE  i.indrelid = rel
    AND    i.indisprimary;

	-- Generate SQL statement creating this table
	SELECT shardman.gen_create_table_sql(rel_name) INTO create_table;

	-- Construct table attributes for create foreign table
	SELECT shardman.reconstruct_table_attrs(rel) INTO table_attrs;

	-- Create rules for redirecting updates
	create_rules := format('CREATE RULE on_update_to_%s AS ON UPDATE TO %I DO INSTEAD UPDATE %I SET (%s) = (%s) WHERE %s;
		 	                CREATE RULE on_insert_to_%s AS ON INSERT TO %I DO INSTEAD INSERT INTO %I (%s) VALUES (%s);
		 	                CREATE RULE on_delete_from_%s AS ON DELETE TO %I DO INSTEAD DELETE FROM %I WHERE %s;',
        rel_name, rel_name, fdw_name, dst, src, pk,
		rel_name, rel_name, fdw_name, dst, src,
		rel_name, rel_name, fdw_name, pk);

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
	SELECT node_id INTO master_node_id FROM shardman.partitions WHERE part_name=mv_part_name;

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
END
$$ LANGUAGE plpgsql;


-- Rebalance replicas between nodes. This function tries to evenly
-- redistribute replicas of partitions of tables which names match LIKE 'pattern'
-- between all nodes of replication groups.
-- It is not able to move replica between replication groups.
-- This function intentionally moves one replica per time to minimize
-- influence on system performance.
CREATE FUNCTION rebalance_replicas(table_pattern text = '%') RETURNS void AS $$
DECLARE
	dst_node int;
	src_node int;
	min_count bigint;
	max_count bigint;
	mv_part_name text;
	repl_group text;
	done bool;
BEGIN
	IF shardman.redirect_to_shardlord(format('rebalance_replicas(%L)', table_pattern))
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
			    WHERE n.replication_group=repl_group AND r.relation LIKE table_pattern
				GROUP BY node_id
				ORDER BY n_parts ASC LIMIT 1;
			-- Select node in this group with maximal number of partitions
			SELECT node_id, count(*) n_parts INTO src_node,max_count
			    FROM shardman.replicas r JOIN shardman.nodes n ON r.node_id=n.id
				WHERE n.replication_group=repl_group AND r.relation LIKE table_pattern
				GROUP BY node_id
				ORDER BY n_parts DESC LIMIT 1;
			-- If difference of number of replicas on this nodes is greater
			-- than 1, then move random partition
			IF max_count - min_count > 1 THEN
			    SELECT src.part_name INTO mv_part_name
				FROM shardman.replicas src
				WHERE src.node_id=src_node AND src.relation LIKE table_pattern
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

---------------------------------------------------------------------
-- Utility functions
---------------------------------------------------------------------

-- Execute command at shardlord
CREATE FUNCTION redirect_to_shardlord(cmd text) RETURNS bool AS $$
BEGIN
	IF NOT shardman.is_shardlord() THEN
	    RAISE NOTICE 'Redirect command "%" to shardlord',cmd;
		PERFORM shardman.broadcast(format('0:SELECT shardman.%s;', cmd));
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
-- Node id '0' means shardlord, shardlord_connstring guc is used.
-- Don't specify them separately with 2pc, we use only one prepared_xact name.
-- No escaping is performed, so ';', '{' and '}' inside queries are not supported.
-- By default functions throws error is execution is failed at some of the
-- nodes, with ignore_errors=true errors are ignored and function returns string
-- with "Error:" prefix containing list of errors terminated by dots with
-- nodes prefixes.
-- In case of normal completion this function return list with node prefixes
-- separated by columns with single result for select queries or number of
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

-- Check from configuration parameters if synchronous replication mode was enabled
CREATE FUNCTION synchronous_replication()
	RETURNS bool AS 'pg_shardman' LANGUAGE C STRICT;

-- Check from configuration parameters if node plays role of shardlord
CREATE FUNCTION is_shardlord()
	RETURNS bool AS 'pg_shardman' LANGUAGE C STRICT;

-- Get subscription status 
CREATE FUNCTION get_subscription_status(sname text) RETURNS "char" AS $$
	SELECT srsubstate FROM pg_subscription_rel srel
		JOIN pg_subscription s on srel.srsubid = s.oid where subname=sname;
$$ LANGUAGE sql;

-- Wait initial sync completion
CREATE FUNCTION wait_sync_completion(src_node_id int, dst_node_id int) RETURNS void AS $$
DECLARE
	timeout_sec int = 1;
	response text;
BEGIN
	LOOP
	    response := shardman.broadcast(format('%s:SELECT shardman.get_subscription_status(''sub_%s_%s'');',
			dst_node_id, dst_node_id, src_node_id));
		EXIT WHEN response='r';
		PERFORM pg_sleep(timeout_sec);
	END LOOP;
END
$$ LANGUAGE plpgsql;


-- Wait completion of partition copy using LR
CREATE FUNCTION wait_copy_completion(src_node_id int, dst_node_id int, part_name text) RETURNS void AS $$
DECLARE
	slot text = format('copy_%s', part_name);
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
		    response := shardman.broadcast(format('%s:SELECT shardman.get_subscription_status(%L);',
				 dst_node_id, slot));
			IF response='r' THEN
			    synced := true;
				RAISE DEBUG 'Table % sync completed', part_name;
				CONTINUE;
			END IF;
	    ELSE
    	    IF NOT locked THEN
			    response := shardman.broadcast(format('%s:SELECT pg_current_wal_lsn() - confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name=%L;', src_node_id, slot));
			ELSE
				response := shardman.broadcast(format('%s:SELECT %L - confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name=%L;', src_node_id, wal_lsn, slot));
			END IF;
			lag := response::bigint;

			RAISE DEBUG 'Replication lag %', lag;
			IF locked THEN
		        IF lag<=0 THEN
			   	    RETURN;
			    END IF;
			ELSIF lag < caughtup_threshold THEN
	   	        PERFORM shardman.broadcast(format('%s:CREATE TRIGGER write_protection BEFORE INSERT OR UPDATE OR DELETE ON %I FOR EACH STATEMENT EXECUTE PROCEDURE shardman.deny_access();',
					src_node_id, part_name));
				SELECT shardman.broadcast(format('%s:SELECT pg_current_wal_lsn();', src_node_id)) INTO wal_lsn;
				locked := true;
				CONTINUE;
			END IF;
		END IF;
		PERFORM pg_sleep(timeout_sec);
	END LOOP;
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION complete_partition_move(src_node_id int, dst_node_id int, part_name text) RETURNS void AS $$
BEGIN
	PERFORM shardman.broadcast(format('%s:TRUNCATE TABLE %I;',
		src_node_id, part_name));
	PERFORM shardman.broadcast(format('%s:DROP TRIGGER write_protection ON %I;',
		src_node_id, part_name));
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION deny_access() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'Access to moving partition is temporary denied';
END
$$ LANGUAGE plpgsql;

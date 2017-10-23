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

-- list of nodes present in the cluster
CREATE TABLE nodes (
	id serial PRIMARY KEY,
	connection_string text UNIQUE NOT NULL,
	replication_group text NOT NULL -- group of nodes within which shard replicas are allocated
);

-- Main partitions
CREATE TABLE partitions (
	part_name text PRIMARY KEY,
	node int NOT NULL REFERENCES nodes(id) ON DELETE CASCADE, -- node on which partition lies
	relation text NOT NULL
);

-- Partition replicas
CREATE TABLE replicas (
	part_name text NOT NULL REFERENCES partitions(part_name) ON DELETE CASCADE,
	node int NOT NULL REFERENCES nodes(id) ON DELETE CASCADE, -- node on which partition lies
	PRIMARY KEY (part_name,node)
);

-- Shardman interface functions

-- Add a node: adjust logical replication channels in replication group and create foreign servers
CREATE FUNCTION add_node(conn_string text, repl_group text = 'all') RETURNS void AS $$
DECLARE
	new_node_id int;
    node shardman.nodes;
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
	sync_standbys text;
	shardlord_conn_string text;
BEGIN
	-- Insert new node in nodes table
	INSERT INTO shardman.nodes (connection_string,replication_group) VALUES (conn_string, repl_group) RETURNING id INTO new_node_id;

	-- Construct list of synchronous standbyes (subscriptions have name node_$ID)
	SELECT string_agg('node_'||id, ',') INTO sync_standbys from shardman.nodes;
	-- Construct foreign server options from connection string of new node
	SELECT * FROM shardman.conninfo_to_postgres_fdw_opts(conn_string) INTO new_server_opts, new_um_opts;

	-- Adjust replication channels within replication group.
	-- We need all-to-all replication channels between all group members.
	FOR node IN SELECT * FROM shardman.nodes WHERE replication_group=repl_group AND id<>new_node_id
	LOOP
		-- Add to new node publications for all existed nodes and add publication for new node to all existed nodes
		pubs := format('%s%s:CREATE PUBLICATION node_%s;
			 			  %s:CREATE PUBLICATION node_%s;
						  %s:SELECT pg_create_logical_replication_slot(''node_%s'', ''pgoutput'');
						  %s:SELECT pg_create_logical_replication_slot(''node_%s'', ''pgoutput'');',
			 pubs, node.id, new_node_id,
			 	   new_node_id, node.id,
			       node.id, new_node_id,
			 	   new_node_id, node.id);
		-- Add to new node subscriptions to existed nodes and add subscription to new node to all existed nodes
		subs := format('%s%s:CREATE SUBSCRIPTION node_%s CONNECTION %L PUBLICATION node_%s with (create_slot=false, slot_name=''node_%s'', synchronous_commit=local);
			 			  %s:CREATE SUBSCRIPTION node_%s CONNECTION %L PUBLICATION node_%s with (create_slot=false, slot_name=''node_%s'', synchronous_commit=local);',
			 subs, node.id, new_node_id, conn_string, node.id, node.id,
			 	   new_node_id, node.id, node.connection_string, new_node_id, new_node_id);

		-- Adjust synchronous standby list at all nodes
		sync := format('%s%s:ALTER SYSTEM SET synchronous_standby_names to %L;',
		     sync, node.id, sync_standbys);
	    conf := format('%s%s:SELECT pg_reload_conf();',
			 conf, node.id);
	END LOOP;

	-- Broadcast create publication commands
    PERFORM shardman.broadcast(pubs);
	-- Broadcast create subscription commands
	PERFORM shardman.broadcast(subs);

	-- In case of synchronous replication broadcast update synchronous standby list commands
	IF shardman.synchronous_replication()
	THEN
	    -- Adjust synchronous standby list at new nodes
		sync := format('%s%s:ALTER SYSTEM SET synchronous_standby_names to %L;',
			 sync, new_node_id, sync_stanbys);
		-- Reload configuration at new node
		conf := format('%s%s:SELECT pg_reload_conf();',
		     conf, new_node_id);
	    PERFORM shardman.broadcast(sync);
	    PERFORM shardman.broadcast(conf);
	END IF;

	-- Add foreign servers for connection to the new node and backward
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
END
$$ LANGUAGE plpgsql;


-- Remove node: try to choose alternative from one of replicas of this nodes, exclude node from replication channels and remove foreign servers.
-- To remove node with existed partitions use force=true parameter
CREATE FUNCTION rm_node(rm_node_id int, force bool = false) RETURNS void AS $$
DECLARE
	node shardman.nodes;
	part shardman.partitions;
	repl shardman.replicas;
	pubs text = '';
	subs text = '';
	fdws text = '';
	sync text = '';
	fdw_part_name text;
    new_master_id int;
	sync_standbys text;
BEGIN
	-- If it is not forced remove, check if there are no partitions at this node
	IF NOT force THEN
	   IF EXISTS (SELECT * FROM shardman.partitions WHERE node=rm_node_id)
	   THEN
	   	  RAISE EXCEPTION 'Use force=true to remove non-empty node';
	   END IF;
    END IF;

	-- Construct new synchronous standby list
	SELECT string_agg('node_'||id, ',') INTO sync_standbys from shardman.nodes WHERE id<>rm_node_id;

	-- Remove all subscriptions and publications of this node
    FOR node IN SELECT * FROM shardman.nodes WHERE replication_group=repl_group AND id<>rm_node_id
	LOOP
		pubs := format('%s%s:DROP PUBLICATION node_%s;
			 			  %s:DROP PUBLICATION node_%s;',
			 pubs, node.id, rm_node_id,
			 	   rm_node_id, node.id);
		subs := format('%s%s:DROP SUBSCRIPTION node_%s;
			 			  %s:DROP SUBSCRIPTION node_%s',
			 subs, rm_node_id, node.id,
			 	   node.id, rm_node_id);
		sync := format('%s%s:SELECT shardman_set_sync_standbys(%L);',
			 sync, node.id, sync_standbys);
	END LOOP;

	-- Broadcast drop subscription commands, ignore errors because removed node may be not available
	PERFORM shardman.broadcast(subs, ignore_errors:=true);
	-- Broadcast drop replication commands
    PERFORM shardman.broadcast(pubs, ignore_errors:=true);

	-- In case of synchronous replication update synchronous standbys list
	IF shardman.synchronous_replication()
	THEN
		-- Update synchronous standbys list at the removed node
		sync := format('%s%s:SELECT shardman_set_sync_standbys(%L);',
			 sync, rm_node_id, sync_standbys);
	    PERFORM shardman.broadcast(sync, ignore_errors:=true);
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
		SELECT node INTO new_master_id FROM shardman.replicas WHERE part_name=part.part_name ORDER BY random() LIMIT 1;
		IF new_master_id IS NOT NULL
		THEN -- exists some replica for this node: redirect foreign table to this replica and refresh LR channels for this replication group
			-- Update partitions table: now replica is promoted to master...
		    UPDATE shardman.partitions SET node=new_master_id WHERE part_name=part.part_name;
			-- ... and is not a replica any more
			DELETE FROM sharaman.replicas WHERE part_name=part.part_name AND node=new_master_id;

			pubs := '';
			subs := '';
			-- Refresh LR channels for this replication group
			FOR repl in SELECT * FROM shardman.replicas WHERE part_name=part.part_name
			LOOP
				-- Publish this partition at new master
			    pubs := format('%s%s:ALTER PUBLICATION node_%s ADD TABLE %I;',
				     pubs, new_master_id, repl.node, part.part_name);
				-- And refresh subscriptions and replicas
				subs := format('%s%s:ALTER SUBSCRIPTION node_%s REFRESH PUBLICATION',
					 subs, repl.node, new_master_id);
			END LOOP;

			-- Broadcast alter publication commands
			PERFORM shardman.broadcast(subs);
			-- Broadcast refresh alter subscription commands
			PERFORM shardman.broadcast(pubs);
		ELSE -- there is no replica: we have to create new empty partition at random mode and redirect all FDWs to it
			SELECT id INTO new_master_id FROM shardman.nodes WHERE id<>rm_node_id ORDER BY random() LIMIT 1;
		    INSERT INTO shardman.partitions (part_name,node,relation) VALUES (part.part.name,new_master_id,part.relation);
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
CREATE FUNCTION create_hash_partitions(rel regclass, expr text, partitions_count int)
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
	IF EXISTS(SELECT relation from shardman.partitions where relation = rel_name)
	THEN
		RAISE EXCEPTION 'Table % is already sharded', rel_name;
	END IF;
	SELECT shardman.gen_create_table_sql(rel_name) INTO create_table;

	-- Create parent table and partitions at all nodes
	FOR node IN SELECT * FROM shardman.nodes
	LOOP
		-- Create parent table at all nodes
		create_tables := format('%s{%s:%s}',
			create_tables, node.id, create_table);
		-- Create partitions using pathman at all nodes
		create_partitions := format('%s%s:select create_hash_partitions(%L,%L,%L);',
			create_partitions, node.id, rel_name, expr, partitions_count);
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

	FOR i IN 0..partitions_count-1
	LOOP
		-- Choose location of new partition
		node_id := node_ids[1 + (i % n_nodes)]; -- round robin
		part_name := format('%s_%s', rel_name, i);
		fdw_part_name := format('%s_fdw', part_name);
		-- Insert information about new partition in partitions table
		INSERT INTO shardman.partitions (part_name,node,relation) VALUES (part_name, node_id, rel_name);
		-- Construct name of the servers where partition will be located
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
END
$$ LANGUAGE plpgsql;

-- Provide requested level of redundancy. 0 means no redundancy.
-- If existed level of redundancy is greater than specified, then right now this function does nothing.
CREATE FUNCTION set_redundancy(rel regclass, redundancy int)
RETURNS void AS $$
DECLARE
	part shardman.partitions;
	n_replicas int;
	repl_node int;
	repl_group text;
	pubs text = '';
	subs text = '';
	rel_name text = rel::text;
BEGIN
	-- Loop though all partitions of this table
	FOR part IN SELECT * from shardman.partitions where relation=rel_name
	LOOP
		-- Count number of replicas of this partition
		SELECT count(*) INTO n_replicas FROM shardman.replicas WHERE part_name=part.part_name;
		IF n_replicas < redundancy
		THEN -- If it is smaller than requested...
			SELECT replication_group INTO repl_group FROM shardman.nodes where id=part.node;
			-- ...then add requested number of replicas in corresponding replication group
			FOR repl_node IN SELECT id FROM shardman.nodes
				WHERE replication_group=repl_group AND id<>part.node AND NOT EXISTS
					(SELECT * FROM shardman.replicas WHERE node=id AND part_name=part.part_name)
				ORDER by random() LIMIT redundancy-n_replicas
			LOOP
				-- Insert information about new replica in replicas table
				INSERT INTO shardman.replicas (part_name,node) VALUES (part.part_name,repl_node);
				-- Establish publications and subscriptions for this partition
				pubs := format('%s%s:ALTER PUBLICATION node_%s ADD TABLE %I;',
					 pubs, part.node, repl_node, part.part_name);
				subs := format('%s%s:ALTER SUBSCRIPTION node_%s REFRESH PUBLICATION;',
					 subs, repl_node, part.node);
			END LOOP;
		END IF;
	END LOOP;

	-- Broadcast alter publication commands
	PERFORM shardman.broadcast(pubs);
	-- Broadcast alter subscription commands
	PERFORM shardman.broadcast(subs);

	-- This function doesn't wait completion of replication sync
END
$$ LANGUAGE plpgsql;


-- Utility functions

-- Generate based on information from catalog SQL statement creating this table
CREATE FUNCTION gen_create_table_sql(relation text)
RETURNS text AS 'pg_shardman' LANGUAGE C STRICT;

-- Reconstruct table attributes for foreign table
CREATE FUNCTION reconstruct_table_attrs(relation regclass)
RETURNS text AS 'pg_shardman' LANGUAGE C STRICT;

-- Broadcast SQL commands to nodes and wait their completion.
-- cmds is list of SQL commands separated by by semi-columns with node prefix: node-id:sql-statement;
-- By default functions throws error is execution is failed at some of the nodes, with ignore_errors=true errors are ignored
-- and function returns string with "Error:" prefix containing list of errors separated by semi-columns with nodes prefixes.
-- In case o normal completion this function return list with node prefixes separated by semi-columns with single result of select queries
-- or number of affected rows for other commands.
-- If two_phase parameter is true, then each statement is wrapped in blocked and prepared with subsequent commit or rollback of prepared transaction
-- at second phase of two phase commit.
CREATE FUNCTION broadcast(cmds text, ignore_errors bool = false, two_phase bool = false)
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


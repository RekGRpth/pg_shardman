/* ------------------------------------------------------------------------
 *
 * shardman.sql
 *   Commands infrastructure, interface funcs, common utility funcs.
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
	connstring text UNIQUE NOT NULL,
	replication_group text -- replication group
);

-- Main partitions
CREATE TABLE partitions (
	part_name text,
	node int NOT NULL REFERENCES nodes(id), -- node on which partition lies
	relation text NOT NULL REFERENCES tables(relation),
	PRIMARY KEY (part_name)
);

-- Partition replicas
CREATE TABLE replicas (
	part_name text NOT NULL REFERENCES parititions(part_name),
	node int NOT NULL REFERENCES nodes(id), -- node on which partition lies
	PRIMARY KEY (part_name,node)
);
	   

-- Shardman interface functions


-- Add a node. Its state will be reset, all shardman data lost.
CREATE FUNCTION add_node(conn_string text, repl_group text) RETURNS void AS $$
DECLARE
	new_node_id integer;
    node    nodes;
	sgardlord_connstr text;
	pubs text := '';
	subs text := '';
	sync text := '';
	conf text := '';
	fdws text := '';
	usms text := '';
	server_opts text;
	um_opts text;
	new_server_opts text;
	new_um_opts text;
BEGIN
	INSERT INTO nodes (connstring,worker_status,replication_group) values (conn_string, 'active',repl_group) returning id into new_node_id;

	SELECT string_agg('node_'||id, ',') INTO sync_stanbys from nodes;

	SELECT * FROM shardman.conninfo_to_postgres_fdw_opts(conn_string) INTO new_server_opts, new_um_opts;

	FOR node IN SELECT * FROM nodes WHERE replication_group=repl_group AND id<>new_node_id
	LOOP
		pubs := format('%s%s:CREATE PUBLICATION node_%s;
			 			  %s:CREATE PUBLICATION node_%s;',
			pubs, node.id, new_node_id,
				  new_node_id, node.id);
		subs := format('%s%s:CREATE SUBSCRIPTION node_%s CONNECTION %L PUBLICATION node_%s with (synchronous_commit = local);
			 			  %s:CREATE SUBSCRIPTION node_%s CONNECTION %L PUBLICATION node_%s with (synchronous_commit = local);',
			 subs, node.id, new_node_id, conn_string, node.id);
			 	   new_node_id, node.id, node.connstring, new_node_id);
		sync := format('%s%s:ALTER SYSTEM SET synchronous_standby_names to %L;', 
			 sync, node.id, sync_stanbys);
		conf := format('%s%s:SELECT pg_reload_conf();', conf, node.id);

		SELECT * FROM shardman.conninfo_to_postgres_fdw_opts(node.connstring) INTO server_opts, um_opts;

		fdws := format('%s%s:CREATE SERVER node_%s FOREIGN DATA WRAPPER postgres_fdw %s;
			 			  %s:CREATE SERVER node_%s FOREIGN DATA WRAPPER postgres_fdw %s;',
			 fdws, new_node_id, node.id, server_ops,
			 	   node.id, new_node_id, new_server_ops);
			 
		usms := format('%s%s:CREATE USER MAPPING FOR CURRENT_USER SERVER node_%s %s;
			 			  %s:CREATE USER MAPPING FOR CURRENT_USER SERVER node_%s %s;',
			 usms, new_node_id, node.id, um_ops,
			      node.id, new_node_id, new_um_ops);
			 
	END LOOP;

	sync := format('%s%s:ALTER SYSTEM SET synchronous_standby_names to %L;', 
		sync, new_node_id, sync_stanbys);
	conf := format('%s%s:SELECT pg_reload_conf();', 
		conf, new_node_id);

	SELECT shardman.shardlord_connection_string() INTO shardlord_connstr;
	subs := format('%s%s:CREATE SUBSCRIPTION shardman_meta_sub CONNECTION %L PUBLICATION shardman_meta_pub with (synchronous_commit = local);',
		 subs, new_node_id, shardlord_connstring);

    PERFORM sharman.broadcast(pubs);
	PERFORM sharman.broadcast(subs);
	PERFORM sharman.broadcast(sync);
	PERFORM sharman.broadcast(conf);
	PERFORM sharman.broadcast(fdws);
	PERFORM sharman.broadcast(usms);
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
		sync := format('%s%s:SELECT shardman_set_sync_standbys(%L);', sync, node.id, sync_stanbys);
		fdws := format('%s%s:DROP SERVER node_%s;
			 			  %s:DROP SERVER node_%s;', 
			fdws, node.id, rm_node_id);
				  rm_node_id, node.id);	
	END LOOP;

    subs := format('%s%s:DROP SUBSCRIPTION shardman_meta_sub;',
		 subs, rm_node_id, connstring);

	PERFORM sharman.broadcast(subs, ignore_errors:=true);
    PERFORM sharman.broadcast(pubs, ignore_errors:=true);
	PERFORM sharman.broadcast(sync, ignore_errors:=true);
	PERFORM sharman.broadcast(fdws, ignore_errors:=true);

	fdws := '';

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
CREATE FUNCTION create_hash_partitions(rel_name text, expr text, partitions_count int)
RETURNS void AS $$
DECLARE
	create_table text;
	cmds text := '';
	node nodes;
	node_ids integer[];
	node_id integer;
	part_name text;
	fdw_part_name text;
	table_attrs text;
	srv_name text;
	create_tables text := '';
	create_partitions text := '';
	create_fdws text := '';
	replace_parts text := '';
	-- Do not drop partitions replaced with foreign tables, ebcause them can be used for replicas
	-- drop_parts text := '';
BEGIN
	IF EXISTS(SELECT relation from shardman.tables where relation = rel_name)
	THEN
		RAISE ERROR 'Table % is already sharded', rel_name;
	END IF;
	SELECT shardman.gen_create_table_sql('%s', '%s') INTO create_table;

	FOR node IN SELECT * FROM nodes WHERE NOT shardlord
	LOOP
		create_tables := format('%s%s:%s;',
			create_tables, node.id, create_table);
		create_partitions := format('%s%s:select create_hash_partitions(''%s'',''%s'',%s);', 
			create_partitions, node.id, rel_name, expr, partitions_count);
	END LOOP;

	PERFORM sharman.broadcast(create_tables);
	PERFORM sharman.broadcast(create_partitions);
	
	SELECT ARRAY(SELECT id from nodes WHERE NOT SHARDLORD ORDER BY random()) INTO node_ids;

	SELECT shardman.reconstruct_table_attrs(rel_nam) INTO table_attr; 

	FOR i IN 1..partitions_count 
	LOOP
		node_id := node_ids[1 + (i % partitions_count)];
		part_name := formt('%s_%s', rel_name, i);
		fdw_part_name :=  format('%s_fdw', part_name);
		INSERT INTO shardman.partitions (part_name,node,relation) VALUES (part_name, node_id, rel_name);
		srv_name := format('node_%s', node_id);
		FOR node IN SELECT * from nodes WHERE NOT shardlord AND id<>node_id
		LOOP
			create_fdw := format('%s%s:CREATE FOREIGN TABLE %I %s SERVER %s OPTIONS (table_name %L)',
				 create_fdws, node.id, fdw_part_name, table_attrs, srv_name, part_name);
			replace_parts := format('%s%s:SELECT replace_hash_partition(%L, %L);', 
				 replace_part, node.id, part_name,  fdw_part_				 
			--drop_parts := format('%s%s:DROP TABKE %I;', 
			--	 drop_parts, node.id, part_name);
		END LOOP;                                                          	
	END LOOP;                                                          	

	PERFORM sharman.broadcast(create_fdws);
	PERFORM sharman.broadcast(replace_parts);
	-- PERFORM sharman.broadcast(drop_parts);
END
$$ LANGUAGE plpgsql;


CREATE FUNCTION set_redundancy(rel_name text, redundancy integer)
RETURNS void AS $$
DECLARE
	part partitions;
	n_replicas integer;
	repl_node integer;
	repl_group text;
	pubs text := '';
	subs text := '';
BEGIN
	FOR part IN SELECT * from partitions where relation=rel_name 
	LOOP
		SELECT count(*) INTO n_replicas FROM sharman.replicas  WHERE part_name=part.part_name;
		IF n_replicas < redundancy
		THEN
			SELECT replication_group INTO repl_group FROM nodes where id=part.node;
			FOR repl_node IN SELECT id INTO repl_node FROM nodes WHERE replication_group=repl_group AND NOT EXISTS(SELECT * FROM replicas WHERE node=id AND part_name=part.part_name ORDER by random() LIMIT redundancy-n_replicas
			LOOP
				INSERT INTO replicas (part_name,node) values (part.part_name,repl_node);
				pubs := format('%s%s:ALTER PUBLICATION node_%s ADD TABLE %I;',
					 pubs, part.node, repl_node, part.part_name);
				subs := formt('%s%s:ALTER SUBSCRIPTION node_%s REFRESH PUBLICATION;',
					 subs, repl_node, part.node);
			END LOOP;
		END IF;
	END LOOP;

	PERFORM sharman.broadcast(pubs);
	PERFORM sharman.broadcast(fdws);
END
$$ LANGUAGE plpgsql;


-- Utility functions

CREATE FUNCTION gen_create_table_sql(relation text, connstring text) 
RETURNS text AS 'pg_shardman' LANGUAGE C STRICT;

CREATE FUNCTION reconstruct_table_attrs(relation regclass)
RETURNS text AS 'pg_shardman' LANGUAGE C STRICT;

CREATE FUNCTION broadcast(cmds text) RETURNS text
RETURNS text AS 'pg_shardman' LANGUAGE C STRICT;

-- Options to postgres_fdw are specified in two places: user & password in user
-- mapping and everything else in create server. The problem is that we use
-- single connstring, however user mapping and server doesn't understand this
-- format, i.e. we can't say create server ... options (dbname 'port=4848
-- host=blabla.org'). So we have to parse the opts and pass them manually. libpq
-- knows how to do it, but doesn't expose that. On the other hand, quote_literal
-- (which is neccessary here) doesn't seem to have handy C API. I resorted to
-- have C function which parses the opts and returns them in two parallel
-- arrays, and this sql function joins them with quoting. TODO: of course,
-- quote_literal_cstr exists.
-- Returns two strings: one with opts ready to pass to CREATE FOREIGN SERVER
-- stmt, and one wih opts ready to pass to CREATE USER MAPPING.
CREATE FUNCTION conninfo_to_postgres_fdw_opts(IN connstring text,
	OUT server_opts text, OUT um_opts text) RETURNS record AS $$
DECLARE
	connstring_keywords text[];
	connstring_vals text[];
	server_opts_first_time_through bool DEFAULT true;
	um_opts_first_time_through bool DEFAULT true;
BEGIN
	server_opts := '';
	um_opts := '';
	SELECT * FROM shardman.pq_conninfo_parse(connstring)
	  INTO connstring_keywords, connstring_vals;
	FOR i IN 1..(SELECT array_upper(connstring_keywords, 1)) LOOP
		IF connstring_keywords[i] = 'client_encoding' OR
			connstring_keywords[i] = 'fallback_application_name' THEN
			CONTINUE; /* not allowed in postgres_fdw */
		ELSIF connstring_keywords[i] = 'user' OR
			connstring_keywords[i] = 'password' THEN -- user mapping option
			IF NOT um_opts_first_time_through THEN
				um_opts := um_opts || ', ';
			END IF;
			um_opts_first_time_through := false;
			um_opts := um_opts ||
				format('%s %L', connstring_keywords[i], connstring_vals[i]);
		ELSE -- server option
			IF NOT server_opts_first_time_through THEN
				server_opts := server_opts || ', ';
			END IF;
			server_opts_first_time_through := false;
			server_opts := server_opts ||
				format('%s %L', connstring_keywords[i], connstring_vals[i]);
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

CREATE FUNCTION pq_conninfo_parse(IN conninfo text, OUT keys text[], OUT vals text[])
	RETURNS record AS 'pg_shardman' LANGUAGE C STRICT;

CREATE FUNCTION shardload_connection_string()
	RETURNS text AS 'pg_shardman' LANGUAGE C STRICT;


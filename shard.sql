/* ------------------------------------------------------------------------
 *
 * shard.sql
 *		Tables & partitions metadata definitions, triggers and utility funcs.
 *
 * ------------------------------------------------------------------------
 */

-- sharded tables
CREATE TABLE tables (
	relation text PRIMARY KEY, -- table name
	expr text NOT NULL,
	partitions_count int NOT NULL,
	create_sql text NOT NULL, -- sql to create the table
	-- Node on which table was partitioned at the beginning. Used only during
	-- initial tables inflation to distinguish between table owner and other
	-- nodes, probably cleaner keep it in separate table.
	initial_node int NOT NULL REFERENCES nodes(id)
);

-- On adding new table, create this table on non-owner nodes using provided sql
-- and partition it. We destroy all existing tables with needed names.
CREATE FUNCTION new_table_worker_side() RETURNS TRIGGER AS $$
DECLARE
	partition_names text[];
	pname text;
BEGIN
	IF NEW.initial_node != (SELECT shardman.get_node_id()) THEN
		EXECUTE format('DROP TABLE IF EXISTS %I CASCADE;', NEW.relation);
		partition_names :=
			(SELECT ARRAY(SELECT part_name FROM shardman.gen_part_names(
				NEW.relation, NEW.partitions_count)));
		FOREACH pname IN ARRAY partition_names LOOP
			EXECUTE format('DROP TABLE IF EXISTS %I', pname);
		END LOOP;
		EXECUTE format('%s', NEW.create_sql);
		EXECUTE format('select create_hash_partitions(%L, %L, %L, true, %L);',
					   NEW.relation, NEW.expr, NEW.partitions_count,
					   partition_names);
	END IF;
	RETURN NULL;
END
$$ LANGUAGE plpgsql;
CREATE TRIGGER new_table_worker_side AFTER INSERT ON shardman.tables
	FOR EACH ROW EXECUTE PROCEDURE new_table_worker_side();
-- fire trigger only on worker nodes
ALTER TABLE shardman.tables ENABLE REPLICA TRIGGER new_table_worker_side;
-- On master side, insert partitions.
-- All of them are primary and have no prev or nxt.
CREATE FUNCTION new_table_master_side() RETURNS TRIGGER AS $$
BEGIN
	INSERT INTO shardman.partitions
	SELECT part_name, 0, NULL, NULL, NEW.relation AS relation, NEW.initial_node AS owner
	  FROM (SELECT part_name FROM shardman.gen_part_names(
		  NEW.relation, NEW.partitions_count))
			   AS partnames;
	RETURN NULL;
END
$$ LANGUAGE plpgsql;
CREATE TRIGGER new_table_master_side AFTER INSERT ON shardman.tables
	FOR EACH ROW EXECUTE PROCEDURE new_table_master_side();

-- Primary shard and its replicas compose a doubly-linked list with 0 shard in
-- the beginning.
CREATE TABLE partitions (
	part_name text,
	-- Shard number. 0 means primary shard.
	num serial,
	nxt int,
	prev int,
	relation text NOT NULL REFERENCES tables(relation),
	owner int REFERENCES nodes(id), -- node on which partition lies
	PRIMARY KEY (part_name, num),
	FOREIGN KEY (part_name, nxt) REFERENCES shardman.partitions(part_name, num),
	FOREIGN KEY (part_name, prev) REFERENCES shardman.partitions(part_name, num),
	-- primary has no prev, replica must have prev
	CONSTRAINT prev_existence CHECK (num = 0 OR prev IS NOT NULL)
);

-- We use _fdw suffix for foreign tables to avoid interleaving with real
-- ones.
CREATE FUNCTION get_fdw_part_name(part_name name) RETURNS name AS $$
BEGIN
	RETURN format('%s_fdw', part_name);
END $$ LANGUAGE plpgsql STRICT;

-- Drop all foreign server's options. Yes, I don't know simpler ways.
CREATE FUNCTION reset_foreign_server_opts(srvname name) RETURNS void AS $$
DECLARE
	opts text[];
	opt text;
	opt_key text;
BEGIN
	EXECUTE format($q$select coalesce(srvoptions, '{}'::text[]) FROM
									  pg_foreign_server WHERE srvname = %L$q$,
									  srvname) INTO opts;
	FOREACH opt IN ARRAY opts LOOP
		opt_key := regexp_replace(substring(opt from '^.*?='), '=$', '');
		EXECUTE format('ALTER SERVER %I OPTIONS (DROP %s);', srvname, opt_key);
	END LOOP;
END $$ LANGUAGE plpgsql STRICT;
-- Same for resetting user mapping opts
CREATE or replace FUNCTION reset_um_opts(srvname name, umuser regrole)
	RETURNS void AS $$
DECLARE
	opts text[];
	opt text;
	opt_key text;
BEGIN
	EXECUTE format($q$select coalesce(umoptions, '{}'::text[]) FROM
				   pg_user_mapping ums JOIN pg_foreign_server fs
				   ON fs.oid = ums.umserver WHERE fs.srvname = %L AND
				   ums.umuser = umuser$q$, srvname)
		INTO opts;

	FOREACH opt IN ARRAY opts LOOP
		opt_key := regexp_replace(substring(opt from '^.*?='), '=$', '');
		EXECUTE format('ALTER USER MAPPING FOR %I SERVER %I OPTIONS (DROP %s);',
					   umuser::name, srvname, opt_key);
	END LOOP;
END $$ LANGUAGE plpgsql STRICT;

-- Update foreign server and user mapping params according to partition part, so
-- this is expected to be called on server/um params change. We use dedicated
-- server for each partition because we plan to use multiple hosts/ports in
-- connstrings for transient fallback to replica if server with main partition
-- fails. FDW server, user mapping, foreign table and (obviously) parent partition
-- must exist when called.
CREATE FUNCTION update_fdw_server(part partitions) RETURNS void AS $$
DECLARE
	connstring text;
	server_opts text;
	um_opts text;
BEGIN
	-- ALTER FOREIGN TABLE doesn't support changing server, ALTER SERVER doesn't
	-- support dropping all params, and I don't want to recreate foreign table
	-- each time server params change, so resorting to these hacks.
	PERFORM shardman.reset_foreign_server_opts(part.part_name);
	PERFORM shardman.reset_um_opts(part.part_name, current_user::regrole);

	SELECT nodes.connstring FROM shardman.nodes WHERE id = part.owner
		INTO connstring;
	SELECT * FROM shardman.conninfo_to_postgres_fdw_opts(connstring, 'ADD ')
	INTO server_opts, um_opts;

	IF server_opts != '' THEN
		EXECUTE format('ALTER SERVER %I %s', part.part_name, server_opts);
	END IF;
	IF um_opts != '' THEN
		EXECUTE format('ALTER USER MAPPING FOR CURRENT_USER SERVER %I %s',
					   part.part_name, um_opts);
	END IF;
END $$ LANGUAGE plpgsql STRICT;

-- Replace existing hash partition with foreign, assuming 'partition' shows
-- where it is stored. Existing partition is dropped.
CREATE FUNCTION replace_usual_part_with_foreign(part partitions)
	RETURNS void AS $$
DECLARE
	connstring text;
	fdw_part_name text;
	server_opts text;
	um_opts text;
BEGIN
	EXECUTE format('DROP SERVER IF EXISTS %I CASCADE;', part.part_name);

	SELECT nodes.connstring FROM shardman.nodes WHERE id = part.owner
		INTO connstring;
	SELECT * FROM shardman.conninfo_to_postgres_fdw_opts(connstring)
		INTO server_opts, um_opts;

	EXECUTE format('CREATE SERVER %I FOREIGN DATA WRAPPER
				   postgres_fdw %s;', part.part_name, server_opts);
	EXECUTE format('DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER %I;',
				   part.part_name);
	-- TODO: support not only CURRENT_USER
	EXECUTE format('CREATE USER MAPPING FOR CURRENT_USER SERVER %I
				   %s;', part.part_name, um_opts);
	SELECT shardman.get_fdw_part_name(part.part_name) INTO fdw_part_name;
	EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I;', fdw_part_name);

	-- Generate and execute CREATE FOREIGN TABLE sql statement which will
	-- clone the existing local table schema. In constrast to
	-- gen_create_table_sql, here we need only the header of the table,
	-- i.e. its attributes. CHECK constraint for partition will be added
	-- during the attachment, and other stuff doesn't seem to have much
	-- sense on foreign table.
	-- In fact, we should have CREATE FOREIGN TABLE (LIKE ...) to make this
	-- sane. We could also used here IMPORT FOREIGN SCHEMA, but it
	-- unneccessary involves network (we already have this schema locally)
	-- and dangerous: what if table was created and dropped before this
	-- change reached us? We might also use it with local table (create
	-- foreign server pointing to it, etc), but that's just ugly.
	EXECUTE format('CREATE FOREIGN TABLE %I %s SERVER %I OPTIONS (table_name %L)',
				   fdw_part_name,
				   (SELECT
						shardman.reconstruct_table_attrs(
							format('%I', part.relation))),
							part.part_name,
							part.part_name);
	-- replace local partition with foreign table
	EXECUTE format('SELECT replace_hash_partition(%L, %L)',
				   part.part_name, fdw_part_name);
	-- And drop old table
	EXECUTE format('DROP TABLE %I', part.part_name);
END $$ LANGUAGE plpgsql;

-- On adding new partition, create proper foreign server & foreign table and
-- replace tmp (empty) partition with it.
CREATE FUNCTION new_partition() RETURNS TRIGGER AS $$
BEGIN
	IF NEW.owner != (SELECT shardman.get_node_id()) THEN
		PERFORM shardman.replace_usual_part_with_foreign(NEW);
	END IF;
	RETURN NULL;
END
$$ LANGUAGE plpgsql;
CREATE TRIGGER new_partition AFTER INSERT ON shardman.partitions
	FOR EACH ROW EXECUTE PROCEDURE new_partition();
-- fire trigger only on worker nodes
ALTER TABLE shardman.partitions ENABLE REPLICA TRIGGER new_partition;

-- Replace foreign table-partition with local. The latter must exist!
-- Foreign table will be dropped.
CREATE FUNCTION replace_foreign_part_with_usual(part partitions)
	RETURNS void AS $$
DECLARE
	fdw_part_name name;
BEGIN
	ASSERT to_regclass(part.part_name) IS NOT NULL;
	SELECT shardman.get_fdw_part_name(part.part_name) INTO fdw_part_name;
	EXECUTE format('SELECT replace_hash_partition(%L, %L);',
				   fdw_part_name, part.part_name);
	EXECUTE format('DROP FOREIGN TABLE %I;', fdw_part_name);
END $$ LANGUAGE plpgsql;

-- Update metadata according to partition move
-- On adding new partition, create proper foreign server & foreign table and
-- replace tmp (empty) partition with it.
CREATE FUNCTION partition_moved() RETURNS TRIGGER AS $$
DECLARE
	movepart_logname text; -- name of logical pub, sub, repslot for copying, etc
	my_id int;
BEGIN
	ASSERT NEW.owner != OLD.owner, 'partition_moved handles only moved parts';
	movepart_logname := format('shardman_copy_%s_%s_%s',
							   OLD.part_name, OLD.owner, NEW.owner);
	my_id := (SELECT shardman.get_node_id());
	IF my_id = OLD.owner THEN -- src node
		-- Drop publication & repslot used for copy
		EXECUTE format('DROP PUBLICATION IF EXISTS %I', movepart_logname);
		PERFORM shardman.drop_repslot(movepart_logname, true);
		-- On src node, replace its partition with foreign one
		PERFORM shardman.replace_usual_part_with_foreign(NEW);
	ELSEIF my_id = NEW.owner THEN -- dst node
		-- Drop subscription used for copy
		PERFORM shardman.eliminate_sub(movepart_logname);
		PERFORM shardman.replace_foreign_part_with_usual(NEW);
	ELSE -- other nodes
		-- just update foreign server
		PERFORM shardman.update_fdw_server(NEW);
	END IF;
	RETURN NULL;
END
$$ LANGUAGE plpgsql;
CREATE TRIGGER partition_moved AFTER UPDATE ON shardman.partitions
	FOR EACH ROW EXECUTE PROCEDURE partition_moved();
-- fire trigger only on worker nodes
ALTER TABLE shardman.partitions ENABLE REPLICA TRIGGER partition_moved;

-- Otherwise partitioned tables on worker nodes not will be dropped properly,
-- see pathman's docs.
ALTER EVENT TRIGGER pathman_ddl_trigger ENABLE ALWAYS;


-- Utility funcs

-- generate one-column table with partition names as 'tablename'_'partnum''suffix'
CREATE FUNCTION gen_part_names(relation text, partitions_count int,
							   suffix text DEFAULT '')
	RETURNS TABLE(part_name text) AS $$
BEGIN
	RETURN QUERY SELECT relation || '_' || range.num || suffix AS partname
		FROM
		(SELECT num FROM generate_series(0, partitions_count - 1, 1)
							 AS range(num)) AS range;
END
$$ LANGUAGE plpgsql;

-- Make table read-only
CREATE FUNCTION readonly_table_on(relation regclass)
	RETURNS void AS $$
BEGIN
	-- Create go away trigger to prevent any new ones
	PERFORM shardman.readonly_table_off(relation);
	EXECUTE format(
		'CREATE TRIGGER shardman_readonly BEFORE INSERT OR UPDATE OR DELETE OR
		TRUNCATE ON %I FOR EACH STATEMENT EXECUTE PROCEDURE shardman.go_away();',
		relation);
	EXECUTE format(
		'ALTER TABLE %I ENABLE ALWAYS TRIGGER shardman_readonly;', relation);
END
$$ LANGUAGE plpgsql STRICT;
CREATE FUNCTION go_away() RETURNS TRIGGER AS $$
BEGIN
	RAISE EXCEPTION 'The "%" table is read only.', TG_TABLE_NAME
		USING HINT = 'Probably table copy is in progress';
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;
-- And make it writable again
CREATE FUNCTION readonly_table_off(relation regclass)
	RETURNS void AS $$
BEGIN
	EXECUTE format('DROP TRIGGER IF EXISTS shardman_readonly ON %s', relation);
END $$ LANGUAGE plpgsql STRICT;

CREATE FUNCTION gen_create_table_sql(relation text, connstring text) RETURNS text
    AS 'pg_shardman' LANGUAGE C;

CREATE FUNCTION reconstruct_table_attrs(relation regclass)
	RETURNS text AS 'pg_shardman' LANGUAGE C STRICT;

-- Options to postgres_fdw are specified in two places: user & password in user
-- mapping and everything else in create server. The problem is that we use
-- single connstring, however user mapping and server doesn't understand this
-- format, i.e. we can't say create server ... options (dbname 'port=4848
-- host=blabla.org'). So we have to parse the opts and pass them manually. libpq
-- knows how to do it, but doesn't expose that. On the other hand, quote_literal
-- (which is neccessary here) doesn't seem to have handy C API. I resorted to
-- have C function which parses the opts and returns them in two parallel
-- arrays, and this sql function joins them with quoting.
-- prfx is prefix added before opt name, e.g. 'ADD ' for use in ALTER SERVER.
-- Returns two strings: one with opts ready to pass to CREATE FOREIGN SERVER
-- stmt, and one wih opts ready to pass to CREATE USER MAPPING.
CREATE FUNCTION conninfo_to_postgres_fdw_opts(
	IN connstring text, IN prfx text DEFAULT '',
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
			um_opts := prfx || um_opts ||
				format('%s %L', connstring_keywords[i], connstring_vals[i]);
		ELSE -- server option
			IF NOT server_opts_first_time_through THEN
				server_opts := server_opts || ', ';
			END IF;
			server_opts_first_time_through := false;
			server_opts := prfx || server_opts ||
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

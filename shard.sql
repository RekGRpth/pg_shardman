/* ------------------------------------------------------------------------
 *
 * shard.sql
 *		Tables & partitions metadata definitions, triggers and utility funcs.
 *
 * ------------------------------------------------------------------------
 */

------------------------------------------------------------
-- Tables
------------------------------------------------------------

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
	SELECT part_name, NEW.initial_node AS owner, NULL, NULL, NEW.relation AS relation
	  FROM (SELECT part_name FROM shardman.gen_part_names(
		  NEW.relation, NEW.partitions_count))
			   AS partnames;
	RETURN NULL;
END
$$ LANGUAGE plpgsql;
CREATE TRIGGER new_table_master_side AFTER INSERT ON shardman.tables
	FOR EACH ROW EXECUTE PROCEDURE new_table_master_side();

------------------------------------------------------------
-- Partitions
------------------------------------------------------------

-- Primary shard and its replicas compose a doubly-linked list: nxt refers to
-- the node containing next replica, prv to node with previous replica (or
-- primary, if we are the first replica). If prv is NULL, this is primary
-- replica. We don't number parts separately since we are not ever going to
-- allow several copies of the same partition on one node.
CREATE TABLE partitions (
	part_name text,
	owner int NOT NULL REFERENCES nodes(id), -- node on which partition lies
	prv int REFERENCES nodes(id),
	nxt int REFERENCES nodes(id),
	relation text NOT NULL REFERENCES tables(relation),
	PRIMARY KEY (part_name, owner)
);

------------------------------------------------------------
-- Metadata triggers
------------------------------------------------------------

-- On adding new primary, create proper foreign server & foreign table and
-- replace tmp (empty) partition with it.
-- TODO: There is a race condition between this trigger and
-- new_table_worker_side trigger during initial tablesync, we should deal with
-- it.
CREATE FUNCTION new_primary() RETURNS TRIGGER AS $$
BEGIN
	RAISE DEBUG '[SHARDMAN] new_primary trigger called for part %, owner %',
		NEW.part_name, NEW.owner;
	IF NEW.owner != shardman.get_node_id() THEN
		PERFORM shardman.replace_usual_part_with_foreign(NEW);
	END IF;
	RETURN NULL;
END
$$ LANGUAGE plpgsql;
CREATE TRIGGER new_primary AFTER INSERT ON shardman.partitions
	FOR EACH ROW WHEN (NEW.prv IS NULL) EXECUTE PROCEDURE new_primary();
-- fire trigger only on worker nodes
ALTER TABLE shardman.partitions ENABLE REPLICA TRIGGER new_primary;

-- Update metadata according to primary move
CREATE FUNCTION primary_moved() RETURNS TRIGGER AS $$
DECLARE
	cp_logname text := shardman.get_cp_logname(OLD.part_name, OLD.owner, NEW.owner);
	my_id int := shardman.get_node_id();
BEGIN
	RAISE DEBUG '[SHARDMAN] primary_moved trigger called for part %, owner %->%',
		NEW.part_name, OLD.owner, NEW.owner;
	ASSERT NEW.owner != OLD.owner, 'primary_moved handles only moved parts';
	IF my_id = OLD.owner THEN -- src node
		-- Drop publication & repslot used for copy
		PERFORM shardman.drop_repslot_and_pub(cp_logname);
		-- On src node, replace its partition with foreign one
		PERFORM shardman.replace_usual_part_with_foreign(NEW);
	ELSEIF my_id = NEW.owner THEN -- dst node
		-- Drop subscription used for copy
		PERFORM shardman.eliminate_sub(cp_logname);
		-- And replace moved table with foreign one
		PERFORM shardman.replace_foreign_part_with_usual(NEW);
	ELSE -- other nodes
		-- just update foreign server
		PERFORM shardman.update_fdw_server(NEW);
	END IF;
	RETURN NULL;
END
$$ LANGUAGE plpgsql;
CREATE TRIGGER primary_moved AFTER UPDATE ON shardman.partitions
	FOR EACH ROW WHEN (OLD.prv is NULL AND NEW.prv IS NULL -- it is primary
					   AND OLD.owner != NEW.owner -- and it is really moved
					   AND OLD.part_name = NEW.part_name) -- sanity check
	EXECUTE PROCEDURE primary_moved();
-- fire trigger only on worker nodes
ALTER TABLE shardman.partitions ENABLE REPLICA TRIGGER primary_moved;

-- Update metadata according to new replica creation.
-- Old tail part is still read-only when this called. There are two main jobs
-- to do: set up LR sync channel between old tail and new replica and update fdw
-- everywhere. For the former we could configure already existing channel used
-- for partition copy, but we will not do that, because
-- * It is not easier than creating new pub & sub: we have to rename pub, drop
--   and create repslot (there is no way to rename it), rename sub, alter sub's
--   slot_name, alter sub's publication, probably rename sub application name,
--   probably run REFRESH (which requires alive pub just as CREATE SUBSCRIPTION)
--   and hope that everything will be ok. Not sure about refreshing, though -- I
--   don't know is it ok not doing it if tables didn't change. Doc says it
--   should be executed.
-- * Since it is not possible to rename repslot and and it is not possible to
--   specify since which lsn start replication, tables must be synced anyway
--   during these operations, so what the point of reusing old sub? And copypart
--   in shard.c really cares that tables are synced at this moment and src is
--   read-only.
CREATE FUNCTION replica_created() RETURNS TRIGGER AS $$
DECLARE
	cp_logname text := shardman.get_cp_logname(NEW.part_name, NEW.prv, NEW.owner);
	oldtail_pubname name := shardman.get_data_pubname(NEW.part_name, NEW.prv);
	oldtail_connstr text := shardman.get_worker_node_connstr(NEW.prv);
	newtail_subname name := shardman.get_data_subname(NEW.part_name, NEW.prv, NEW.owner);
	my_id int := shardman.get_node_id();
BEGIN
	RAISE DEBUG '[SHARDMAN] replica_created trigger called';
	IF my_id = NEW.prv THEN -- old tail node
		-- Drop publication & repslot used for copy
		PERFORM shardman.drop_repslot_and_pub(cp_logname);
		-- Create publication & repslot for new data channel
		PERFORM shardman.create_repslot(oldtail_pubname);
		EXECUTE format('DROP PUBLICATION IF EXISTS %I', oldtail_pubname);
		EXECUTE format('CREATE PUBLICATION %I FOR TABLE %I',
					   oldtail_pubname, NEW.part_name);
		-- Make this channel sync
		PERFORM shardman.ensure_sync_standby(newtail_subname);
		-- Now it is safe to make old tail writable again
		PERFORM shardman.readonly_table_off(relation);
	ELSEIF my_id = NEW.owner THEN -- created replica, i.e. new tail node
		-- Drop subscription used for copy
		PERFORM shardman.eliminate_sub(cp_logname);
		-- And create subscription for new data channel
		-- It should never exist at this moment, but just in case...
		PERFORM shardman.eliminate_sub(newtail_subname);
		EXECUTE format(
			'CREATE SUBSCRIPTION %I connection %L
			PUBLICATION %I with (create_slot = false, slot_name = %L);',
			newtail_subname, oldtail_connstr, oldtail_pubname, oldtail_pubname);
		-- Now fdw connstring to this part should include only primary and myself
		PERFORM shardman.update_fdw_server(NEW);
	ELSE -- other nodes
		-- just update fdw connstr to add new replica
		PERFORM shardman.update_fdw_server(NEW);
	END IF;
	RETURN NULL;
END
$$ LANGUAGE plpgsql;
CREATE TRIGGER replica_created AFTER INSERT ON shardman.partitions
	FOR EACH ROW WHEN (NEW.prv IS NOT NULL) EXECUTE PROCEDURE replica_created();
-- fire trigger only on worker nodes
ALTER TABLE shardman.partitions ENABLE REPLICA TRIGGER replica_created;

-- Otherwise partitioned tables on worker nodes not will be dropped properly,
-- see pathman's docs.
ALTER EVENT TRIGGER pathman_ddl_trigger ENABLE ALWAYS;

------------------------------------------------------------
-- Funcs related to fdw
------------------------------------------------------------

-- Convention: we use _fdw suffix for foreign tables to avoid interleaving with
-- real ones.
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
	RAISE DEBUG '[SHARDMAN] my id: %, creating ft %',
		shardman.get_node_id(), part.part_name;
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

------------------------------------------------------------
-- Read-only tables and replicas
------------------------------------------------------------

-- Make table read-only
CREATE FUNCTION readonly_table_on(relation regclass)
	RETURNS void AS $$
BEGIN
	-- Create go away trigger to prevent any modifications
	PERFORM shardman.readonly_table_off(relation);
	PERFORM shardman.create_modification_triggers(relation, 'shardman_readonly',
												 'shardman.go_away()');
END
$$ LANGUAGE plpgsql STRICT;
CREATE FUNCTION go_away() RETURNS TRIGGER AS $$
BEGIN
	RAISE EXCEPTION 'The "%" table is read only.', TG_TABLE_NAME
		USING HINT = 'Probably table copy is in progress';
END;
$$ LANGUAGE plpgsql;
-- And make it writable again
CREATE FUNCTION readonly_table_off(relation regclass)
	RETURNS void AS $$
BEGIN
	EXECUTE format('DROP TRIGGER IF EXISTS shardman_readonly ON %s', relation);
	EXECUTE format('DROP TRIGGER IF EXISTS shardman_readonly_stmt ON %s', relation);
END $$ LANGUAGE plpgsql STRICT;

-- Make replica read-only, i.e. readonly for all but LR apply workers
CREATE FUNCTION readonly_replica_on(relation regclass)
	RETURNS void AS $$
BEGIN
	RAISE DEBUG '[SHARDMAN] table % made read-only for all but apply workers', relation;
	PERFORM shardman.readonly_replica_off(relation);
	PERFORM shardman.create_modification_triggers(
		relation, 'shardman_readonly_replica', 'shardman.ror_go_away()');
END $$ LANGUAGE plpgsql STRICT;
-- This function is impudent because it is used as both stmt and row trigger.
-- The idea is that we must never reach RETURN NEW after stmt row trigger,
-- because stmt trigger fires only on TRUNCATE which is impossible in LR.
-- Besides, I checked that nothing bad happens if we return NEW from stmt
-- trigger function anyway.
CREATE FUNCTION ror_go_away() RETURNS TRIGGER AS $$
BEGIN
	IF NOT shardman.inside_apply_worker() THEN
		RAISE EXCEPTION 'The "%" table is read only for non-apply workers', TG_TABLE_NAME
		USING HINT =
		'If you see this, most probably node with primary part has failed and' ||
		' you need to promote replica. Promotion is not yet implemented, sorry :(';
	END IF;
	raise warning 'NEW IS %', NEW;
  RETURN NEW;
END $$ LANGUAGE plpgsql;
-- And make replica writable again
CREATE FUNCTION readonly_replica_off(relation regclass) RETURNS void AS $$
BEGIN
	EXECUTE format('DROP TRIGGER IF EXISTS shardman_readonly_replica ON %s',
				   relation);
	EXECUTE format('DROP TRIGGER IF EXISTS shardman_readonly_replica_stmt ON %s',
				   relation);
END $$ LANGUAGE plpgsql STRICT;
CREATE FUNCTION inside_apply_worker() RETURNS bool AS 'pg_shardman' LANGUAGE C;

-- Create two triggers firing exec_proc before any modification operation, make
-- them ALWAYS ENABLE. We need two triggers because TRUNCATE doesn't work with
-- FOR EACH ROW, while LR doesn't support STATEMENT triggers (well, there is no
-- statements in WAL) and changes may sneak through it.
-- If you are curious, we use %I to format any identifiers (e.g. quote identifier
-- with "" if it contains spaces) and use %s while formatting regclass, because
-- it quotes everything automatically while casting oid to name.
CREATE FUNCTION create_modification_triggers(
	relation regclass, trigname name, exec_proc text) RETURNS void AS $$
DECLARE
	stmt_trigname text;
BEGIN
	EXECUTE format(
		'CREATE TRIGGER %I BEFORE INSERT OR UPDATE OR DELETE
		ON %s FOR EACH ROW EXECUTE PROCEDURE %s;',
		trigname, relation, exec_proc);
	EXECUTE format(
		'ALTER TABLE %s ENABLE ALWAYS TRIGGER %I;', relation::text, trigname);
	stmt_trigname := format('%s_stmt', trigname);
	EXECUTE format(
		'CREATE TRIGGER %I BEFORE
		TRUNCATE ON %s FOR EACH STATEMENT EXECUTE PROCEDURE %s;',
		stmt_trigname, relation, exec_proc);
	EXECUTE format(
		'ALTER TABLE %s ENABLE ALWAYS TRIGGER %I;', relation::text, stmt_trigname);
END $$ LANGUAGE plpgsql STRICT;

CREATE FUNCTION gen_create_table_sql(relation text, connstring text) RETURNS text
    AS 'pg_shardman' LANGUAGE C;

CREATE FUNCTION reconstruct_table_attrs(relation regclass)
	RETURNS text AS 'pg_shardman' LANGUAGE C STRICT;

------------------------------------------------------------
-- Other funcs
------------------------------------------------------------

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

-- Convention about pub, sub and repslot name used for copying part part_name
-- from src node to dst node.
CREATE FUNCTION get_cp_logname(part_name text, src int, dst int)
	RETURNS name AS $$
BEGIN
	RETURN format('shardman_copy_%s_%s_%s', part_name, src, dst);
END $$ LANGUAGE plpgsql STRICT;

-- Convention about pub and repslot name used for data replication from part
-- on pub_node node to any part. We don't change pub and repslot while
-- switching subs, so sub node is not included here.
CREATE FUNCTION get_data_pubname(part_name text, pub_node int)
	RETURNS name AS $$
BEGIN
	RETURN format('shardman_data_%s_%s', part_name, pub_node);
END $$ LANGUAGE plpgsql STRICT;

-- Convention about sub and application_name used for data replication. We do
-- recreate sub while switching pub, so pub node is included here.
-- See comment to replica_created on why we don't reuse subs.
CREATE FUNCTION get_data_subname(part_name text, pub_node int, sub_node int)
	RETURNS name AS $$
BEGIN
	RETURN format('shardman_data_%s_%s_%s', part_name, pub_node, sub_node);
END $$ LANGUAGE plpgsql STRICT;

-- Make sure that standby_name is present in synchronous_standby_names. If not,
-- add it via ALTER SYSTEM and SIGHUP postmaster to reread conf.
CREATE FUNCTION ensure_sync_standby(newtail_subname text) RETURNS void as $$
BEGIN
	RAISE DEBUG '[SHARDMAN] imagine standby updated';
END $$ LANGUAGE plpgsql STRICT;

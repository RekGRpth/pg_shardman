-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_shardman" to load this file. \quit

-- Functions here use some gucs defined in .so, so we have to ensure that the
-- library is actually loaded.
DO $$
BEGIN
-- Yes, malicious user might have another extension containing 'pg_shardman'...
-- Probably better just call no-op func from the library
    IF strpos(current_setting('shared_preload_libraries'), 'pg_shardman') = 0 THEN
        RAISE EXCEPTION 'pg_shardman must be loaded via shared_preload_libraries. Refusing to
						proceed.';
    END IF;
END
$$;

-- active is the normal mode, others needed only for proper node add and removal
CREATE TYPE worker_node_status AS ENUM ('active', 'add_in_progress', 'rm_in_progress');

-- list of nodes present in the cluster
CREATE TABLE nodes (
	id serial PRIMARY KEY,
	connstring text NOT NULL UNIQUE,
	worker_status worker_node_status,
	-- While currently we don't support master and worker roles on one node,
	-- potentially node can be either worker, master or both, so we need 3 bits.
	-- One bool with NULL might be fine, but it seems a bit counter-intuitive.
	worker bool NOT NULL DEFAULT true,
	master bool NOT NULL DEFAULT false
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
-- On master side, insert partitions
CREATE FUNCTION new_table_master_side() RETURNS TRIGGER AS $$
BEGIN
	INSERT INTO shardman.partitions
	SELECT part_name, NEW.relation AS relation, NEW.initial_node AS owner
	  FROM (SELECT part_name FROM shardman.gen_part_names(
		  NEW.relation, NEW.partitions_count))
			   AS partnames;
	RETURN NULL;
END
$$ LANGUAGE plpgsql;
CREATE TRIGGER new_table_master_side AFTER INSERT ON shardman.tables
	FOR EACH ROW EXECUTE PROCEDURE new_table_master_side();

CREATE TABLE partitions (
	part_name text PRIMARY KEY,
	relation text NOT NULL REFERENCES tables(relation),
	owner int REFERENCES nodes(id) -- node on which partition lies
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
-- fails. FDW server, user mapping, foreign table and (obviously) parent partition must
-- exist when called.
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

-- Currently it is used just to store node id, in general we can keep any local
-- node metadata here. If is ever used extensively, probably hstore suits better.
CREATE TABLE local_meta (
	k text NOT NULL, -- key
	v text -- value
);
INSERT INTO @extschema@.local_meta VALUES ('node_id', NULL);

-- available commands
CREATE TYPE cmd AS ENUM ('add_node', 'rm_node', 'create_hash_partitions',
						 'move_primary');
-- command status
CREATE TYPE cmd_status AS ENUM ('waiting', 'canceled', 'failed', 'in progress',
								'success');

CREATE TABLE cmd_log (
	id bigserial PRIMARY KEY,
	cmd_type cmd NOT NULL,
	status cmd_status DEFAULT 'waiting' NOT NULL,
	-- only for add_node cmd -- generated id for newly added node. Exists only
	-- when node adding is in progress or node is active. Cleaner to keep this
	-- in separate table...
	node_id int REFERENCES nodes(id)
);

-- Notify shardman master bgw about new commands
CREATE FUNCTION notify_shardmaster() RETURNS trigger AS $$
BEGIN
	NOTIFY shardman_cmd_log_update;
	RETURN NULL;
END
$$ LANGUAGE plpgsql;
CREATE TRIGGER cmd_log_inserts
	AFTER INSERT ON cmd_log
	FOR EACH STATEMENT
	EXECUTE PROCEDURE notify_shardmaster();

-- probably better to keep opts in an array field, but working with arrays from
-- libpq is not very handy
-- opts must be inserted sequentially, we order by them by id
CREATE TABLE cmd_opts (
	id bigserial PRIMARY KEY,
	cmd_id bigint REFERENCES cmd_log(id),
	opt text NOT NULL
);


-- Internal functions

-- Called on shardmaster bgw start. Add itself to nodes table, set id, create
-- publication.
CREATE FUNCTION master_boot() RETURNS void AS $$
DECLARE
	-- If we have never booted as a master before, we have a work to do
	init_master bool DEFAULT false;
	master_connstring text;
	master_id int;
BEGIN
	raise INFO 'Booting master';
	PERFORM shardman.create_meta_pub();

	master_id := shardman.get_node_id();
	IF master_id IS NULL THEN
		SELECT pg_settings.setting into master_connstring from pg_settings
			WHERE NAME = 'shardman.master_connstring';
		EXECUTE format(
			'INSERT INTO @extschema@.nodes VALUES (DEFAULT, %L, NULL, false, true)
			RETURNING id', master_connstring) INTO master_id;
		PERFORM shardman.set_node_id(master_id);
		init_master := true;
	ELSE
		EXECUTE 'SELECT NOT (SELECT master FROM shardman.nodes WHERE id = $1)'
			INTO init_master USING master_id;
		EXECUTE 'UPDATE shardman.nodes SET master = true WHERE id = $1' USING master_id;
	END IF;
	IF init_master THEN
		-- TODO: set up lr channels
	END IF;
END $$ LANGUAGE plpgsql;

-- These tables will be replicated to worker nodes, notifying them about changes.
-- Called on master.
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
-- drop subscription on far end) or pass 'true' to 'with_fire' option, which does
-- the following dirty hack. It kills twice active walsender with 1 second
-- interval. After the first kill, replica will immediately try to reconnect,
-- so the connection resurrects instantly. However, if we kill it second time,
-- replica won't try to reconnect until wal_retrieve_retry_interval after its
-- first reaction passes, which is 5 secs by default. Of course, this is not
-- reliable and should be redesigned.
CREATE FUNCTION drop_repslot(slot_name text, with_fire bool DEFAULT false)
	RETURNS void AS $$
DECLARE
	slot_exists bool;
BEGIN
	RAISE DEBUG 'Dropping repslot %', slot_name;
	EXECUTE format('SELECT EXISTS (SELECT * FROM pg_replication_slots
				   WHERE slot_name = %L)', slot_name) INTO slot_exists;
	IF slot_exists THEN
		IF with_fire THEN -- kill walsender twice
			RAISE DEBUG 'Killing repslot % with fire', slot_name;
			PERFORM shardman.terminate_repslot_walsender(slot_name);
			PERFORM pg_sleep(1);
			PERFORM shardman.terminate_repslot_walsender(slot_name);
		END IF;
		EXECUTE format('SELECT pg_drop_replication_slot(%L)', slot_name);
	END IF;
END
$$ LANGUAGE plpgsql STRICT;
CREATE FUNCTION terminate_repslot_walsender(slot_name text) RETURNS void AS $$
BEGIN
	EXECUTE format('SELECT pg_terminate_backend(active_pid) FROM
				   pg_replication_slots WHERE slot_name = %L', slot_name);
END $$ LANGUAGE plpgsql STRICT;

-- If sub exists, disable it, detach repslot from it and possibly drop. We
-- manage repslots ourselves, so it is essential to detach rs before dropping
-- sub, and repslots can't be detached while subscription is active.
CREATE FUNCTION eliminate_sub(subname name, drop_sub bool DEFAULT true)
	RETURNS void AS $$
DECLARE
	sub_exists bool;
BEGIN
	EXECUTE format('SELECT count(*) > 0 FROM pg_subscription WHERE subname
				   = %L', subname) INTO sub_exists;
	IF sub_exists THEN
		EXECUTE format('ALTER SUBSCRIPTION %I DISABLE', subname);
		EXECUTE format('ALTER SUBSCRIPTION %I SET (slot_name = NONE)', subname);
		IF drop_sub THEN
			EXECUTE format('DROP SUBSCRIPTION %I', subname);
		END IF;
	END IF;
END $$ LANGUAGE plpgsql STRICT;

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
--    it is our extension is deleting, it calls plpgsql cleanup func
CREATE OR REPLACE FUNCTION pg_shardman_cleanup(drop_subs bool DEFAULT true)
	RETURNS void AS $$
DECLARE
	pub record;
	sub record;
	rs record;
BEGIN
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

	PERFORM shardman.reset_node_id();
END;
$$ LANGUAGE plpgsql;
CREATE FUNCTION pg_shardman_cleanup_c() RETURNS event_trigger
    AS 'pg_shardman' LANGUAGE C;
CREATE EVENT TRIGGER cleanup_lr_trigger ON ddl_command_start
	WHEN TAG in ('DROP EXTENSION')
	EXECUTE PROCEDURE pg_shardman_cleanup_c();

-- Get local node id. NULL means node is not in the cluster yet.
CREATE FUNCTION get_node_id() RETURNS int AS $$
	SELECT v::int FROM @extschema@.local_meta WHERE k = 'node_id';
$$ LANGUAGE sql;

-- Exclude node from the cluster
CREATE FUNCTION reset_node_id() RETURNS void AS $$
	UPDATE @extschema@.local_meta SET v = NULL WHERE k = 'node_id';
$$ LANGUAGE sql;

-- Set local node id.
CREATE FUNCTION set_node_id(node_id int) RETURNS void AS $$
	UPDATE @extschema@.local_meta SET v = node_id WHERE k = 'node_id';
$$ LANGUAGE sql;

-- If for cmd cmd_id we haven't yet inserted new node, do that; mark it as passive
-- for now, we still need to setup lr and set its id on the node itself
-- Return generated or existing node id
CREATE FUNCTION insert_node(connstring text, cmd_id bigint) RETURNS int AS $$
DECLARE
	n_id int;
BEGIN
	SELECT node_id FROM @extschema@.cmd_log INTO n_id WHERE id = cmd_id;
	IF n_id IS NULL THEN
		INSERT INTO @extschema@.nodes
			VALUES (DEFAULT, connstring, 'add_in_progress')
			RETURNING id INTO n_id;
		UPDATE @extschema@.cmd_log SET node_id = n_id WHERE id = cmd_id;
	END IF;
	RETURN n_id;
END
$$ LANGUAGE plpgsql;

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

-- Interface functions

-- Add a node. Its state will be reset, all shardman data lost.
CREATE FUNCTION add_node(connstring text) RETURNS void AS $$
DECLARE
	c_id int;
BEGIN
	INSERT INTO @extschema@.cmd_log VALUES (DEFAULT, 'add_node')
										   RETURNING id INTO c_id;
	INSERT INTO @extschema@.cmd_opts VALUES (DEFAULT, c_id, connstring);
END
$$ LANGUAGE plpgsql;

-- Remove node. Its state will be reset, all shardman data lost.
CREATE FUNCTION rm_node(node_id int) RETURNS void AS $$
DECLARE
	c_id int;
BEGIN
	INSERT INTO @extschema@.cmd_log VALUES (DEFAULT, 'rm_node')
										   RETURNING id INTO c_id;
	INSERT INTO @extschema@.cmd_opts VALUES (DEFAULT, c_id, node_id);
END
$$ LANGUAGE plpgsql;

-- Shard table with hash partitions. Params as in pathman, except for relation
-- (master doesn't know oid of the table)
CREATE FUNCTION create_hash_partitions(
	node_id int, expr text, relation text, partitions_count int)
	RETURNS void AS $$
DECLARE
	c_id int;
BEGIN
	INSERT INTO @extschema@.cmd_log VALUES (DEFAULT, 'create_hash_partitions')
										   RETURNING id INTO c_id;
	INSERT INTO @extschema@.cmd_opts VALUES (DEFAULT, c_id, node_id);
	INSERT INTO @extschema@.cmd_opts VALUES (DEFAULT, c_id, expr);
	INSERT INTO @extschema@.cmd_opts VALUES (DEFAULT, c_id, relation);
	INSERT INTO @extschema@.cmd_opts VALUES (DEFAULT, c_id, partitions_count);
END
$$ LANGUAGE plpgsql;

-- Move master partition to another node. Params:
-- 'part_name' is name of the partition to move
-- 'dest' is id of the destination node
CREATE FUNCTION move_primary(part_name text, dest int) RETURNS int AS $$
DECLARE
	c_id int;
BEGIN
	INSERT INTO @extschema@.cmd_log VALUES (DEFAULT, 'move_primary')
										   RETURNING id INTO c_id;
	INSERT INTO @extschema@.cmd_opts VALUES (DEFAULT, c_id, part_name);
	INSERT INTO @extschema@.cmd_opts VALUES (DEFAULT, c_id, dest);
	RETURN c_id;
END $$ LANGUAGE plpgsql;

-- Otherwise partitioned tables on worker nodes not will be dropped properly,
-- see pathman's docs.
ALTER EVENT TRIGGER pathman_ddl_trigger ENABLE ALWAYS;

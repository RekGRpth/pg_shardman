/* ------------------------------------------------------------------------
 *
 * shard.sql
 *		Tables & partitions metadata definitions, triggers and utility funcs.
 *
 * Copyright (c) 2017, Postgres Professional
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
	IF NEW.initial_node != (SELECT shardman.my_id()) THEN
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
-- On lord side, insert partitions.
-- All of them are primary and have no prev or nxt.
CREATE FUNCTION new_table_lord_side() RETURNS TRIGGER AS $$
BEGIN
	INSERT INTO shardman.partitions
	SELECT part_name, NEW.initial_node AS owner, NULL, NULL, NEW.relation AS relation
	  FROM (SELECT part_name FROM shardman.gen_part_names(
		  NEW.relation, NEW.partitions_count))
			   AS partnames;
	RETURN NULL;
END
$$ LANGUAGE plpgsql;
CREATE TRIGGER new_table_lord_side AFTER INSERT ON shardman.tables
	FOR EACH ROW EXECUTE PROCEDURE new_table_lord_side();

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
-- Metadata triggers and funcs called from libpq updating metadata & LR channels
------------------------------------------------------------

-- On adding new primary, create proper foreign server & foreign table and
-- replace tmp (empty) partition with it.
-- TODO: There is a race condition between this trigger and
-- new_table_worker_side trigger during initial tablesync, we should deal with
-- it.
CREATE FUNCTION new_primary() RETURNS TRIGGER AS $$
BEGIN
	RAISE DEBUG '[SHMN] new_primary trigger called for part %, owner %',
		NEW.part_name, NEW.owner;
	IF NEW.owner != shardman.my_id() THEN
		PERFORM shardman.replace_usual_part_with_foreign(NEW);
	END IF;
	RETURN NULL;
END
$$ LANGUAGE plpgsql;
CREATE TRIGGER new_primary AFTER INSERT ON shardman.partitions
	FOR EACH ROW WHEN (NEW.prv IS NULL) EXECUTE PROCEDURE new_primary();
-- fire trigger only on worker nodes
ALTER TABLE shardman.partitions ENABLE REPLICA TRIGGER new_primary;

-- Executed on prev replica after partition move, see mp_rebuild_lr
CREATE FUNCTION part_moved_prev(p_name name, src int, dst int)
	RETURNS void AS $$
DECLARE
	me int := shardman.my_id();
	lname text := shardman.get_data_lname(p_name, me, dst);
BEGIN
	PERFORM shardman.create_repslot(lname);
	-- Create publication for new data channel prev replica -> dst, make it sync
	EXECUTE format('DROP PUBLICATION IF EXISTS %I', lname);
	EXECUTE format('CREATE PUBLICATION %I FOR TABLE %I', lname, p_name);
	-- This is neccessary since sub is not created, and with sync commit we will
	-- hang forever
	SET LOCAL synchronous_commit TO local;
	PERFORM shardman.ensure_sync_standby(lname);
END $$ LANGUAGE plpgsql STRICT;

-- Executed on node with new part, see mp_rebuild_lr
CREATE FUNCTION part_moved_dst(p_name name, src int, dst int)
	RETURNS void AS $$
DECLARE
	next_rep int := nxt FROM shardman.partitions WHERE part_name = p_name
				 AND owner = src;
	prev_rep int := prv FROM shardman.partitions WHERE part_name = p_name
				 AND owner = src;
	next_lname text;
	prev_lname text;
	prev_connstr text;
BEGIN
	ASSERT dst = shardman.my_id(), 'part_moved_dst must be called on dst';
	IF next_rep IS NOT NULL THEN -- we need to setup channel dst -> next replica
		next_lname := shardman.get_data_lname(p_name, dst, next_rep);
		-- This must be first write in the transaction!
		PERFORM shardman.create_repslot(next_lname);
		EXECUTE format('DROP PUBLICATION IF EXISTS %I', next_lname);
		EXECUTE format('CREATE PUBLICATION %I FOR TABLE %I',
					   next_lname, p_name);
		-- This is neccessary since sub is not created, and with sync commit we will
		-- hang forever
		SET LOCAL synchronous_commit TO local;
		PERFORM shardman.ensure_sync_standby(next_lname);
	END IF;

	IF prev_rep IS NOT NULL THEN -- we need to setup channel prev replica -> dst
		prev_lname := shardman.get_data_lname(p_name, prev_rep, dst);
		prev_connstr := shardman.get_worker_node_connstr(prev_rep);
		PERFORM shardman.eliminate_sub(prev_lname);
		EXECUTE format(
			'CREATE SUBSCRIPTION %I connection %L
		PUBLICATION %I with (create_slot = false, slot_name = %L, copy_data = false, synchronous_commit = on);',
		prev_lname, prev_connstr, prev_lname, prev_lname);
		-- If we have prev, we are replica
		PERFORM shardman.readonly_replica_on(p_name::regclass);
	END IF;
END $$ LANGUAGE plpgsql STRICT;

-- Executed on next replica after partition move, see mp_rebuild_lr
CREATE FUNCTION part_moved_next(p_name name, src int, dst int)
	RETURNS void AS $$
DECLARE
	me int := shardman.my_id();
	lname text := shardman.get_data_lname(p_name, dst, me);
	dst_connstr text := shardman.get_worker_node_connstr(dst);
BEGIN
	-- Create subscription for new data channel dst -> next replica
	-- It should never exist at this moment, but just in case...
	PERFORM shardman.eliminate_sub(lname);
	EXECUTE format(
		'CREATE SUBSCRIPTION %I connection %L
		PUBLICATION %I with (create_slot = false, slot_name = %L, copy_data = false, synchronous_commit = on);',
		lname, dst_connstr, lname, lname);
END $$ LANGUAGE plpgsql STRICT;

-- Partition moved, update fdw and drop old LR channels.
CREATE FUNCTION part_moved() RETURNS TRIGGER AS $$
DECLARE
	cp_logname text := shardman.get_cp_logname(NEW.part_name, OLD.owner, NEW.owner);
	me int := shardman.my_id();
	prev_src_lname text;
	src_next_lname text;
BEGIN
	ASSERT NEW.owner != OLD.owner, 'part_moved handles only moved parts';
	RAISE DEBUG '[SHMN] part_moved trigger called for part %, owner % -> %',
		NEW.part_name, OLD.owner, NEW.owner;
	ASSERT NEW.nxt = OLD.nxt OR (NEW.nxt IS NULL AND OLD.nxt IS NULL),
		'both part and replica must not be moved in one update';
	ASSERT NEW.prv = OLD.prv OR (NEW.prv IS NULL AND OLD.prv IS NULL),
		'both part and replica must not be moved in one update';
	IF NEW.prv IS NOT NULL THEN
		prev_src_lname := shardman.get_data_lname(NEW.part_name, NEW.prv, OLD.owner);
	END IF;
	IF NEW.nxt IS NOT NULL THEN
		src_next_lname := shardman.get_data_lname(NEW.part_name, OLD.owner, NEW.nxt);
	END IF;

	IF me = OLD.owner THEN -- src node
		-- Drop publication & repslot used for copy
		PERFORM shardman.drop_repslot_and_pub(cp_logname);
		-- If primary part was moved, replace on src node its partition with
		-- foreign one
		IF NEW.prv IS NULL THEN
			PERFORM shardman.replace_usual_part_with_foreign(NEW);
		ELSE
			-- On the other hand, if prev replica existed, drop sub for old
			-- channel prev -> src
			PERFORM shardman.eliminate_sub(prev_src_lname);
		END IF;
		IF NEW.nxt IS NOT NULL THEN
			-- If next replica existed, drop pub for old channel src -> next
			PERFORM shardman.drop_repslot_and_pub(src_next_lname);
			PERFORM shardman.remove_sync_standby(src_next_lname);
		END IF;
		-- Drop old table anyway;
		EXECUTE format('DROP TABLE IF EXISTS %I', NEW.part_name);
	ELSEIF me = NEW.owner THEN -- dst node
		RAISE DEBUG '[SHMN] part_moved trigger on dst node';
		-- Drop subscription used for copy
		PERFORM shardman.eliminate_sub(cp_logname);
		-- If primary part was moved, replace moved table with foreign one
		IF NEW.prv IS NULL THEN
			PERFORM shardman.replace_foreign_part_with_usual(NEW);
		END IF;
	ELSEIF me = NEW.prv THEN -- node with prev replica
		-- Drop pub for old channel prev -> src
		PERFORM shardman.drop_repslot_and_pub(prev_src_lname);
		PERFORM shardman.remove_sync_standby(prev_src_lname);
	ELSEIF me = NEW.nxt THEN -- node with next replica
		-- Drop sub for old channel src -> next
		PERFORM shardman.eliminate_sub(src_next_lname);
	END IF;

	-- And update fdw almost everywhere
	PERFORM shardman.update_fdw_server(NEW);
	RETURN NULL;
END
$$ LANGUAGE plpgsql;
CREATE TRIGGER part_moved AFTER UPDATE ON shardman.partitions
	FOR EACH ROW WHEN (OLD.owner != NEW.owner -- part is really moved
					   AND OLD.part_name = NEW.part_name) -- sanity check
	EXECUTE PROCEDURE part_moved();
-- fire trigger only on worker nodes
ALTER TABLE shardman.partitions ENABLE REPLICA TRIGGER part_moved;


-- Partition removed: drop LR channel and promote replica if primary was
-- removed. Since for now we support only 2 copies (1 replica), we promote
-- replica immediately if needed. Case with several replicas is much more
-- complex because we need to rebuild LR channels, so later we will have
-- separate cmd promote_replica(), while part deletion will just perform
-- cleanup. Here we do nothing if we are removing the last copy of data, the
-- caller is responsible for tracking that.
CREATE FUNCTION part_removed() RETURNS TRIGGER AS $$
DECLARE
	replica_removed bool := OLD.prv IS NOT NULL; -- replica or primary removed?
	 -- if primary removed, is there replica that we will promote?
	replica_exists bool := OLD.nxt IS NOT NULL;
	prim_repl_lname text; -- channel between primary and replica
	me int := shardman.my_id();
	new_primary shardman.partitions;
	drop_slot_delay int := 2; -- two seconds
BEGIN
	RAISE DEBUG '[SHMN] part_removed trigger called for part %, owner %',
		OLD.part_name, OLD.owner;

	IF OLD.prv IS NOT NULL AND OLD.nxt IS NOT NULL THEN
		RAISE WARNING '[SHMN] part_removed is not yet implemented for redundancy level > 2';
		RETURN NULL;
	END IF;

	-- get log channel name and part we will promote, if any
	IF replica_removed THEN
		prim_repl_lname := shardman.get_data_lname(OLD.part_name, OLD.prv, OLD.owner);
	ELSE -- Primary is removed
		IF replica_exists THEN -- Primary removed, and it has replica
			prim_repl_lname := shardman.get_data_lname(OLD.part_name, OLD.owner,
													   OLD.nxt);
			-- This replica is new primary
			SELECT * FROM shardman.partitions
			 WHERE owner = OLD.nxt AND part_name= OLD.part_name INTO new_primary;
			-- whole record nullability seems to be non-working
			ASSERT new_primary.part_name IS NOT NULL;
		END IF;
	END IF;

	IF me = OLD.owner THEN -- part dropped on us
		IF replica_removed THEN -- replica removed on us
			PERFORM shardman.eliminate_sub(prim_repl_lname);
		ELSE -- primary removed on us
			IF replica_exists IS NOT NULL THEN
				-- If next replica existed, drop pub & rs for data channel
 				-- Wait sometime to let replica first remove subscription
				PERFORM pg_sleep(drop_slot_delay);
				PERFORM shardman.drop_repslot_and_pub(prim_repl_lname);
				PERFORM shardman.remove_sync_standby(prim_repl_lname);
				-- replace removed table with foreign one on promoted replica
				PERFORM shardman.replace_usual_part_with_foreign(new_primary);
			END IF;
		END IF;
		-- Drop old table anyway
		EXECUTE format('DROP TABLE IF EXISTS %I', OLD.part_name);
	ELSEIF me = OLD.prv THEN -- node with primary for which replica was dropped
		-- Wait sometime to let other node first remove subscription
		PERFORM pg_sleep(drop_slot_delay);
		-- Drop pub & rs for data channel
		PERFORM shardman.drop_repslot_and_pub(prim_repl_lname);
		PERFORM shardman.remove_sync_standby(prim_repl_lname);
	ELSEIF me = OLD.nxt THEN -- node with replica for which primary was dropped
		-- Drop sub for data channel
		PERFORM shardman.eliminate_sub(prim_repl_lname);
	    -- This replica is promoted to primary node, so drop trigger disabling
	    -- writes to the table and replace fdw with normal part
		PERFORM shardman.readonly_replica_off(OLD.part_name);
		-- Replace FDW with local partition
	    PERFORM shardman.replace_foreign_part_with_usual(new_primary);
 	END IF;

	IF NOT replica_removed AND shardman.me_worker() THEN
	   -- update fdw almost everywhere
	   PERFORM shardman.update_fdw_server(new_primary);
	END IF;

	IF shardman.me_lord() THEN
		-- update partitions table: promote replica immediately after primary
		-- removal or remove link to dropped replica.
		IF replica_removed THEN
			UPDATE shardman.partitions SET nxt = NULL WHERE owner = OLD.prv AND
			part_name = OLD.part_name;
		ELSE
			UPDATE shardman.partitions SET prv = NULL
			 WHERE owner = OLD.nxt AND part_name = OLD.part_name;
		END IF;
	END IF;

	RETURN NULL;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER part_removed AFTER DELETE ON shardman.partitions
	FOR EACH ROW
	EXECUTE PROCEDURE part_removed();
-- fire trigger only on either shardlord and worker nodes
ALTER TABLE shardman.partitions ENABLE ALWAYS TRIGGER part_removed;




-- Executed on newtail node, see cr_rebuild_lr
CREATE FUNCTION replica_created_drop_cp_sub(
	part_name name, oldtail int, newtail int) RETURNS void AS $$
DECLARE
	cp_logname text := shardman.get_cp_logname(part_name, oldtail, newtail);
BEGIN
	-- Drop subscription used for copy
	PERFORM shardman.eliminate_sub(cp_logname);
END $$ LANGUAGE plpgsql;

-- Executed on oldtail node, see cr_rebuild_lr
CREATE FUNCTION replica_created_create_data_pub(
	part_name name, oldtail int, newtail int) RETURNS void AS $$
DECLARE
	cp_logname text := shardman.get_cp_logname(part_name, oldtail, newtail);
	lname name := shardman.get_data_lname(part_name, oldtail, newtail);
BEGIN
	-- Repslot for new data channel. Must be first, since we "cannot create
	-- logical replication slot in transaction that has performed writes".
	PERFORM shardman.create_repslot(lname);
	-- Drop publication & repslot used for copy
	PERFORM shardman.drop_repslot_and_pub(cp_logname);
	-- Create publication for new data channel
	EXECUTE format('DROP PUBLICATION IF EXISTS %I', lname);
	EXECUTE format('CREATE PUBLICATION %I FOR TABLE %I', lname, part_name);
	-- Make this channel sync.
	-- This is neccessary since sub is not created, and with sync commit we will
	-- hang forever.
	SET LOCAL synchronous_commit TO local;
	PERFORM shardman.ensure_sync_standby(lname);
	-- Now it is safe to make old tail writable again
	PERFORM shardman.readonly_table_off(part_name::regclass);
END $$ LANGUAGE plpgsql;

-- Executed on newtail node, see cr_rebuild_lr
CREATE FUNCTION replica_created_create_data_sub(
	part_name name, oldtail int, newtail int) RETURNS void AS $$
DECLARE
	lname name := shardman.get_data_lname(part_name, oldtail, newtail);
	oldtail_connstr text := shardman.get_worker_node_connstr(oldtail);
BEGIN
	PERFORM shardman.readonly_replica_on(part_name::regclass);
	-- Create subscription for new data channel
	-- It should never exist at this moment, but just in case...
	PERFORM shardman.eliminate_sub(lname);
	EXECUTE format(
		'CREATE SUBSCRIPTION %I connection %L
		PUBLICATION %I with (create_slot = false, slot_name = %L, copy_data = false, synchronous_commit = on);',
		lname, oldtail_connstr, lname, lname);
END $$ LANGUAGE plpgsql;

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
CREATE FUNCTION reset_foreign_server_opts(sname name) RETURNS void AS $$
DECLARE
	opts text[];
	opt text;
	opt_key text;
BEGIN
	ASSERT EXISTS (SELECT 1 FROM pg_foreign_server WHERE srvname = sname);
	EXECUTE format($q$SELECT coalesce(srvoptions, '{}'::text[]) FROM
									  pg_foreign_server WHERE srvname = %L$q$,
									  sname) INTO opts;
	FOREACH opt IN ARRAY opts LOOP
		opt_key := regexp_replace(substring(opt from '^.*?='), '=$', '');
		EXECUTE format('ALTER SERVER %I OPTIONS (DROP %s);', sname, opt_key);
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

-- Update foreign server and user mapping params on current node according to
-- partition part, so this is expected to be called on server/um params
-- change. We use dedicated server for each partition because we plan to use
-- multiple hosts/ports in connstrings for transient fallback to replica if
-- server with main partition fails. FDW server, user mapping, foreign table and
-- (obviously) parent partition must exist when called if they need to be
-- updated; however, it is ok to call this func on nodes which don't need fdw
-- setup for this part because they hold primary partition.
CREATE FUNCTION update_fdw_server(part partitions) RETURNS void AS $$
DECLARE
	connstring text;
	server_opts text;
	um_opts text;
	me int := shardman.my_id();
	my_part shardman.partitions;
BEGIN
	RAISE DEBUG '[SHMN] update_fdw_server called for part %, owner %',
		part.part_name, part.owner;

	SELECT * FROM shardman.partitions WHERE part_name = part.part_name AND
											owner = me INTO my_part;
	IF my_part.part_name IS NOT NULL THEN -- we are holding the part
		IF my_part.prv IS NULL THEN
			RAISE DEBUG '[SHMN] we are holding primary for part %, not updating fdw server for it', part.part_name;
			RETURN;
		ELSE
			RAISE DEBUG '[SHMN] we are holding replica for part %, updating fdw server for it', part.part_name;
		END IF;
	END IF;

	SELECT nodes.connstring FROM shardman.nodes WHERE id = part.owner
		INTO connstring;
	SELECT * FROM shardman.conninfo_to_postgres_fdw_opts(connstring)
	  INTO server_opts, um_opts;

	-- ALTER FOREIGN TABLE doesn't support changing server, ALTER SERVER doesn't
	-- support dropping all params, and I don't want to recreate foreign table
	-- each time server params change, so resorting to these hacks.
	PERFORM shardman.reset_foreign_server_opts(part.part_name);
	PERFORM shardman.reset_um_opts(part.part_name, current_user::regrole);

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
	RAISE DEBUG '[SHMN] creating ft %', part.part_name;
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
	RAISE EXCEPTION '[SHMN] The "%" table is read only.', TG_TABLE_NAME
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
	RAISE DEBUG '[SHMN] table % made read-only for all but apply workers',
		relation;
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
		RAISE EXCEPTION '[SHMN] The "%" table is read only for non-apply workers', TG_TABLE_NAME
		USING HINT =
		'If you see this, most probably node with primary part has failed and' ||
		' you need to promote replica. Promotion is not yet implemented, sorry :(';
	END IF;
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
    AS 'pg_shardman' LANGUAGE C STRICT;

CREATE FUNCTION reconstruct_table_attrs(relation regclass)
	RETURNS text AS 'pg_shardman' LANGUAGE C STRICT;

------------------------------------------------------------
-- Other funcs
------------------------------------------------------------

-- Drop (locally) all partitions of given table, if they exist
CREATE FUNCTION drop_parts(relation text, partitions_count int)
	RETURNS void as $$
DECLARE
	r record;
BEGIN
	FOR r IN SELECT part_name
		FROM shardman.gen_part_names(relation, partitions_count) LOOP
		EXECUTE format('DROP TABLE IF EXISTS %I;', r.part_name);
	END LOOP;
END $$ LANGUAGE plpgsql STRICT;

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

/*
 * Convention about pub, repslot, sub and application_name used for data
 * replication. We recreate sub while switching pub and recreate pub when
 * switching sub, so including both in the name. See top comment on why we
 * don't reuse pubs and subs.
 */
CREATE FUNCTION get_data_lname(part_name text, pub_node int, sub_node int)
	RETURNS name AS $$
BEGIN
	RETURN format('shardman_data_%s_%s_%s', part_name, pub_node, sub_node);
END $$ LANGUAGE plpgsql STRICT;

-- Make sure that standby_name is present in synchronous_standby_names. If not,
-- add it via ALTER SYSTEM and SIGHUP postmaster to reread conf.
CREATE FUNCTION ensure_sync_standby(standby text) RETURNS void AS $$
DECLARE
	newval text := shardman.ensure_sync_standby_c(standby);
BEGIN
	IF newval IS NOT NULL THEN
		RAISE DEBUG '[SHMN] Adding standby %, new value is %', standby, newval;
		PERFORM shardman.set_sync_standbys(newval);
	END IF;
END $$ LANGUAGE plpgsql STRICT;
CREATE FUNCTION ensure_sync_standby_c(standby text) RETURNS text
    AS 'pg_shardman' LANGUAGE C STRICT;

-- Remove 'standby' from synchronous_standby_names, if it is there, and SIGHUP
-- postmaster.
CREATE FUNCTION remove_sync_standby(standby text) RETURNS void AS $$
DECLARE
	newval text := shardman.remove_sync_standby_c(standby);
BEGIN
	IF newval IS NOT NULL THEN
		RAISE DEBUG '[SHMN] Removing standby %, new value is %', standby, newval;
		PERFORM shardman.set_sync_standbys(newval);
	END IF;
END $$ LANGUAGE plpgsql STRICT;
CREATE FUNCTION remove_sync_standby_c(standby text) RETURNS text
	AS 'pg_shardman' LANGUAGE C STRICT;

CREATE FUNCTION set_sync_standbys(standby text) RETURNS void AS $$
BEGIN
	PERFORM pg_reload_conf();
	PERFORM shardman.alter_system_c('synchronous_standby_names', standby);
	RAISE DEBUG '[SHMN] sync_standbys set to %', standby;
END $$ LANGUAGE plpgsql STRICT;

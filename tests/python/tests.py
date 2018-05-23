#!/usr/bin/env python3
# coding: utf-8
"""
 tests.py
        pg_shardman tests.

        Copyright (c) 2017, Postgres Professional
"""

import unittest
import os
import threading
import time

import testgres
from testgres import PostgresNode, TestgresConfig, NodeStatus, IsolationLevel
from testgres import ProgrammingError


STMT_TIMEOUT_ERRCODE = '57014'
DBNAME = "postgres"


def common_conn_string(port):
    return "dbname={} port={}".format(DBNAME, port)


class Shardlord(PostgresNode):
    def __init__(self,
                 num_repgroups=0,
                 nodes_in_repgroup=1,
                 worker_creation_cbk=None,
                 additional_lord_conf="",
                 additional_worker_conf=""):
        # worker_id (int) -> PostgresNode
        self.workers_dict = {}
        # list of allocated PostgresNodes (probably already excluded from the
        # cluster), used for cleanup
        self.worker_nodes = []
        # next port to allocate in debug mode
        self.next_port = 5432

        # create logs directory
        script_dir = os.path.dirname(os.path.realpath(__file__))
        self.tests_logs_dir = os.path.join(script_dir, "logs")
        if not os.path.exists(self.tests_logs_dir):
            os.makedirs(self.tests_logs_dir)

        super(Shardlord, self).__init__(port=self.get_next_port())
        super(Shardlord, self).init()

        self.create_cluster(num_repgroups, nodes_in_repgroup,
                            worker_creation_cbk, additional_lord_conf,
                            additional_worker_conf)

    def __enter__(self):
        return self

    # cleanup everything
    def __exit__(self, type, value, traceback):
        super(Shardlord, self).__exit__(type, value, traceback)

        for worker in self.worker_nodes:
            worker.__exit__(type, value, traceback)

    def get_next_port(self):
        port = self.next_port
        self.next_port += 1
        return port

    def _shardlord_connstring(self):
        return common_conn_string(self.port)

    def _common_conf_lines(self):
        # yapf: disable
        return ("""
            shared_preload_libraries = 'pg_shardman'
            shardman.shardlord_connstring = '{}'

            shared_buffers = 512MB
            max_connections = 1000
            max_wal_size = 5GB

            log_min_messages = DEBUG1
            client_min_messages = NOTICE
            log_replication_commands = on
            log_statement = all
            TimeZone = 'Europe/Moscow'
            log_timezone = 'Europe/Moscow'
        """).format(self._shardlord_connstring())

    # reset conf and start clean shardlord
    def start_lord(self, additional_conf=""):
        if self.status() == NodeStatus.Running:
            self.stop()
        self.default_conf()
        config_lines = self._common_conf_lines()
        config_lines += ("shardman.shardlord = on\n"
                         "shardman.shardlord_dbname = {}\n"
                         "shardman.sync_replication = on\n").format(DBNAME)
        config_lines += additional_conf
        self.append_conf("postgresql.conf", config_lines)
        super(Shardlord, self).start()
        self.safe_psql("create extension pg_shardman cascade")

        # create symlink to lord's log
        log_path = os.path.join(self.tests_logs_dir, "lord.log")
        try:
            os.remove(log_path)
        except OSError:
            pass
        os.symlink(self.pg_log_name, log_path)

        return self

    # create fresh cluster with given num of repgroups and nodes in each one
    def create_cluster(self,
                       num_repgroups,
                       nodes_in_repgroup=1,
                       worker_creation_cbk=None,
                       additional_lord_conf="",
                       additional_worker_conf=""):
        self.start_lord(additional_conf=additional_lord_conf)
        for rgnum in range(num_repgroups):
            for nodenum in range(nodes_in_repgroup):
                self.add_node(repl_group="rg_{}".format(rgnum),
                              worker_creation_cbk=worker_creation_cbk,
                              additional_conf=additional_worker_conf)

    # destroy and shutdown everything, but keep nodes.
    # Note: this was used when we reused single node in different tests.
    # Now it is not needed, but probably we will use it some day again.
    def destroy_cluster(self):
        for worker_id, worker in self.workers_dict.items():
            # start node to cleanup if it was shut down during the test
            if worker.status() != NodeStatus.Running:
                worker.start()
            worker.safe_psql("""
set local synchronous_commit to local;
select shardman.wipe_state();
drop extension pg_shardman cascade;
do language plpgsql $$
        declare
                rel_name name;
        begin
                for rel_name in select relname from pg_class where relname like 'pt%' and relkind = 'r' loop
                        execute format('drop table if exists %I cascade', rel_name);
                end loop;
                for rel_name in select relname from pg_class where relname like 'pt%' and relkind = 'f' loop
                        execute format('drop foreign table %I cascade', rel_name);
                end loop;
        end
$$;
                             """)
            worker.stop()
        self.workers_dict = {}

        self.safe_psql("drop extension pg_shardman cascade;")

    # allocate and init new worker with proper conf
    def spawn_worker(self):
        # Set env var DBG_MODE=1 to bind PG to standard ports
        if os.environ.get('DBG_MODE') == '1':
            port = self.get_next_port()
        else:
            port = None

        node = PostgresNode(port=port).init()

        # worker conf

        # reset conf
        node.default_conf()
        config_lines = self._common_conf_lines()
        config_lines += """
        wal_level = logical
        max_replication_slots = 101
        max_wal_senders = 51
        max_logical_replication_workers = 51
        max_worker_processes = 60
        wal_receiver_timeout = 60s

        synchronous_commit = on

        max_prepared_transactions = 1000
        postgres_fdw.use_2pc = on
        # only for testing performace; setting this to 'on' violates visibility
        postgres_fdw.use_repeatable_read = off

        shardman.shardlord = off
        """
        node.append_conf("postgresql.conf", config_lines)
        return node

    # Add worker. Returns (node instance, node id) pair. Callback is called when
    # node is started, but not yet registred. It might return conn_string, uh.
    def add_node(self,
                 repl_group=None,
                 additional_conf="",
                 worker_creation_cbk=None):
        node = self.spawn_worker()

        # start this node
        node.append_conf("postgresql.conf", additional_conf) \
            .start() \
            .safe_psql("create extension pg_shardman cascade;")
        # call callback, if needed
        conn_string = None
        if worker_creation_cbk:
            conn_string = worker_creation_cbk(node)
        conn_string = "'{}'".format(conn_string) if conn_string else 'NULL'
        repl_group = "'{}'".format(repl_group) if repl_group else 'NULL'
        # and register this node
        super_conn_string = common_conn_string(node.port)
        add_node_cmd = "select shardman.add_node('{}', conn_string => {}, " "repl_group => {})" \
            .format(super_conn_string, conn_string, repl_group)
        new_node_id = int(self.execute(add_node_cmd)[0][0])
        self.workers_dict[new_node_id] = node
        # used for cleanup
        self.worker_nodes.append(node)

        # create symlink to this node's log
        log_path = os.path.join(self.tests_logs_dir,
                                "{}.log".format(new_node_id))
        try:
            os.remove(log_path)
        except OSError:
            pass
        os.symlink(node.pg_log_name, log_path)

        return node, new_node_id

    # exclude node from the cluster
    def rm_node(self, node_id):
        self.safe_psql('select shardman.rm_node({}, force => true);'
                       .format(node_id))
        del self.workers_dict[node_id]

    # find keys belonging to given node
    def get_keys_for_node(self, table_name, node_id, keyname = 'id'):
        with self.connect() as con:
            numparts = int(con.execute(
                "select partitions_count from shardman.tables where relation = '{}';"
                .format(table_name))[0][0])
            parts_tups = con.execute(
                "select part_name from shardman.partitions where relation = '{}' and node_id = {};"
                .format(table_name, node_id))
        # parts lying on this node
        parts = [part_tup[0] for part_tup in parts_tups]
        sql = "select {} from {} ".format(keyname, parts[0])
        for p in parts[1:]:
            sql += "union select {} from {} ".format(keyname, p)
        sql += ";"
        keys = self.workers_dict[node_id].execute(sql)
        keys = [k[0] for k in keys]
        return keys

    # return random worker
    def any_worker(self):
        return next(iter(self.workers_dict.values()))


def sum_query(rel):
    return "select sum(payload) from {}".format(rel)


class ShardmanTests(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        # shardman requires unique sys ids
        TestgresConfig.cache_initdb = True
        TestgresConfig.cached_initdb_unique = True

    @classmethod
    def tearDownClass(self):
        pass

    # utility methods

    # check that every node sees the whole table
    def pt_everyone_sees_the_whole(self, lord):
        luke_sum = int(lord.any_worker().execute(sum_query("pt"))[0][0])
        for _, worker in lord.workers_dict.items():
            worker_sum = int(worker.execute(sum_query("pt"))[0][0])
            self.assertEqual(luke_sum, worker_sum)

    # check every replica integrity
    def pt_replicas_integrity(self, lord, required_replicas=None):
        parts = lord.execute(
            "select part_name, node_id from shardman.partitions")
        for part_name, node_id in parts:
            part_sum = lord.workers_dict[int(node_id)].execute(
                sum_query(part_name))
            replicas = lord.execute(
                "select node_id from shardman.replicas where part_name = '{}'"
                .format(part_name))
            # exactly required_replicas replicas?
            if required_replicas:
                self.assertEqual(required_replicas, len(replicas))
            for replica in replicas:
                replica_id = int(replica[0])
                replica_sum = lord.workers_dict[replica_id].execute(
                    sum_query(part_name))
                self.assertEqual(part_sum, replica_sum)

    # tests

    def test_add_node(self):
        with Shardlord() as lord:
            lord.add_node(repl_group="bananas")
            lord.add_node(repl_group="bananas")
            yellow_fellow, _ = lord.add_node(repl_group="mangos")
            self.assertEqual(
                2,
                len(
                    lord.execute(
                        "select * from shardman.nodes where replication_group = '{}'"
                        .format("bananas"))))
            self.assertEqual(
                1,
                len(
                    lord.execute(
                        "select * from shardman.nodes where replication_group = '{}'"
                        .format("mangos"))))

            # without specifying replication group, rg must be equal to sys id
            _, not_fruit_id = lord.add_node()
            sys_id_and_rep_group = lord.execute(
                "select system_id, replication_group from shardman.nodes where id={}".
                format(not_fruit_id))[0]
            self.assertEqual(sys_id_and_rep_group[0], int(sys_id_and_rep_group[1]))

            # try to add existing node
            with self.assertRaises(testgres.QueryException) as cm:
                # add some spaces to cheat that we have another connstring
                conn_string = common_conn_string(yellow_fellow.port) + "  "
                add_node_cmd = "select shardman.add_node('{}', repl_group => '{}')" \
                    .format(conn_string, "bananas")
                lord.safe_psql(add_node_cmd)
                self.assertIn("is already in the cluster", str(cm.exception))

    def test_get_my_id(self):
        with Shardlord() as lord:
            yellow_fellow, _ = lord.add_node(repl_group="bananas")
            self.assertTrue(
                int(
                    yellow_fellow.execute("select shardman.get_my_id()")[0]
                    [0]) > 0)

    def test_hash_shard_table_and_rm_table(self):
        with Shardlord() as lord:
            lord.safe_psql(
                'create table pt(id int primary key, payload int) partition by hash(id);')
            # try to shard table without any workers
            with self.assertRaises(testgres.QueryException) as cm:
                lord.safe_psql("""
                select shardman.hash_shard_table('pt', 30,
                redundancy => 1);
                """)
                self.assertIn("add some nodes first", str(cm.exception))

        # shard some table, make sure everyone sees it and replicas are good
        with Shardlord(num_repgroups=3, nodes_in_repgroup=2) as lord:
            lord.safe_psql(
                'create table pt(id int primary key, payload int) partition by hash(id);')

            lord.safe_psql(
                "select shardman.hash_shard_table('pt', 30, redundancy => 1);"
            )

            lord.any_worker().safe_psql(
                "insert into pt select generate_series(1, 1000), (random() * 100)::int"
            )

            self.pt_everyone_sees_the_whole(lord)
            self.pt_replicas_integrity(lord)

            # now rm table
            lord.safe_psql("select shardman.rm_table('pt')")
            for _, worker in lord.workers_dict.items():
                ptrels = worker.execute(
                    "select relname from pg_class where relname ~ '^pt.*';")
                self.assertEqual(len(ptrels), 0)

                # now request too many replicas
                with self.assertRaises(testgres.QueryException) as cm:
                    lord.safe_psql("""
                    select shardman.hash_shard_table('pt', 30,
                    redundancy => 2);
                    """)
                    self.assertIn("redundancy 2 is too high", str(cm.exception))

    def test_set_redundancy(self):
        with Shardlord(num_repgroups=2, nodes_in_repgroup=3) as lord:
            lord.safe_psql(
                'create table pt(id int primary key, payload int) partition by hash(id);')
            # shard table without any replicas
            lord.safe_psql(
                "select shardman.hash_shard_table('pt', 30);")
            lord.any_worker().safe_psql(
                "insert into pt select generate_series(1, 1000), (random() * 100)::int"
            )
            self.assertEqual(
                0,
                int(
                    lord.execute(
                        "select count(*) from shardman.replicas;")[0][0]))

            # now add two replicas to each part (2 replicas need changelog table
            # creation, better to go through that code path too)
            lord.safe_psql("select shardman.set_redundancy('pt', 2);")
            # wait for sync
            lord.safe_psql("select shardman.ensure_redundancy();")
            # and ensure their integrity
            # must be exactly two replicas for each partition
            self.pt_replicas_integrity(lord, required_replicas=2)

    def test_rebalance(self):
        with Shardlord(num_repgroups=2, nodes_in_repgroup=2) as lord:
            lord.safe_psql(
                "create table pt(id int primary key, payload int) partition by hash(id);")
            lord.safe_psql(
                "select shardman.hash_shard_table('pt', 30, redundancy => 1)"
            )
            lord.any_worker().safe_psql(
                "insert into pt select generate_series(1, 1000), (random() * 100)::int"
            )

            # now add new node to each repgroup
            lord.add_node(repl_group="rg_0")
            lord.add_node(repl_group="rg_1")

            # rebalance parts and replicas
            lord.safe_psql("select shardman.rebalance('pt%')")
            lord.safe_psql("select shardman.rebalance_replicas('pt%')")

            # make sure partitions are balanced
            for rg in ["rg_0", "rg_1"]:
                # max parts on node - min parts on node
                diff = int(
                    lord.execute("""
                    with parts_count as (
                    select count(*) from shardman.partitions parts
                    join shardman.nodes nodes on parts.node_id = nodes.id
                    where nodes.replication_group = '{}'
                    group by node_id
                    )
                    select max(count) - min(count) from parts_count;
                    """.format(rg))[0][0])
                self.assertTrue(diff <= 1)

                diff_replicas = int(
                    lord.execute("""
                    with replicas_count as (
                    select count(*) from shardman.replicas replicas
                    join shardman.nodes nodes on replicas.node_id = nodes.id
                    where nodes.replication_group = '{}'
                    group by node_id
                    )
                    select max(count) - min(count) from replicas_count;
                    """.format(rg))[0][0])
                self.assertTrue(diff_replicas <= 1)

                # and data is consistent
                self.pt_everyone_sees_the_whole(lord)
                self.pt_replicas_integrity(lord)

    # perform basic steps: add nodes, shard table, rebalance it and rm table
    # with non-super user.
    def test_non_super_user(self):
        os.environ["PGPASSWORD"] = "12345"
        with Shardlord(num_repgroups=3, nodes_in_repgroup=2,
                       worker_creation_cbk=non_super_user_cbk) as lord:

            # shard some table, make sure everyone sees it and replicas are good
            lord.safe_psql(
                'create table pt(id int primary key, payload int) partition by hash(id);')
            lord.safe_psql(
                "select shardman.hash_shard_table('pt', 12, redundancy => 1);"
            )
            lord.any_worker().safe_psql(
                "insert into pt select generate_series(1, 1000), (random() * 100)::int",
                username='joe')

            # everyone sees the whole
            luke_sum = int(lord.any_worker().execute(
                sum_query("pt"), username='joe')[0][0])
            for _, worker in lord.workers_dict.items():
                worker_sum = int(
                    worker.execute(sum_query("pt"), username='joe')[0][0])
                self.assertEqual(luke_sum, worker_sum)

            # replicas integrity
            with lord.connect() as lord_con:
                parts = lord_con.execute(
                    "select part_name, node_id from shardman.partitions;")
                for part_name, node_id in parts:
                    part_sum = lord.workers_dict[int(node_id)].execute(
                        sum_query(part_name), username='joe')
                    replicas = lord_con.execute(
                        "select node_id from shardman.replicas where part_name = '{}';"
                        .format(part_name))
                    for replica in replicas:
                        replica_id = int(replica[0])
                        replica_sum = lord.workers_dict[replica_id].execute(
                            sum_query(part_name), username='joe')
                        self.assertEqual(part_sum, replica_sum)

            # now rm table
            lord.safe_psql("select shardman.rm_table('pt')")
            for _, worker in lord.workers_dict.items():
                ptrels = worker.execute(
                    "select relname from pg_class where relname ~ '^pt.*';")
                self.assertEqual(len(ptrels), 0)

    # wipe_state everywhere and make sure recover() fixes things
    def test_recover_basic(self):
        with Shardlord(num_repgroups=2, nodes_in_repgroup=3) as lord:
            lord.safe_psql(
                'create table pt(id int primary key, payload int default 1) partition by hash(id);')
            lord.safe_psql(
                "select shardman.hash_shard_table('pt', 30, redundancy => 2);"
            )
            lord.any_worker().safe_psql(
                "insert into pt select generate_series(1, 10000), (random() * 100)::int"
            )

            # now accidently remove state from everywhere
            for _, worker in lord.workers_dict.items():
                worker.safe_psql(
                    "set local synchronous_commit to local; select shardman.wipe_state();"
                )
            # repair things back
            lord.safe_psql("select shardman.recover()")

            # write some more data
            lord.any_worker().safe_psql(
                "insert into pt select generate_series(10001, 20000), (random() * 100)::int"
            )

            # and make sure data is still consistent
            self.pt_everyone_sees_the_whole(lord)
            self.pt_replicas_integrity(lord)

    # Move part between two nodes with the 3rd node killed. Insert some data,
    # remember sum. Then run recover and make sure data is consistent.
    def test_mv_partition_with_offline_node(self):
        with Shardlord() as lord:
            src, src_id = lord.add_node()
            dst, dst_id = lord.add_node()
            watcher, watcher_id = lord.add_node()

            lord.safe_psql(
                'create table pt(id int primary key, payload int) partition by hash(id);')
            lord.safe_psql(
                "select shardman.hash_shard_table('pt', 5);")
            src.safe_psql(
                "insert into pt select generate_series(1, 1000), (random() * 100)::int"
            )
            luke_sum_before_move = int(src.execute(sum_query("pt"))[0][0])

            part_to_move = lord.execute(
                "select part_name from shardman.partitions where node_id = {};"
                .format(src_id))[0][0]
            watcher.stop()  # shut down watcher
            ret, out, err = lord.psql(
                "select shardman.mv_partition('{}', {})"
                .format(part_to_move, dst_id))
            self.assertTrue(ret == 0, err.decode())
            self.assertTrue(b'FDW mappings update failed on some nodes' in err)

            # Insert some more data. This will fail for keys belonging to watcher,
            # so insert one-by-one and ignore 'could not connect' error
            with src.connect() as con:
                for key in range(1001, 1200):
                    try:
                        con.cursor.execute(
                            "insert into pt values ({}, (random() * 100)::int);".
                            format(key))
                    except Exception as e:
                        if "could not connect to server" not in str(e):
                            raise e
                    finally:
                        con.connection.commit()
            watcher.start()  # get watcher back online
            luke_sum = int(src.execute(sum_query("pt"))[0][0])
            # make sure we actually inserted something
            self.assertTrue(luke_sum > luke_sum_before_move,
                            msg="luke_sum is {}, luke_sum_before_move is {}"
                            .format(luke_sum, luke_sum_before_move))
            # and another node confirms that
            self.assertTrue(
                luke_sum == int(dst.execute(sum_query("pt"))[0][0]))

            lord.safe_psql(
                "select shardman.recover()")  # let it know about mv
            # everything is visible now from anyone, including keys on failed node
            luke_sum = int(src.execute(sum_query("pt"))[0][0])
            for worker in [dst, watcher]:
                worker_sum = int(worker.execute(sum_query("pt"))[0][0])
                self.assertEqual(luke_sum, worker_sum)

    # Just rm_node failed worker with all other nodes online and make sure
    # data is not lost
    def test_worker_failover_basic(self):
        with Shardlord(num_repgroups=2, nodes_in_repgroup=2) as lord:
            lord.safe_psql(
                'create table pt(id int primary key, payload int default 1) partition by hash(id);')
            lord.safe_psql(
                "select shardman.hash_shard_table('pt', 30, redundancy => 1);"
            )
            lord.any_worker().safe_psql(
                "insert into pt select generate_series(1, 10000), (random() * 100)::int"
            )

            # turn off some node
            failed_node_id = 2
            lord.rm_node(failed_node_id)

            # now there should be only 3 nodes
            self.assertTrue(3 == int(
                lord.execute(
                    'select count(*) from shardman.nodes;')[0][0]))

            self.pt_everyone_sees_the_whole(lord)
            self.pt_replicas_integrity(lord)

    # rm_node failed worker while another node from this replication group
    # is offline; turn it on, run recovery and make sure data is fine
    def test_worker_failover_with_offline_neighbour(self):
        with Shardlord() as lord:
            yellow_fellow, yf_id = lord.add_node(repl_group="bananas")
            green_fellow, gf_id = lord.add_node(repl_group="bananas")
            # second replica is necessary to test sync of replicas and LR update
            # code executed during promotion
            lord.add_node(repl_group="bananas")
            lord.add_node(repl_group="mangos")
            lord.add_node(repl_group="mangos")
            lord.add_node(repl_group="mangos")
            lord.safe_psql(
                'create table pt(id int primary key, payload int default 1) partition by hash(id);')
            # num of parts must be at least equal to num of nodes, or removed node
            # probably won't get single partition
            lord.safe_psql(
                "select shardman.hash_shard_table('pt', 6, redundancy => 2);"
            )
            yellow_fellow.safe_psql(
                "insert into pt select generate_series(1, 10000), (random() * 100)::int"
            )

            yellow_fellow.stop()
            green_fellow.stop()
            lord.rm_node(yf_id)

            green_fellow.start()
            lord.safe_psql("select shardman.recover()")

            self.pt_everyone_sees_the_whole(lord)
            self.pt_replicas_integrity(lord)

    # dummiest test with no actual 2PC
    def test_recover_xacts_no_xacts(self):
        with Shardlord(num_repgroups=2, nodes_in_repgroup=2) as lord:
            lord.safe_psql("select shardman.recover_xacts()")

    # 2PC sanity check
    def test_twophase_sanity(self):
        with Shardlord() as lord:
            # Now 2PC is only enabled with global snaps
            additional_worker_conf = (
                "track_global_snapshots = true\n"
                "postgres_fdw.use_tsdtm = true\n"
                "global_snapshot_defer_time = 30\n"
            )
            apple, _ = lord.add_node(additional_conf=additional_worker_conf)
            mango, _ = lord.add_node(additional_conf=additional_worker_conf)
            watermelon, _ = lord.add_node(additional_conf=additional_worker_conf)

            lord.safe_psql(
                'create table pt(id int primary key, payload int) partition by hash(id);')
            lord.safe_psql(
                "select shardman.hash_shard_table('pt', 4)")
            lord.any_worker().safe_psql(
                "insert into pt select generate_series(1, 100), 0")

            apple_key = lord.get_keys_for_node("pt", 1)[0]
            mango_key = lord.get_keys_for_node("pt", 2)[0]
            watermelon_key = lord.get_keys_for_node("pt", 3)[0]

            with apple.connect() as con:
                # xact 1 started
                con.begin()
                con.execute("update pt set payload = 42 where id = {};"
                            .format(apple_key))
                con.execute("update pt set payload = -20 where id = {};"
                            .format(mango_key))
                con.execute("update pt set payload = -22 where id = {};"
                            .format(watermelon_key))
                # stop one node, let's hope coordinator will start committing not
                # from it (otherwise this test is almost useless)
                mango.stop()
                # commit the xact, ignoring error
                try:
                    con.commit()
                except Exception as e:
                    pass

            # get the node back
            mango.start()
            # resolve prepared xacts
            lord.safe_psql("select shardman.recover_xacts()")
            # and check the balance
            balance = int(mango.execute("select sum(payload) from pt;")[0][0])
            self.assertTrue(balance == 0, msg=('balance={}'.format(balance)))

    # test that monitor removes failed node
    def test_monitor_rm_node(self):
        with Shardlord(num_repgroups=2, nodes_in_repgroup=2) as lord:
            lord.safe_psql(
                'create table pt(id int primary key, payload int default 1) partition by hash(id);')
            lord.safe_psql(
                "select shardman.hash_shard_table('pt', 30, redundancy => 1);"
            )
            lord.any_worker().safe_psql(
                "insert into pt select generate_series(1, 10000), (random() * 100)::int"
            )

            # turn off some node
            lord.any_worker().stop()

            # let monitor remove it
            try:
                with lord.connect() as con:
                    con.execute("set statement_timeout = '3s'")
                    con.execute(
                        "select shardman.monitor(check_timeout_sec => 1, rm_node_timeout_sec => 1)"
                    )
            except ProgrammingError as e:
                # make sure it was statement_timeout
                self.assertEqual(e.args[2], STMT_TIMEOUT_ERRCODE)

            # now there should be only 3 nodes
            self.assertTrue(3 == int(
                lord.execute('select count(*) from shardman.nodes;')[
                    0][0]))

    def test_deadlock_detector(self):
        with Shardlord(num_repgroups=2) as lord:
            lord.safe_psql(
                'create table pt(id int primary key, payload int) partition by hash(id);')
            lord.safe_psql(
                "select shardman.hash_shard_table('pt', 2)")
            lord.any_worker().safe_psql(
                "insert into pt select generate_series(1, 100), 0")

            # take parts & keys from node 1 and node 2 to work with
            node_1, node_2 = lord.workers_dict[1], lord.workers_dict[2]
            node_1_key = lord.get_keys_for_node("pt", 1)[0]
            node_2_key = lord.get_keys_for_node("pt", 2)[0]
            # Induce deadlock. It would be much better to use async db connections,
            # but pg8000 doesn't support them, and we generally aim at portability
            def xact_1():
                with node_1.connect() as con:
                    con.begin()
                    con.execute("update pt set payload = 42 where id = {}"
                                .format(node_1_key))
                    barrier.wait()
                    try:
                        con.execute("update pt set payload = 43 where id = {}"
                                    .format(node_2_key))
                    except ProgrammingError as e:
                        # make sure it was statement_timeout
                        if e.args[2] == STMT_TIMEOUT_ERRCODE:
                            global xact_1_aborted
                            xact_1_aborted = True

            def xact_2():
                with node_2.connect() as con:
                    con.begin()
                    con.execute("update pt set payload = 42 where id = {}"
                                .format(node_2_key))
                    barrier.wait()
                    try:
                        con.execute("update pt set payload = 43 where id = {}"
                                    .format(node_1_key))
                    except ProgrammingError as e:
                        # make sure it was statement_timeout
                        if e.args[2] == STMT_TIMEOUT_ERRCODE:
                            global xact_2_aborted
                            xact_2_aborted = True

            barrier = threading.Barrier(2)
            global xact_1_aborted
            global xact_2_aborted
            xact_1_aborted, xact_2_aborted = False, False
            t1 = threading.Thread(target=xact_1, args=())
            t1.start()
            t2 = threading.Thread(target=xact_2, args=())
            t2.start()
            # monitor for some time
            try:
                with lord.connect() as con:
                    con.execute("set statement_timeout = '10s'")
                    con.execute("select shardman.monitor(check_timeout_sec => 1)")
            except Exception as e:
                pass
            t1.join(10)
            self.assertTrue(not t1.is_alive())
            t2.join(10)
            self.assertTrue(not t2.is_alive())
            self.assertTrue(xact_1_aborted or xact_2_aborted)

    # global snapshot sanity check, simple bank transfer. Without global
    # snapshot, read skew will arise.
    def test_global_snapshot(self):
        additional_worker_conf = (
            "track_global_snapshots = true\n"
            "postgres_fdw.use_tsdtm = true\n"
            "global_snapshot_defer_time = 30\n"
            "default_transaction_isolation = 'repeatable read'\n"
            "log_min_messages = DEBUG3\n"
            "shared_preload_libraries = 'pg_shardman, postgres_fdw'\n"
        )
        with Shardlord() as lord:
            apple, apple_id = lord.add_node(additional_conf=additional_worker_conf)
            mango, mango_id = lord.add_node(additional_conf=additional_worker_conf)

            lord.safe_psql(
                "create table pt(id int primary key, payload int) partition by hash(id);")
            lord.safe_psql(
                "select shardman.hash_shard_table('pt', 4)")
            lord.any_worker().safe_psql(
                "insert into pt select generate_series(1, 200), 0")

            apple_key = lord.get_keys_for_node("pt", apple_id)[0]
            mango_key = lord.get_keys_for_node("pt", mango_id)[0]
            # print("apple_key is {}, mango key is {}".format(apple_key, mango_key));

            with apple.connect() as balance_con, mango.connect() as transfer_con:
                # xact 1 started
                balance_con.begin(isolation_level=IsolationLevel.RepeatableRead)
                apple_balance = int(balance_con.execute("select payload from pt where id = {}"
                                                        .format(apple_key))[0][0])
                # and only then xact 2 started, so xact 1 must not see xact 2 effects
                transfer_con.begin(isolation_level=IsolationLevel.RepeatableRead)
                transfer_con.execute("update pt set payload = 42 where id = {}".format(mango_key))
                # Though we already read apple_key, we must do that.
                transfer_con.execute("update pt set payload = -42 where id = {}".format(apple_key))
                transfer_con.commit()
                mango_balance = int(balance_con.execute("select payload from pt where id = {}"
                                                        .format(mango_key))[0][0])

            balances = 'apple balance is {}, mango balance is {}'.format(apple_balance, mango_balance)
            self.assertTrue(apple_balance + mango_balance == 0, balances)

    # Same simple bank transfer, but connect from third-party observer
    def test_global_snapshot_external(self):
        additional_worker_conf = (
            "track_global_snapshots = true\n"
            "postgres_fdw.use_tsdtm = true\n"
            "global_snapshot_defer_time = 30\n"
            "default_transaction_isolation = 'repeatable read'\n"
            "log_min_messages = DEBUG3\n"
            "shared_preload_libraries = 'pg_shardman, postgres_fdw'\n"
        )
        with Shardlord() as lord:
            apple, _ = lord.add_node(additional_conf=additional_worker_conf)
            mango, _ = lord.add_node(additional_conf=additional_worker_conf)
            watermelon, _ = lord.add_node(additional_conf=additional_worker_conf)

            lord.safe_psql(
                'create table pt(id int primary key, payload int) partition by hash(id);')
            lord.safe_psql(
                "select shardman.hash_shard_table('pt', 4)")

            lord.any_worker().safe_psql(
                "insert into pt select generate_series(1, 200), 0")

            apple_key = lord.get_keys_for_node("pt", 1)[0]
            mango_key = lord.get_keys_for_node("pt", 1)[0]
            # print('apple_key is {}, mango key is {}'.format(apple_key, mango_key))
            # print('inserted keys are {}'.format(watermelon.execute('select id from pt order by id;')))

            with watermelon.connect() as balance_con, watermelon.connect() as transfer_con:
                # xact 1 started
                balance_con.begin(isolation_level=IsolationLevel.RepeatableRead)
                apple_balance = int(balance_con.execute("select payload from pt where id = {};"
                                                        .format(apple_key))[0][0])
                # and only then xact 2 started, so xact 1 must not see xact 2 effects
                transfer_con.begin(isolation_level=IsolationLevel.RepeatableRead)
                transfer_con.execute("update pt set payload = 42 where id = {};".format(mango_key))
                # Though we already read apple_key, we must do that.
                transfer_con.execute("update pt set payload = -42 where id = {};".format(apple_key))
                transfer_con.commit()
                mango_balance = int(balance_con.execute("select payload from pt where id = {};"
                                                        .format(mango_key))[0][0])
                balance_con.commit()
            balances = 'apple balance is {}, mango balance is {}'.format(apple_balance, mango_balance)
            self.assertTrue(apple_balance + mango_balance == 0, balances)

    # This is not very useful anymore since PG supports COPY FROM natively...
    def test_copy_from(self):
        with Shardlord(num_repgroups=3, nodes_in_repgroup=2) as lord:
            lord.safe_psql(
                'create table pt(id int primary key, payload int default 1) partition by hash(id);')
            lord.safe_psql(
                "select shardman.hash_shard_table('pt', 30, redundancy => 1);"
            )

            lord.safe_psql(
                'create table pt_text(id int primary key, payload text) partition by hash(id);')
            lord.safe_psql(
                "select shardman.hash_shard_table('pt_text', 30, redundancy => 1);"
            )

            # copy some data on one node
            lord.workers_dict[1].safe_psql(
                'copy pt from stdin;',
                input=b"""1	2
2	3
4	5
6	7
8	9
10	10
\.
""")
            # and make sure another sees it
            res_sum = int(lord.workers_dict[2].execute(sum_query("pt"))[0][0])
            self.assertEqual(res_sum, 36)
            lord.workers_dict[1].safe_psql('delete from pt;')

            # specify column
            lord.workers_dict[1].safe_psql(
                'copy pt (id) from stdin;',
                input=b"""1
2
3
\.
""")
            res_sum = int(lord.workers_dict[2].execute(sum_query('pt'))[0][0])
            self.assertEqual(res_sum, 3)
            lord.workers_dict[1].safe_psql('delete from pt;')

            # csv
            lord.workers_dict[1].safe_psql(
                'copy pt from stdin (format csv);',
                input=b"""1,2
3,4
5,6""")
            res_sum = int(lord.workers_dict[2].execute(sum_query('pt'))[0][0])
            self.assertEqual(res_sum, 12)
            lord.workers_dict[1].safe_psql('delete from pt;')

            # binary
            # generate binary data good for this platform
            data = lord.workers_dict[1].safe_psql(
                """create table pt_local (id int, payload int);
                insert into pt_local values (1, 2);
                insert into pt_local values (3, 4);
                copy pt_local to stdout (format binary);
                drop table pt_local;
                """)
            lord.workers_dict[1].safe_psql(
                'copy pt from stdin (format binary);', input=data)
            res_sum = int(lord.workers_dict[2].execute(sum_query('pt'))[0][0])
            self.assertEqual(res_sum, 6)
            lord.workers_dict[1].safe_psql('delete from pt;')

            # freeze off
            lord.workers_dict[1].safe_psql(
                'copy pt from stdin (format csv, freeze false);',
                input=b"""1,2
3,4
5,6""")
            res_sum = int(lord.workers_dict[2].execute(sum_query('pt'))[0][0])
            self.assertEqual(res_sum, 12)
            lord.workers_dict[1].safe_psql('delete from pt;')

            # freeze on
            with self.assertRaises(
                    testgres.QueryException, msg="freeze not supported") as cm:
                lord.workers_dict[1].safe_psql(
                    'copy pt from stdin (format csv, freeze true);',
                    input=b"""1,2
3,4
5,6""")
                self.assertIn("freeze is not supported", str(cm.exception))

            # delimiter
            lord.workers_dict[1].safe_psql(
                "copy pt from stdin (format csv, delimiter '|');",
                input=b"""1|2
3|4
5|6""")
            res_sum = int(lord.workers_dict[2].execute(sum_query('pt'))[0][0])
            self.assertEqual(res_sum, 12)
            lord.workers_dict[1].safe_psql('delete from pt;')

            # null
            lord.workers_dict[1].safe_psql(
                "copy pt FROM stdin (format csv, null '44');",
                input=b"""1,2
3,44
5,6""")
            res_sum = int(lord.workers_dict[2].execute(sum_query('pt'))[0][0])
            self.assertEqual(res_sum, 8)
            lord.workers_dict[1].safe_psql('delete from pt;')

            # header
            lord.workers_dict[1].safe_psql(
                "copy pt FROM stdin (format csv, header true);",
                input=b"""hoho,hehe
3,4
5, 6""")
            res_sum = int(lord.workers_dict[2].execute(sum_query('pt'))[0][0])
            self.assertEqual(res_sum, 10)
            lord.workers_dict[1].safe_psql('delete from pt;')

            # quote
            lord.workers_dict[1].safe_psql(
                "copy pt FROM stdin (format csv, QUOTE '^');",
                input=b"""1,2
3,^4^
5, 6""")
            res_sum = int(lord.workers_dict[2].execute(sum_query('pt'))[0][0])
            self.assertEqual(res_sum, 12)
            lord.workers_dict[1].safe_psql('delete from pt;')

            # escape
            lord.workers_dict[1].safe_psql(
                "copy pt FROM stdin (format csv, quote '4', escape '@');",
                input=b"""1,2
3,4@44
5, 6""")
            res_sum = int(lord.workers_dict[2].execute(sum_query('pt'))[0][0])
            self.assertEqual(res_sum, 12)
            lord.workers_dict[1].safe_psql('delete from pt;')

            # FORCE_NOT_NULL
            lord.workers_dict[1].safe_psql(
                "copy pt FROM stdin (format csv, null '44', force_not_null (payload));",
                input=b"""1,2
3,44
5,6""")
            res_sum = int(lord.workers_dict[2].execute(sum_query('pt'))[0][0])
            self.assertEqual(res_sum, 52)
            lord.workers_dict[1].safe_psql('delete from pt;')

            # FORCE_NULL
            lord.workers_dict[1].safe_psql(
                "copy pt FROM stdin (format csv, quote '^', null '44', force_null (payload));",
                input=b"""1,2
3,^44^
5,6""")
            res_sum = int(lord.workers_dict[2].execute(sum_query('pt'))[0][0])
            self.assertEqual(res_sum, 8)
            lord.workers_dict[1].safe_psql('delete from pt;')

            # Encoding. We should probably test workers with different server
            # encodings...
            lord.workers_dict[1].safe_psql(
                "copy pt_text from stdin (format csv, encoding 'KOI8R')",
                input="""1,йожин
3,с
5,бажин""".encode('koi8_r'))
            self.assertEqual("йожинсбажин", lord.workers_dict[2].execute(
                "select string_agg(payload, '' order by id) from pt_text;")[0][0])

    # add column
    def test_alter_table_add_column(self):
        with Shardlord(num_repgroups=2, nodes_in_repgroup=2) as lord:
            lord.safe_psql(
                'create table pt(id int primary key, payload int default 1) partition by hash(id);')
            lord.safe_psql(
                "select shardman.hash_shard_table('pt', 4, redundancy => 1);"
            )
            lord.workers_dict[1].safe_psql(
                "insert into pt select generate_series(1, 100), 0")
            # add column
            lord.safe_psql(
                "select shardman.alter_table('pt', 'add column added_col int');"
            )
            # insert data into it
            lord.workers_dict[1].safe_psql(
                "update pt set added_col = floor(random() * (10));"
            )
            # remember sum
            res_sum = int(lord.workers_dict[1].execute(
                'select sum(added_col) from pt;')[0][0])
            # now rm some node to ensure that replicas got this column too
            lord.rm_node(1)
            # and make sure sum is still the same
            sum_after_rm = int(lord.workers_dict[2].execute(
                'select sum(added_col) from pt;')[0][0])
            self.assertEqual(res_sum, sum_after_rm)


# Create user joe for accessing data; configure pg_hba accordingly.
# Unfortunately, we must use password, because postgres_fdw forbids passwordless
# access for non-superusers
def non_super_user_cbk(worker):
    worker.safe_psql("""
                     set synchronous_commit to local;
                     drop role if exists joe;
                     create role joe login password '12345';
                     """)
    worker.stop()
    hba_conf_path = os.path.join(worker.data_dir, "pg_hba.conf")
    with open(hba_conf_path, "w") as hba_conf_file:
        # yapf: disable
        pg_hba = [
            "local\tall\tjoe\tpassword\n",
            "local\tall\tall\ttrust\n",
            "host\tall\tall\t127.0.0.1/32\ttrust\n",
            "local\treplication\tall\ttrust\n",
            "host\treplication\tall\t127.0.0.1/32\ttrust\n"
            "host\treplication\tall\t::1/128\ttrust\n"
        ]
        hba_conf_file.writelines(pg_hba)
    worker.start()
    conn_string = common_conn_string(worker.port) + " user=joe password=12345"
    return conn_string


# We violate good practices and order the tests -- it doesn't make sense to
# e.g. test copy_from if add_node doesn't work.
def suite():
    suite = unittest.TestSuite()
    suite.addTest(ShardmanTests('test_add_node'))
    suite.addTest(ShardmanTests('test_get_my_id'))
    suite.addTest(ShardmanTests('test_hash_shard_table_and_rm_table'))
    suite.addTest(ShardmanTests('test_set_redundancy'))
    suite.addTest(ShardmanTests('test_rebalance'))
    suite.addTest(ShardmanTests('test_non_super_user'))
    suite.addTest(ShardmanTests('test_recover_basic'))
    suite.addTest(ShardmanTests('test_mv_partition_with_offline_node'))
    suite.addTest(ShardmanTests('test_worker_failover_basic'))
    suite.addTest(ShardmanTests('test_worker_failover_with_offline_neighbour'))
    suite.addTest(ShardmanTests('test_recover_xacts_no_xacts'))
    suite.addTest(ShardmanTests('test_twophase_sanity'))
    suite.addTest(ShardmanTests('test_monitor_rm_node'))
    suite.addTest(ShardmanTests('test_deadlock_detector'))
    suite.addTest(ShardmanTests('test_global_snapshot'))
    suite.addTest(ShardmanTests('test_global_snapshot_external'))
    suite.addTest(ShardmanTests('test_copy_from'))
    suite.addTest(ShardmanTests('test_alter_table_add_column'))

    return suite

if __name__ == "__main__":
    runner = unittest.TextTestRunner(verbosity=2, failfast=True)
    runner.run(suite())

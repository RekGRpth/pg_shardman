#!/usr/bin/env python3
# coding: utf-8

"""
 tests.py
	pg_shardman tests.

	Copyright (c) 2017, Postgres Professional
"""

import unittest
import logging
import os
import threading
from time import sleep
from contextlib import contextmanager

import testgres

from testgres import PostgresNode, TestgresConfig, NodeStatus

DBNAME = "postgres"
NODE_NAMES = ["Luke", "C3PO", "Palpatine", "DarthMaul", "jabba", "bobafett"]

class Shardlord(PostgresNode):
    def __init__(self, name, port=None):
        # worker_id (int) -> PostgresNode
        self.workers_dict = {}
        # list of allocated, but currently not used workers
        self.reserved_workers = []
        # next port to allocate in debug mode
        self.next_port = 5432
        # next name to allocate in NODE_NAMES
        self.next_name_idx = 0

        super(Shardlord, self).__init__(name=name,
                                        port = self.get_next_port(),
                                        use_logging=True)
        super(Shardlord, self).init()

    @staticmethod
    def _common_conn_string(port):
        return "dbname={} port={}".format(DBNAME, port)

    def _shardlord_connstring(self):
        return self._common_conn_string(self.port)

    def _common_conf_lines(self):
        return (
            """shared_preload_libraries = 'pg_shardman, pg_pathman'
            shardman.shardlord_connstring = '{}'

            shared_buffers = 512MB
            max_connections = 1000
            max_wal_size = 5GB

            log_min_messages = DEBUG1
            client_min_messages = NOTICE
            log_replication_commands = on
            TimeZone = 'Europe/Moscow'
            log_timezone = 'Europe/Moscow'
            """
        ).format(self._shardlord_connstring())

    # reset conf and start clean shardlord
    def start_lord(self):
        if self.status() == NodeStatus.Running:
            self.stop()
        self.default_conf()
        config_lines = self._common_conf_lines()
        config_lines += (
            "shardman.shardlord = on\n"
            "shardman.shardlord_dbname = {}\n"
            "shardman.sync_replication = on\n"
        ).format(DBNAME)
        self.append_conf("postgresql.conf", config_lines)
        super(Shardlord, self).start()
        self.safe_psql(
            DBNAME, "drop extension if exists pg_shardman; create extension pg_shardman cascade")

        return self

    # create fresh cluster with given num of repgroups and nodes in each one
    def create_cluster(self, num_repgroups, nodes_in_repgroup=1):
        self.destroy_cluster()
        self.start_lord()
        for rgnum in range(num_repgroups):
            for nodenum in range(nodes_in_repgroup):
                self.add_node(repl_group="rg_{}".format(rgnum))

    # destroy and shutdown everything, but keep nodes
    def destroy_cluster(self):
        for worker_id, worker in self.workers_dict.items():
            if worker.status() == NodeStatus.Running:
                worker.safe_psql(DBNAME,
                                 """
                                 set local synchronous_commit to local;
                                 select shardman.wipe_state();
                                 drop extension pg_shardman cascade;
                                 """)
                worker.stop()
            self.reserved_workers.append(worker)
        self.workers_dict = {}

        if self.status() == NodeStatus.Running:
            self.stop()

    def cleanup(self):
        super(Shardlord, self).cleanup()

        for node in self.reserved_workers:
            node.cleanup()

        for worker_id, worker in self.workers_dict.items():
            worker.cleanup()

        return self

    def get_next_port(self):
        port = self.next_port
        self.next_port += 1
        return port

    # give one of reserved workers or create a new one
    def pop_worker(self):
        if self.reserved_workers:
            node = self.reserved_workers.pop()
            if node.status() == NodeStatus.Running:
                node.safe_psql(DBNAME,
                                 """
                                 set local synchronous_commit to local;
                                 select shardman.wipe_state();
                                 drop extension pg_shardman cascade;
                                 """)
                worker.stop()
        else:
            # time to create a new one

            # Set env var DBG_MODE=1 to bind PG to standard ports
            if os.environ.get('DBG_MODE') == '1':
                port = self.get_next_port()
            else:
                port = None

            try:
                name = NODE_NAMES[self.next_name_idx]
                self.next_name_idx += 1
            except IndexError as e:
                print("Please provide some more funny node names")
                raise e

            node = PostgresNode(name=name, port=port, use_logging=True).init()

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
        return node;

    # Add worker using reserved node, returns node instance, node id pair
    def add_node(self, repl_group=None, additional_conf=""):
        node = self.pop_worker()

        # start this node
        node.append_conf("postgresql.conf", additional_conf) \
            .start() \
            .safe_psql(DBNAME, "drop extension if exists pg_shardman; create extension pg_shardman cascade;")
        # and register this node
        conn_string = self._common_conn_string(node.port)
        add_node_cmd = "select shardman.add_node('{}' {})".format(
            conn_string, ", repl_group => '{}'".format(repl_group) if repl_group
            else '')
        new_node_id = int(self.execute(DBNAME, add_node_cmd)[0][0])
        self.workers_dict[new_node_id] = node

        return node, new_node_id

    @property
    def workers(self):
        return list(self.workers_dict.values())

def sum_query(rel):
    return "select sum(payload) from {}".format(rel)

class ShardmanTests(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        # collect all logs into a single file
        logfile = "/tmp/shmn.log"
        open(logfile, 'w').close()  # truncate log file
        logging.basicConfig(filename=logfile, level=logging.DEBUG)
        lord = Shardlord(name="DarthVader")
        self.lord = lord
        # if we cache initdb, all instances have the same system id
        TestgresConfig.cache_initdb = False

    @classmethod
    def tearDownClass(self):
        TestgresConfig.cache_initdb = True
        self.lord.cleanup()

    # utility methods

    # check that every node sees the whole table
    def pt_everyone_sees_the_whole(self):
        luke_sum = int(self.lord.workers[0].execute(
            DBNAME, sum_query("pt"))[0][0])
        for worker in self.lord.workers[1:]:
            worker_sum = int(worker.execute(
                DBNAME, sum_query("pt"))[0][0])
            self.assertEqual(luke_sum, worker_sum)

    # check every replica integrity
    def pt_replicas_integrity(self, required_replicas=None):
        parts = self.lord.execute(
            DBNAME, "select part_name, node_id from shardman.partitions")
        for part_name, node_id in parts:
            part_sum = self.lord.workers_dict[int(node_id)].execute(
                DBNAME, sum_query(part_name))
            replicas = self.lord.execute(
                DBNAME,
                "select node_id from shardman.replicas where part_name = '{}'" \
                .format(part_name))
            # exactly required_replicas replicas?
            if required_replicas:
                self.assertEqual(required_replicas, len(replicas))
            for replica in replicas:
                replica_id = int(replica[0])
                replica_sum = self.lord.workers_dict[replica_id].execute(
                    DBNAME, sum_query(part_name))
                self.assertEqual(part_sum, replica_sum)

    def pt_cleanup(self):
        self.lord.safe_psql(DBNAME, "select shardman.rm_table('pt')")
        self.lord.safe_psql(DBNAME, "drop table pt;")
        self.lord.destroy_cluster();

    # tests

    def test_add_node(self):
        self.lord.start_lord()
        self.lord.add_node(repl_group="banana")
        self.lord.add_node(repl_group="banana")
        yellow_fellow, _ = self.lord.add_node(repl_group="mango")
        self.assertEqual(2, len(self.lord.execute(
            DBNAME, "select * from shardman.nodes where replication_group = '{}'" \
            .format("banana"))))
        self.assertEqual(1, len(self.lord.execute(
            DBNAME, "select * from shardman.nodes where replication_group = '{}'" \
            .format("mango"))))

        # without specifying replication group, rg must be equal to sys id
        _, not_fruit_id = self.lord.add_node()
        sys_id_and_rep_group = self.lord.execute(
            DBNAME, "select system_id, replication_group from shardman.nodes where id={}".format(not_fruit_id))[0]
        self.assertEqual(sys_id_and_rep_group[0], int(sys_id_and_rep_group[1]))

        # try to add existing node
        with self.assertRaises(testgres.QueryException) as cm:
            # add some spaces to cheat that we have another connstring
            conn_string = self.lord._common_conn_string(yellow_fellow.port) + "  "
            add_node_cmd =  "select shardman.add_node('{}', repl_group => '{}')".format(
                conn_string, "banana")
            self.lord.safe_psql(DBNAME, add_node_cmd)
        self.assertIn("is already in the cluster", str(cm.exception))

        self.lord.destroy_cluster()

    def test_get_my_id(self):
        self.lord.start_lord()
        yellow_fellow, _ = self.lord.add_node(repl_group="banana")
        self.assertTrue(int(
            yellow_fellow.execute(DBNAME,
                                  "select shardman.get_my_id()")[0][0]) > 0)
        self.lord.destroy_cluster()

    def test_rm_node(self):
        self.lord.start_lord()
        self.lord.add_node(repl_group="banana")
        _, yf_id = self.lord.add_node(repl_group="banana")
        self.lord.safe_psql(DBNAME, 'select shardman.rm_node({})'.format(yf_id))
        self.assertEqual(1, len(self.lord.execute(
            DBNAME, "select * from shardman.nodes;")))
        self.lord.destroy_cluster()

    def test_create_hash_partitions_and_rm_table(self):
        self.lord.start_lord()
        self.lord.safe_psql(
            DBNAME, 'create table pt(id int primary key, payload int);')
        # try to shard table without any workers
        with self.assertRaises(testgres.QueryException) as cm:
            self.lord.safe_psql(DBNAME, """
            select shardman.create_hash_partitions('pt', 'id', 30,
            redundancy => 1);
            """)
        self.assertIn("add some nodes first", str(cm.exception))

        # shard some table, make sure everyone sees it and replicas are good
        self.lord.create_cluster(3, 2)
        self.lord.safe_psql(
            DBNAME, "select shardman.create_hash_partitions('pt', 'id', 30, redundancy => 1);")
        self.lord.workers[0].safe_psql(
            DBNAME,
            "insert into pt select generate_series(1, 1000), (random() * 100)::int")

        self.pt_everyone_sees_the_whole()
        self.pt_replicas_integrity()

        # now rm table
        self.lord.safe_psql(DBNAME, "select shardman.rm_table('pt')")
        for worker in self.lord.workers:
            ptrels = worker.execute(
                DBNAME, "select relname from pg_class where relname ~ '^pt.*';")
            self.assertEqual(len(ptrels), 0)

        # now request too many replicas
        with self.assertRaises(testgres.QueryException) as cm:
            self.lord.safe_psql(DBNAME, """
            select shardman.create_hash_partitions('pt', 'id', 30,
            redundancy => 2);
            """)
        self.assertIn("redundancy 2 is too high", str(cm.exception))

        self.lord.safe_psql(DBNAME, "drop table pt;")
        self.lord.destroy_cluster()

    def test_set_redundancy(self):
        self.lord.create_cluster(2, 3)
        self.lord.safe_psql(
            DBNAME, 'create table pt(id int primary key, payload int);')
        # shard table without any replicas
        self.lord.safe_psql(
            DBNAME, "select shardman.create_hash_partitions('pt', 'id', 30);")
        self.lord.workers[0].safe_psql(
            DBNAME,
            "insert into pt select generate_series(1, 1000), (random() * 100)::int")
        self.assertEqual(0, int(self.lord.execute(
            DBNAME, "select count(*) from shardman.replicas;")[0][0]))

        # now add two replicas to each part (2 replicas need changelog table
        # creation, better to go through that code path too)
        self.lord.safe_psql(
            DBNAME, "select shardman.set_redundancy('pt', 2);")
        # wait for sync
        self.lord.safe_psql(DBNAME, "select shardman.ensure_redundancy();")
        # and ensure their integrity
        # must be exactly two replicas for each partition
        self.pt_replicas_integrity(required_replicas=2)

        self.pt_cleanup()

    def test_rebalance(self):
        self.lord.create_cluster(2, 2)
        self.lord.safe_psql(
            DBNAME, 'create table pt(id int primary key, payload int);')
        # shard table
        self.lord.safe_psql(
            DBNAME, "select shardman.create_hash_partitions('pt', 'id', 30, redundancy => 1);")
        self.lord.workers[0].safe_psql(
            DBNAME,
            "insert into pt select generate_series(1, 1000), (random() * 100)::int")

        # now add new node to each repgroup
        self.lord.add_node(repl_group="rg_0")
        self.lord.add_node(repl_group="rg_1")

        # rebalance parts and replicas
        self.lord.safe_psql(DBNAME, "select shardman.rebalance('pt%')")
        self.lord.safe_psql(DBNAME, "select shardman.rebalance_replicas('pt%')")

        # make sure partitions are balanced
        for rg in ["rg_0", "rg_1"]:
            # max parts on node - min parts on node
            diff = int(self.lord.execute(DBNAME, """
            with parts_count as (
	      select count(*) from shardman.partitions parts
	      join shardman.nodes nodes on parts.node_id = nodes.id
	      where nodes.replication_group = '{}'
	      group by node_id
	    )
            select max(count) - min(count) from parts_count;
            """.format(rg))[0][0])
            self.assertTrue(diff <= 1)

            diff_replicas = int(self.lord.execute(DBNAME, """
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
        self.pt_everyone_sees_the_whole()
        self.pt_replicas_integrity()

        self.pt_cleanup()

    def test_deadlock_detector(self):
        self.lord.create_cluster(2)
        self.lord.safe_psql(
            DBNAME, 'create table pt(id int primary key, payload int);')
        self.lord.safe_psql(
            DBNAME, "select shardman.create_hash_partitions('pt', 'id', 4)")
        self.lord.workers[0].safe_psql(
            DBNAME,
            "insert into pt select generate_series(1, 100), (random() * 100)::int")

        node_1_part = self.lord.execute(
            DBNAME,
            "select part_name from shardman.partitions where node_id = 1;")[0][0]

        # take parts & keys from node 1 and node 2 to work with
        node_1, node_2 = self.lord.workers_dict[1], self.lord.workers_dict[2]
        node_1_part = self.lord.execute(
            DBNAME,
            "select part_name from shardman.partitions where node_id = 1;")[0][0]
        node_1_key = node_1.execute(
            DBNAME, "select id from {} limit 1;".format(node_1_part))[0][0]
        node_2_part = self.lord.execute(
            DBNAME,
            "select part_name from shardman.partitions where node_id = 2;")[0][0]
        node_2_key = node_2.execute(
            DBNAME, "select id from {} limit 1;".format(node_2_part))[0][0]

        # Induce deadlock. It would be much better to use async db connections,
        # but pg8000 doesn't support them, and we generally aim at portability
        def xact_1():
            with node_1.connect() as con:
                con.begin()
                con.execute("update pt set payload = 42 where id = {}" \
                            .format(node_1_key))
                barrier.wait()
                try:
                    con.execute("update pt set payload = 43 where id = {}" \
                                .format(node_2_key))
                except Exception as e:
                    if "canceling statement due to user request" in str(e):
                        global xact_1_aborted
                        xact_1_aborted = True

        def xact_2():
            with node_1.connect() as con:
                con.begin()
                con.execute("update pt set payload = 42 where id = {}" \
                            .format(node_2_key))
                barrier.wait()
                try:
                    con.execute("update pt set payload = 43 where id = {}" \
                                .format(node_1_key))
                except Exception as e:
                    if "canceling statement due to user request" in str(e):
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
            with self.lord.connect() as con:
                con.execute("set statement_timeout = '3s'")
                con.execute("select shardman.monitor(check_timeout_sec => 1)")
        except:
            pass
        t1.join(5)
        self.assertTrue(not t1.is_alive())
        t2.join(5)
        self.assertTrue(not t2.is_alive())
        self.assertTrue(xact_1_aborted or xact_2_aborted)

        self.pt_cleanup()

    # dummiest test with no actual 2PC
    def test_recover_xacts_no_xacts(self):
        self.lord.create_cluster(2, 2)
        self.lord.safe_psql(DBNAME, "select shardman.recover_xacts()")
        self.lord.destroy_cluster()

    # WIP
    # def test_worker_failover(self):
    #     self.lord.create_cluster(2, 2)
    #     self.lord.safe_psql(
    #         DBNAME, 'create table pt(id int primary key, payload int default 1);')
    #     self.lord.safe_psql(
    #         DBNAME, "select shardman.create_hash_partitions('pt', 'id', 30, redundancy => 1);")
    #     self.lord.workers[0].safe_psql(
    #         DBNAME,
    #         "insert into pt select generate_series(1, 10000), (random() * 100)::int")

    #     # turn off some node
    #     # self.lord.workers[0].stop()
    #     # sleep(432423)

    #     # let monitor remove it
    #     try:
    #         with self.lord.connect() as con:
    #             con.execute("set statement_timeout = '3s'")
    #             con.execute("select shardman.monitor(check_timeout_sec => 1, rm_node_timeout_sec => 1)")
    #     except Exception as e:
    #         # make sure it was statement_timeout
    #         self.assertTrue("canceling statement due to statement timeout" in str(e))

    #     self.pt_cleanup()

    def test_recover(self):
        self.lord.create_cluster(2, 3)
        self.lord.safe_psql(
            DBNAME, 'create table pt(id int primary key, payload int default 1);')
        self.lord.safe_psql(
            DBNAME, "select shardman.create_hash_partitions('pt', 'id', 30, redundancy => 2);")
        self.lord.workers[0].safe_psql(
            DBNAME,
            "insert into pt select generate_series(1, 10000), (random() * 100)::int")

        # now accidently remove state from everywhere
        for worker in self.lord.workers:
            worker.safe_psql(DBNAME, "set local synchronous_commit to local; select shardman.wipe_state();")
        # repair things back
        self.lord.safe_psql(DBNAME, "select shardman.recover()")

        # write some more data
        self.lord.workers[0].safe_psql(
            DBNAME,
            "insert into pt select generate_series(10001, 20000), (random() * 100)::int")

        # and make sure data is still consistent
        self.pt_everyone_sees_the_whole()
        self.pt_replicas_integrity()

        self.pt_cleanup()

    def test_copy_from(self):
        self.lord.create_cluster(3, 2)
        self.lord.safe_psql(
            DBNAME, 'create table pt(id int primary key, payload int default 1);')
        self.lord.safe_psql(
            DBNAME, "select shardman.create_hash_partitions('pt', 'id', 30, redundancy => 1);")

        self.lord.safe_psql(
            DBNAME, 'create table pt_text(id int primary key, payload text);')
        self.lord.safe_psql(
            DBNAME, "select shardman.create_hash_partitions('pt_text', 'id', 30, redundancy => 1);")

        # copy some data on one node
        self.lord.workers[0].safe_psql(DBNAME, 'copy pt from stdin;', inp=
b"""1	2
2	3
4	5
6	7
8	9
10	10
\.
""")
        # and make sure another sees it
        res_sum = int(self.lord.workers[1].execute(
            DBNAME, sum_query("pt"))[0][0])
        self.assertEqual(res_sum, 36)
        self.lord.workers[0].safe_psql(DBNAME, 'delete from pt;')

        # specify column
        self.lord.workers[0].safe_psql(DBNAME, 'copy pt (id) from stdin;', inp=
b"""1
2
3
\.
""")
        res_sum = int(self.lord.workers[1].execute(DBNAME, sum_query('pt'))[0][0])
        self.assertEqual(res_sum, 3)
        self.lord.workers[0].safe_psql(DBNAME, 'delete from pt;')

        # csv
        self.lord.workers[0].safe_psql(
            DBNAME, 'copy pt from stdin (format csv);', inp=
            b"""1,2
            3,4
            5,6""")
        res_sum = int(self.lord.workers[1].execute(DBNAME, sum_query('pt'))[0][0])
        self.assertEqual(res_sum, 12)
        self.lord.workers[0].safe_psql(DBNAME, 'delete from pt;')

        # binary
        # generate binary data good for this platform
        data = self.lord.workers[0].safe_psql(
            DBNAME, """create table pt_local (id int, payload int);
            insert into pt_local values (1, 2);
            insert into pt_local values (3, 4);
            copy pt_local to stdout (format binary);
            drop table pt_local;
            """)
        # ... and make sure we don't support it
        with self.assertRaises(testgres.QueryException, msg="binary copy from not supported") as cm:
            self.lord.workers[0].safe_psql(
                DBNAME, 'copy pt from stdin (format binary);', inp=data)
        self.assertIn("cannot copy to postgres_fdw table", str(cm.exception))


        # freeze off
        self.lord.workers[0].safe_psql(
            DBNAME, 'copy pt from stdin (format csv, freeze false);', inp=
            b"""1,2
            3,4
            5,6""")
        res_sum = int(self.lord.workers[1].execute(DBNAME, sum_query('pt'))[0][0])
        self.assertEqual(res_sum, 12)
        self.lord.workers[0].safe_psql(DBNAME, 'delete from pt;')

        # freeze on
        with self.assertRaises(testgres.QueryException, msg="freeze not supported") as cm:
            self.lord.workers[0].safe_psql(
                DBNAME, 'copy pt from stdin (format csv, freeze true);', inp=
                b"""1,2
                3,4
                5,6""")
        self.assertIn("freeze is not supported", str(cm.exception))

        # delimiter
        self.lord.workers[0].safe_psql(
            DBNAME, "copy pt from stdin (format csv, delimiter '|');", inp=
            b"""1|2
            3|4
            5|6""")
        res_sum = int(self.lord.workers[1].execute(DBNAME, sum_query('pt'))[0][0])
        self.assertEqual(res_sum, 12)
        self.lord.workers[0].safe_psql(DBNAME, 'delete from pt;')

        # null
        self.lord.workers[0].safe_psql(
            DBNAME, "copy pt FROM stdin (format csv, null '44');", inp=
            b"""1,2
            3,44
            5,6""")
        res_sum = int(self.lord.workers[1].execute(DBNAME, sum_query('pt'))[0][0])
        self.assertEqual(res_sum, 8)
        self.lord.workers[0].safe_psql(DBNAME, 'delete from pt;')

        # header
        self.lord.workers[0].safe_psql(
            DBNAME, "copy pt FROM stdin (format csv, header true);", inp=
            b"""hoho,hehe
            3,4
            5, 6""")
        res_sum = int(self.lord.workers[1].execute(DBNAME, sum_query('pt'))[0][0])
        self.assertEqual(res_sum, 10)
        self.lord.workers[0].safe_psql(DBNAME, 'delete from pt;')

        # quote
        self.lord.workers[0].safe_psql(
            DBNAME, "copy pt FROM stdin (format csv, QUOTE '^');", inp=
            b"""1,2
            3,^4^
            5, 6""")
        res_sum = int(self.lord.workers[1].execute(DBNAME, sum_query('pt'))[0][0])
        self.assertEqual(res_sum, 12)
        self.lord.workers[0].safe_psql(DBNAME, 'delete from pt;')

        # escape
        self.lord.workers[0].safe_psql(
            DBNAME, "copy pt FROM stdin (format csv, quote '4', escape '@');", inp=
            b"""1,2
            3,4@44
            5, 6""")
        res_sum = int(self.lord.workers[1].execute(DBNAME, sum_query('pt'))[0][0])
        self.assertEqual(res_sum, 12)
        self.lord.workers[0].safe_psql(DBNAME, 'delete from pt;')

        # FORCE_NOT_NULL
        self.lord.workers[0].safe_psql(
            DBNAME, "copy pt FROM stdin (format csv, null '44', force_not_null (payload));", inp=
            b"""1,2
            3,44
            5,6""")
        res_sum = int(self.lord.workers[1].execute(DBNAME, sum_query('pt'))[0][0])
        self.assertEqual(res_sum, 52)
        self.lord.workers[0].safe_psql(DBNAME, 'delete from pt;')

        # FORCE_NULL
        self.lord.workers[0].safe_psql(
            DBNAME, "copy pt FROM stdin (format csv, quote '^', null '44', force_null (payload));", inp=
            b"""1,2
            3,^44^
            5,6""")
        res_sum = int(self.lord.workers[1].execute(DBNAME, sum_query('pt'))[0][0])
        self.assertEqual(res_sum, 8)
        self.lord.workers[0].safe_psql(DBNAME, 'delete from pt;')

        # Encoding. We should probably test workers with different server
        # encodings...
        self.lord.workers[0].safe_psql(
            DBNAME, "copy pt_text from stdin (format csv, encoding 'KOI8R')", inp=
            """1,йожин
            3,с
            5,бажин""".encode('koi8_r'))
        self.assertEqual("йожинсбажин", self.lord.workers[1].execute(
            DBNAME, "select string_agg(payload, '' order by id) from pt_text;")[0][0])

        self.lord.safe_psql(DBNAME, "select shardman.rm_table('pt');")
        self.lord.safe_psql(DBNAME, "drop table pt;")
        self.lord.safe_psql(DBNAME, "select shardman.rm_table('pt_text');")
        self.lord.safe_psql(DBNAME, "drop table pt_text;")
        self.lord.destroy_cluster()

# We violate good practices and order the tests -- it doesn't make sense to
# e.g. test copy_from if add_node doesn't work.
def suite():
    suite = unittest.TestSuite()
    suite.addTest(ShardmanTests('test_add_node'))
    suite.addTest(ShardmanTests('test_get_my_id'))
    suite.addTest(ShardmanTests('test_rm_node'))
    suite.addTest(ShardmanTests('test_create_hash_partitions_and_rm_table'))
    suite.addTest(ShardmanTests('test_set_redundancy'))
    suite.addTest(ShardmanTests('test_rebalance'))
    suite.addTest(ShardmanTests('test_deadlock_detector'))
    suite.addTest(ShardmanTests('test_recover_xacts_no_xacts'))
    suite.addTest(ShardmanTests('test_copy_from'))
    return suite

if __name__ == "__main__":
    runner = unittest.TextTestRunner(verbosity=2, failfast=True)
    runner.run(suite())

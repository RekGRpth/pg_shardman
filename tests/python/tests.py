#!/usr/bin/env python3
# coding: utf-8

"""
 tests.py
	pg_shardman tests.

	Copyright (c) 2017, Postgres Professional
"""

import unittest
import logging
from time import sleep
from contextlib import contextmanager

from testgres import PostgresNode, TestgresConfig

DBNAME = "postgres"

class Shardlord(PostgresNode):
    def __init__(self, name, port=None):
        super(Shardlord, self).__init__(name=name,
                                        port=port,
                                        use_logging=True)

        self.nodes_dict = {}

    @staticmethod
    def _common_conn_string(port):
        return "dbname={} port={}".format(DBNAME, port)

    def _shardlord_connstring(self):
        return self._common_conn_string(self.port)

    def _common_conf_lines(self):
        return (
            "shared_preload_libraries = 'pg_pathman'\n"
            "shardman.shardlord_connstring = '{}'\n"

            "shared_buffers = 512MB\n"
            "max_connections = 1000\n"
            "max_wal_size = 5GB\n"

            "log_min_messages = DEBUG1\n"
            "client_min_messages = NOTICE\n"
            "log_replication_commands = on\n"
        ).format(self._shardlord_connstring())

    def init(self):
        super(Shardlord, self).init()

        # common config lines
        config_lines = self._common_conf_lines()

        config_lines += (
            "shardman.shardlord = on\n"
            "shardman.shardlord_dbname = {}\n"
            "shardman.sync_replication = on\n"
        ).format(DBNAME)

        self.append_conf("postgresql.conf", config_lines)

        return self

    def install(self):
        self.safe_psql(DBNAME, "create extension pg_shardman cascade")

        return self

    def cleanup(self):
        super(Shardlord, self).cleanup()

        for node in self.nodes:
            node.cleanup()

        return self

    def add_node(self, name, repl_group='default', port=None):
        # worker conf
        config_lines = """
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

        # add common config lines
        config_lines += self._common_conf_lines()

        # create a new node
        node = PostgresNode(name=name, port=port, use_logging=True)
        # start this node
        node.init() \
            .append_conf("postgresql.conf", config_lines) \
            .start() \
            .safe_psql(DBNAME, "create extension pg_shardman cascade")

        # finally, register this node
        conn_string = self._common_conn_string(node.port)
        add_node_cmd = "select shardman.add_node('{}', repl_group => '{}')".format(
            conn_string, repl_group)
        new_node_id = int(self.execute(DBNAME, add_node_cmd)[0][0])
        self.nodes_dict[new_node_id] = node

        return node

    @property
    def nodes(self):
        return list(self.nodes_dict.values())

# run worker and some nodes, register them
@contextmanager
def DarthVader():
    # prepare ports for nodes
    ports = [5432+i for i in range(27)]
    ports.reverse()

    # collect all logs into a single file
    logfile = "/tmp/shmn.log"
    open(logfile, 'w').close()  # truncate log file
    logging.basicConfig(filename=logfile, level=logging.DEBUG)

    lord = Shardlord(name="DarthVader", port=ports.pop())
    lord.init().start().install()

    lord.add_node(name="Luke", repl_group='good', port=ports.pop())
    lord.add_node(name="C3PO", repl_group='good', port=ports.pop())

    lord.add_node(name="palpatine", repl_group='evil', port=ports.pop())
    lord.add_node(name="darthmaul", repl_group='evil', port=ports.pop())

    lord.add_node(name="jabba", repl_group='dontcare', port=ports.pop())
    lord.add_node(name="bobafett", repl_group='dontcare', port=ports.pop())

    yield lord

    lord.cleanup()

# shard simple table
def shard_table(lord):
            lord.safe_psql(DBNAME, 'create table pt(id int primary key, payload int);')
            lord.safe_psql(DBNAME, "select shardman.create_hash_partitions('pt', 'id', 30, redundancy => 1);")
            lord.nodes[0].safe_psql(DBNAME, "insert into pt select generate_series(1, 1000), (random() * 100)::int")

class ShardmanTests(unittest.TestCase):
    @staticmethod
    def sum_query(tname):
        return 'select sum(payload) from {}'.format(tname)

    def test_add_node(self):
        with DarthVader() as lord:
            nodes = lord.execute(DBNAME, 'select * from shardman.nodes')
            self.assertEqual(len(lord.nodes), len(nodes))

    def test_rm_node(self):
        # if we cache initdb, all instances have the same system id, so get_my_id
        # doesn't work
        TestgresConfig.cache_initdb = False
        with DarthVader() as lord:
            luke_id = lord.nodes[0].execute(DBNAME, 'select shardman.get_my_id()')[0][0]
            lord.safe_psql(DBNAME, 'select shardman.rm_node({})'.format(luke_id))
            nodes = lord.execute(DBNAME, 'select * from shardman.nodes')
            self.assertEqual(len(lord.nodes) - 1, len(nodes))
        TestgresConfig.cache_initdb = True

    def test_0_create_hash_partitions(self):
        with DarthVader() as lord:
            shard_table(lord)
            sum_query = 'select sum(payload) from pt'
            luke_sum = int(lord.nodes[0].execute(DBNAME, self.sum_query('pt'))[0][0])

            # check that every node sees the whole table
            for worker in lord.nodes[1:]:
                worker_sum = int(worker.execute(DBNAME, self.sum_query('pt'))[0][0])
                self.assertEqual(luke_sum, worker_sum)

            # check every replica integrity TODO
            # parts = lord.execute(DBNAME, "select part_name, node_id from shardman.partitions")
            # for part_name, node_id in parts:
            #     print("node id is {}".format(node_id))
            #     print("query is {}".format(self.sum_query(part_name)))
            #     print('sum is {}'.format(lord.nodes_dict[int(node_id)].execute(
            #         DBNAME, self.sum_query(part_name))))
            #     part_sum = int(lord.nodes_dict[int(node_id)].execute(
            #         DBNAME, self.sum_query(part_name))[0][0])

            # sleep(999999)

    def test_rm_table(self):
        with DarthVader() as lord:
            shard_table(lord)
            lord.safe_psql(DBNAME, "select shardman.rm_table('pt')")
            for worker in lord.nodes:
                ptrels = worker.execute(
                    DBNAME, "select relname from pg_class where relname ~ '^pt.*';")
                self.assertEqual(len(ptrels), 0)


if __name__ == "__main__":
    unittest.main()

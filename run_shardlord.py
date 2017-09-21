#!/usr/bin/env python

from time import sleep

from testgres import PostgresNode
from testgres import get_new_node


class Shardlord(PostgresNode):
    def __init__(self, name):
        super(Shardlord, self).__init__(name=name, port=5432)

        self.nodes = []

    @staticmethod
    def _common_conf_lines():
        return (
            "shared_preload_libraries = 'pg_pathman, pg_shardman'\n"

            "log_min_messages = DEBUG1\n"
            "client_min_messages = NOTICE\n"
            "log_replication_commands = on\n"

            "synchronous_commit = on\n"

            "wal_level = logical\n"

            "max_replication_slots = 100\n"
            "max_wal_senders = 50\n"
            "max_connections = 200\n"
        )

    def init(self):
        super(Shardlord, self).init()

        config_lines = (
            "shardman.shardlord = on\n"
            "shardman.shardlord_dbname = postgres\n"
            "shardman.shardlord_connstring = 'dbname=postgres port={}'\n"
            "shardman.cmd_retry_naptime = 500\n"
            "shardman.poll_interval = 500\n"
        ).format(self.port)

        # add common config lines
        config_lines += self._common_conf_lines()

        self.append_conf("postgresql.conf", config_lines)

        return self

    def install(self):
        self.safe_psql(dbname="postgres",
                       query="create extension pg_shardman cascade")

        return self

    def cleanup(self):
        super(Shardlord, self).cleanup()

        for node in self.nodes:
            node.cleanup()

        return self

    def add_node(self, name):
        config_lines = (
            "max_logical_replication_workers = 50\n"
            "max_worker_processes = 60\n"
            "wal_receiver_timeout = 60s\n"
        )

        # add common config lines
        config_lines += self._common_conf_lines()

        # create a new node
        node = get_new_node(name)
        self.nodes.append(node)

        # start this node
        node.init() \
            .append_conf("postgresql.conf", config_lines) \
            .start() \
            .safe_psql(dbname="postgres",
                       query="create extension pg_shardman cascade")

        # finally, register this node
        add_node_cmd = (
            "select shardman.add_node('dbname={} port={}')"
        ).format("postgres", node.port)
        self.safe_psql("postgres", add_node_cmd)

        return self


if __name__ == "__main__":
    with Shardlord("DarthVader") as lord:
        lord.init().start().install()

        lord.add_node("Luke")
        lord.add_node("ObiVan")
        lord.add_node("C3PO")

        print("%s:" % lord.name)
        print("\t-> port %i" % lord.port)
        print("\t-> dir  %s" % lord.base_dir)

        for node in lord.nodes:
            print()

            print("\t=> %s:" % node.name)
            print("\t\t-> port %i" % node.port)
            print("\t\t-> dir  %s" % node.base_dir)

        print()
        print("Press Ctrl+C to exit")

        # loop until SIGINT
        while True:
            sleep(1)

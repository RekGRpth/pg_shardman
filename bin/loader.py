#!/usr/bin/env python3

import argparse
import psycopg2
import time
import datetime
import traceback
from multiprocessing import Process, Pipe, Queue
import queue
import sys

class WorkerState:
    def __init__(self):
        self.worker_id = None
        self.node_id = None
        self.connstr = None
        self.numnodes = None
        self.child_conn = None  # only for child

        self.parent_conn = None  # only for parent
        self.child_handle = None  # only at parent

# Imitates file for copy_from
class Reader:
    def __init__(self, pipe, maxrows, bufsize):
        self.buf = ""
        self.pos = 0
        self.buflen = 0
        self.maxrows = maxrows
        self.rowcount = 0
        self.pipe = pipe
        self.eof = False # no more data in the pipe
        self.fill_bufsize = bufsize

    def read(self, size=None):
        if (self.pos == self.buflen) and not self.eof:
            # the buffer is read, fill it again
            self.fill_buffer()

        to_return = self.buflen - self.pos
        if (size is not None) and to_return > size:
            to_return = size
        res = self.buf[self.pos:self.pos + to_return]
        self.pos += to_return

        return res

    def readline(self, size=None):
        raise ValueError('who cares about this func?')

    def fill_buffer(self):
        assert self.pos == self.buflen

        s = ""
        while (len(s) <= self.fill_bufsize) and not self.eof and self.rowcount < self.maxrows:
            try:
                line = self.pipe.recv()
                # print("got {}".format(line))
                self.rowcount += 1
                s += line
            except EOFError:
                self.eof = True

        self.buf = s
        self.buflen = len(s)
        self.pos = 0

    def reset_rowcount(self):
        assert self.pos == self.buflen
        self.rowcount = 0

class CompletedXact:
    def __init__(self, worker_id, node_id, gid, rowcount):
        self.worker_id = worker_id
        self.node_id = node_id
        self.gid = gid
        self.rowcount = rowcount

class ErrorMsg:
    def __init__(self, worker_id, node_id, msg, backtrace):
        self.worker_id = worker_id
        self.node_id = node_id
        self.msg = msg
        self.backtrace = backtrace

def worker_main_internal(ws, args, completed_xacts):
    xact_count = 0
    reader = Reader(ws.child_conn, args.max_rows_per_xact, args.reader_bufsize)
    with psycopg2.connect(ws.connstr) as conn:
        with conn.cursor() as curs:
            curs.execute("select system_identifier from pg_control_system()")
            sysid = curs.fetchall()[0][0]
        conn.commit()
        while not reader.eof:
            gid = ''
            if not args.notwophase:
                # Try to respect shardman's gid format so recover_xacts can
                # handle it.
                # This actually won't work in most cases because we don't know
                # xid (apparently thanks to XA spec) and number of participants,
                # but it is easy to rollback/commit these xacts manually.
                gid = "pgfdw:{}:{}:{}:{}:{}:{}_shmnloader_{}_{}_{}_{}".format(
                    int(time.time()),
                    sysid,
                    0,  # procpid
                    0,  # xid
                    0,  # xact count
                    nnodes,  # participants count, take all
                    args.table_name,
                    ws.worker_id,
                    ws.node_id,
                    xact_count)
                conn.tpc_begin(gid)
            with conn.cursor() as curs:
                copy_sql = "copy {} from stdin (format csv, delimiter '{}', quote '{}', escape '{}')".format(args.table_name, args.delimiter, args.quote, args.escape)
                curs.copy_expert(copy_sql, reader, size=args.reader_bufsize)
            if not args.notwophase:
                # print("preparing {}".format(gid))
                conn.tpc_prepare()
                conn.reset()  # allow to run further xacts without finishing prepared
            else:
                conn.commit()
            completed_xact = CompletedXact(ws.worker_id, ws.node_id, gid, reader.rowcount)
            completed_xacts.append(completed_xact)
            reader.reset_rowcount()
            # print("xact {} finished".format(xact_count))
            xact_count += 1

def worker_main(ws, args):
    ws.parent_conn.close()  # close parent fd
    completed_xacts = []
    err_msg = None

    try:
        worker_main_internal(ws, args, completed_xacts)
    except Exception as e:
        err_msg = ErrorMsg(ws.worker_id, ws.node_id, str(e), traceback.format_exc())
    finally:
        ws.child_conn.close()
        # print("Process {} is done".format(ws.worker_id))
        if err_msg is None:
            for completed_xact in completed_xacts:
                ws.feedback_queue.put(completed_xact)
            ws.feedback_queue.put('STOP')
        else:
            ws.feedback_queue.put(err_msg)

class WorkerError(Exception):
    def __init__(self, errormsg):
        super().__init__("worker died")
        self.errormsg = errormsg

class Progress:
    def __init__(self, report_each_rows):
        self.start = time.time()
        self.report_each_rows = report_each_rows
        self.sent_count = 0
    def row_sent(self):
        self.sent_count += 1
        if self.sent_count % self.report_each_rows == 0:
            self.report()
    def report(self):
        elapsed_sec = time.time() - self.start
        print("{} rows sent, {} elapsed, {} rows per sec".format(self.sent_count,
                                                                 datetime.timedelta(seconds=int(elapsed_sec)),
                                                                 int(self.sent_count / elapsed_sec)))


def send_row(row, conn, feedback_queue, progress):
    m = None
    try: # notice errors asap
        m = feedback_queue.get(block=False)
    except queue.Empty:
        pass
    if m is not None:
        assert isinstance(m, ErrorMsg)
        raise WorkerError(m)

    try:
        conn.send(row)
    except Exception as e:
        # something wrong; probably worker died and closed the pipe?
        m = feedback_queue.get(block=True)
        assert isinstance(m, ErrorMsg)
        print("sending row failed with {} {}".format(str(e), traceback.format_exc()))
        raise WorkerError(m)

    if progress is not None:
        progress.row_sent()


def scatter_data(file_path, workers, nworkers, feedback_queue, args):
    row = ""
    buf = ""
    pos_copied = 0 # first char of buf not yet copied into row
    pos_approved = 0 # first char of buf not yet approved as part of current line
    bufsize = args.scatter_bufsize
    need_data = False
    last_was_esc = False
    in_quote = False
    last_was_cr = False
    next_worker = 0
    eof = False
    file_path = args.file_path
    quotec = args.quote
    escapec = args.escape

    if args.file_path is not None:
        f = open(file_path)
    else:
        f = sys.stdin
    try:
        for line in f:
            workers[next_worker].parent_conn.send(line)
            next_worker = (next_worker + 1) % nworkers
    except Exception as e:
        # something wrong; probably worker died and closed the pipe?
        m = feedback_queue.get(block=True)
        assert isinstance(m, ErrorMsg)
        print("sending row failed with {} {}".format(str(e), traceback.format_exc()))
        raise WorkerError(m)
    if args.file_path is not None:
        f.close()
    return

    # All this stuff is here because csv allows to have CR and LF characters
    # literally in import file if they are quotted and I wanted to have some fun
    # parsing it. Otherwise we could just read and send rows line-by-line...
    # Python csv parser is not entirely ok because a) It doesn't give raw lines
    # b) not sure how it handles newlines in data.
    # Partly inspired by CopyReadLineText.
    with open(file_path) as f:
        while True:
            # fill the buffer, if it is fully processed
            if pos_approved == len(buf):
                # pump read data into ready row
                row += buf[pos_copied:pos_approved]
                buf = ""
                pos_copied = 0
                pos_approved = 0
                while not eof and len(buf) < bufsize:
                    read_data = f.read(bufsize - len(buf))
                    buf += read_data
                    if read_data == '':
                        eof = True
                need_data = False

            if eof and pos_approved == len(buf):
                # eof and buffer processed
                if row != "":
                    # print(row)
                    assert row[-1] == '\r'
                    send_row(row, workers[next_worker].parent_conn, feedback_queue, progress)
                if progress:
                    progress.report()
                break

            c = buf[pos_approved]

            if last_was_cr:  # eol, either \r or \n
                if c == '\n':
                    # \r\n ending, include \n in current string
                    pos_approved += 1
                row += buf[pos_copied:pos_approved]
                # send row
                # print(row)
                send_row(row, workers[next_worker].parent_conn, feedback_queue, progress)
                next_worker = (next_worker + 1) % nworkers
                row = ""
                pos_copied = pos_approved
                last_was_cr = False
                if c == '\n':  # already processed
                    continue

            pos_approved += 1  # c anyway belongs to current row

            if escapec != quotec:
                # quotec always toggles quote unless last was escape
                if c == quotec and not last_was_esc:
                    in_quote = not in_quote;
                if in_quote and c == escapec:
                    last_was_esc = not last_was_esc
                if c != escapec:
                    last_was_esc = False
            else:
                # just toggle
                if c == quotec:
                    in_quote = not in_quote

            if not in_quote and c == '\r':
                last_was_cr = True

            if not in_quote and c == '\n':  # eol
                row += buf[pos_copied:pos_approved]
                # send row
                # print(row)
                send_row(row, workers[next_worker].parent_conn, feedback_queue, progress)
                next_worker = (next_worker + 1) % nworkers
                row = ""
                pos_copied = pos_approved


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="""
Dummy csv loader for shardman. Round-robins csv file row by row to a bunch of
workers; each worker round-robingly connects to some shardman node and performs
COPY FROM. Max rows per transaction is configurably limited. By default, 2PC
is used; script will first prepare xacts everywhere, then inform you that
everything is ok (or not) and then commit (abort) prepared xacts everywhere.
All gids contain shmnloader_${table_name}. Can also be used for importing data
into shared tables; in this case all workers knock directly to master node;
parallelism probably doesn't make much sense here.
Requires psycopg2 (though you probably already know it in since you are reading this, ha).
""",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-p', dest='nworkers', default=1, type=int,
                        help='number of working processes')
    parser.add_argument('-x', dest='max_rows_per_xact', default=4096, type=int,
                        help='max rows per xact')
    parser.add_argument('--no-twophase', dest='notwophase', action='store_const',
                        const=True, default=False,
                        help="""
                        Don't employ 2PC which is used by default.
                        """)
    parser.add_argument('--direct-connstr', dest='direct_connstr', default=None, type=str,
                        help=argparse.SUPPRESS)
    parser.add_argument('--reader-bufsize', dest='reader_bufsize', default=8192, type=int,
                        help=argparse.SUPPRESS)
    parser.add_argument('--scatter-bufsize', dest='scatter_bufsize', default=8192, type=int,
                        help=argparse.SUPPRESS)
    parser.add_argument('-d', dest='delimiter', default=',', type=str,
                        help='delimiter')
    parser.add_argument('-q', dest='quote', default='"', type=str,
                        help='quote')
    parser.add_argument('-e', dest='escape', default='"', type=str,
                        help='escape')
    parser.add_argument('--file-path', dest='file_path', default=None, type=str,
                        help='CSV file path. If not provided, data is read from stdin.')
    parser.add_argument('lord_connstring', metavar='lord_connstring', type=str,
                        help='shardlord connstring')
    parser.add_argument('table_name', metavar='table_name', type=str,
                        help='Table name, must be quotted if needed')

    args = parser.parse_args()
    workers = []
    nworkers = args.nworkers
    lord_connstring = args.lord_connstring
    table_name = args.table_name
    file_path = args.file_path

    nodes = []
    sharded_table = True
    if args.direct_connstr is None:
        with psycopg2.connect(lord_connstring) as lconn:
            with lconn.cursor() as curs:
                curs.execute("select id, connection_string from shardman.nodes")
                nodes = curs.fetchall()

                curs.execute("select master_node from shardman.tables where relation = '{}'".format(args.table_name))
                master_nodes = curs.fetchall()
                if len(master_nodes) != 1:
                    raise ValueError('Table {} is not sharded/shared'.format(args.table_name))
                master_node = master_nodes[0][0]
                if master_node is None:
                    sharded_table = True
                else:
                    sharded_table = False  # shared table
                    for node in nodes:
                        if node[0] == master_node:
                            master_connstr = node[1]
    else:
        nodes = [(1, args.direct_connstr)]

    nnodes = len(nodes)
    if nnodes < 1:
        raise ValueError("No nodes");

    # Use queue for collecting results. We could reuse pipes (selecting them),
    # but who cares
    feedback_queue = Queue()
    for i in range(nworkers):
        ws = WorkerState()
        workers.append(ws)
        ws.worker_id = i
        node_idx = i % (nnodes)
        if sharded_table or args.direct_connstr is not None:
            ws.node_id, ws.connstr = nodes[node_idx]
        else:
            ws.node_id, ws.connstr = (master_node, master_connstr)
        ws.numnodes = nnodes
        ws.parent_conn, ws.child_conn = Pipe()
        ws.feedback_queue = feedback_queue
        ws.child_handle = Process(target=worker_main, args=(ws, args))
        ws.child_handle.start()
        ws.child_conn.close()  # close child fd

    error = False
    finished_workers = 0
    try:
        scatter_data(file_path, workers, nworkers, feedback_queue, args)
    except WorkerError as e:
        error = True
        finished_workers = 1
        m = e.errormsg
        print("Worker {} attached to node {} failed with error '{}' \nThe backtrace is {}".format(m.worker_id, m.node_id, m.msg, m.backtrace))

    for i in range(nworkers):
        workers[i].parent_conn.close()

    workstat = [{'worker_id': i, 'numxacts': 0, 'rowcount': 0} for i in range(nworkers)]
    while finished_workers < nworkers:
        m = feedback_queue.get()
        if type(m) is str or isinstance(m, ErrorMsg):
            if isinstance(m, ErrorMsg):
                print("Worker {} attached to node {} failed with error '{}' \nThe backtrace is {}".format(m.worker_id, m.node_id, m.msg, m.backtrace))
                error=True
            else:
                assert type(m) is str and m == 'STOP'
            finished_workers += 1
        else:
            assert isinstance(m, CompletedXact)
            workstat[m.worker_id]['numxacts'] += 1
            workstat[m.worker_id]['rowcount'] += m.rowcount

    for i in range(nworkers):
        workers[i].child_handle.join()

    for i in range(nworkers):
        print("Worker {} completed {} xacts with total rowcount {}".format(i, workstat[i]['numxacts'], workstat[i]['rowcount']))

    if args.notwophase:
        if error:
            print("Looks like some transactions have failed. Examine the logs.")
        else:
            print("All transactions completed successfully")
        exit(0)

    gid_contains = "shmnloader_{}".format(args.table_name)
    if error:
        print('Some transactions have failed. Attempting to rollback all prepared xacts containing "{}" in gid...'.format(gid_contains))
    else:
        print('All transactions successfully prepared. Proceeding to commit all prepared xacts containing "{}" in gid...'.format(gid_contains))

    action = 'rollback' if error else 'commit'
    for node in nodes:
        connstr = node[1]
        with psycopg2.connect(connstr) as conn:
            conn.set_session(autocommit=True) # for commit/abort prepared
            with conn.cursor() as curs:
                curs.execute("select gid from pg_prepared_xacts where gid ~ '.*shmnloader_{}.*'".format(args.table_name))
                gids = [gidt[0] for gidt in curs.fetchall()]
                for gid in gids:
                    curs.execute("{} prepared '{}'".format(action, gid))
    print("Done")

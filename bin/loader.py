#!/usr/bin/env python3

import argparse
import psycopg2
import csv
from multiprocessing import Process, Pipe, Queue

class WorkerState:
    def __init__(self):
        self.worker_id = None
        self.node_id = None
        self.connstr = None
        self.child_conn = None  # only for child

        self.parent_conn = None  # only for parent
        self.child_handle = None  # only at parent

bufsize = 8 * (1 << 10)

# Imitates file for copy_from
class Reader:
    def __init__(self, pipe, maxrows):
        self.buf = ""
        self.pos = 0
        self.buflen = 0
        self.maxrows = maxrows
        self.rowcount = 0
        self.pipe = pipe
        self.eof = False # no more data in the pipe

    def read(self, size=None):
        if (self.pos == self.buflen) and not self.eof:
            # the buffer is read, fill it again
            self.fill_buffer()

        to_return = self.buflen - self.pos
        if (size is not None) and to_return > size:
            to_return = size
        res = self.buf[self.pos:self.pos + to_return]
        self.pos += to_return

        # print("res is {}".format(res))
        return res

    def readline(self, size=None):
        raise ValueError('who cares about this func?')

    def fill_buffer(self):
        assert self.pos == self.buflen

        s = ""
        while (len(s) <= bufsize) and not self.eof and self.rowcount < self.maxrows:
            try:
                line = self.pipe.recv()
                # print("got {}".format(line))
                self.rowcount += 1
                s += line
            except EOFError:
                self.eof = True

        self.buf = s
        self.buflen = len(s)

    def reset_rowcount(self):
        assert self.pos == self.buflen
        self.rowcount = 0

class CompletedXact:
    def __init__(self, worker_id, node_id, gid, rowcount):
        self.worker_id = worker_id
        self.node_id = node_id
        self.gid = gid
        self.rowcount = rowcount

def worker_main(ws, args):
    ws.parent_conn.close()  # close parent fd
    xact_count = 0
    completed_xacts = []

    reader = Reader(ws.child_conn, args.max_rows_per_xact)
    with psycopg2.connect(ws.connstr) as conn:
        while not reader.eof:
            gid = ''
            if args.twophase:
                gid = "shmnloader_{}_{}_{}_{}".format(args.table_name,
                                                               ws.worker_id,
                                                               ws.node_id,
                                                               xact_count)
                conn.tpc_begin(gid)
            with conn.cursor() as curs:
                copy_sql = "copy {} from stdin (format csv, delimiter '{}', quote '{}', escape '{}')".format(args.table_name, args.delimiter, args.quote, args.escape)
                curs.copy_expert(copy_sql, reader, size=bufsize)
            if args.twophase:
                conn.tpc_prepare()
            else:
                conn.commit()
            completed_xact = CompletedXact(ws.worker_id, ws.node_id, gid, reader.rowcount)
            completed_xacts.append(completed_xact)
            reader.reset_rowcount()
            # print("xact {} finished".format(xact_count))
            xact_count += 1
    ws.child_conn.close()
    # print("Process {} is done".format(ws.worker_id))
    for completed_xact in completed_xacts:
        ws.feedback_queue.put(completed_xact)
    ws.feedback_queue.put('STOP')

def scatter_data(file_path, workers, nworkers, quotec, escapec):
    row = ""
    buf = ""
    pos_copied = 0 # first char of buf not yet copied into row
    pos_approved = 0 # first char of buf not yet approved as part of current line
    bufsize = 1 << 20
    need_data = False
    last_was_esc = False
    in_quote = False
    last_was_cr = False
    next_worker = 0
    eof = False

    # All this stuff is here because csv allows to have CR and LF characters
    # literally in import file if they are quotted and I wanted to have some fun
    # parsing it. Otherwise we could just read and send rows line-by-line...
    # Python csv parser is not ok because a) It doesn't give raw lines b)
    # not sure how it handles newlines in data.
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
                    assert row[-1] == '\r'
                    workers[next_worker].parent_conn.send(row)
                break

            c = buf[pos_approved]

            if last_was_cr:  # eol, either \r or \n
                if c == '\n':
                    # \r\n ending, include \n in current string
                    pos_approved += 1
                row += buf[pos_copied:pos_approved]
                # send row
                workers[next_worker].parent_conn.send(row)
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
                workers[next_worker].parent_conn.send(row)
                next_worker = (next_worker + 1) % nworkers
                row = ""
                pos_copied = pos_approved


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="""
Dummy csv loader for shardman. Round-robins csv file row by row to a bunch of 
workers; each worker round-robingly connects to some shardman node and performs
COPY FROM. Max rows per transaction is configurably limited.
Requires psycopg2.
""",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-p', dest='nworkers', default=1, type=int,
                        help='number of working processes')
    parser.add_argument('-x', dest='max_rows_per_xact', default=4096, type=int,
                        help='max rows per xact')
    parser.add_argument('--no-twophase', dest='twophase', action='store_const',
                        const=False, default=True,
                        help='Don\'t use 2PC')
    parser.add_argument('-d', dest='delimiter', default=',', type=str,
                        help='delimiter')
    parser.add_argument('-q', dest='quote', default='"', type=str,
                        help='quote')
    parser.add_argument('-e', dest='escape', default='"', type=str,
                        help='escape')
    parser.add_argument('lord_connstring', metavar='lord_connstring', type=str,
                        help='shardlord connstring')
    parser.add_argument('table_name', metavar='table_name', type=str,
                        help='Table name, must be quotted if needed')
    parser.add_argument('file_path', metavar='file_path', type=str,
                        help='csv file path')


    args = parser.parse_args()
    workers = []
    nworkers = args.nworkers
    lord_connstring = args.lord_connstring
    table_name = args.table_name
    file_path = args.file_path

    nodes = []
    with psycopg2.connect(lord_connstring) as lconn:
        with lconn.cursor() as curs:
            curs.execute("select id, connection_string from shardman.nodes")
            nodes = curs.fetchall()
    nnodes = len(nodes)
    if nnodes < 1:
        raise ValueError("No workers");

    # Use queue for collecting results. We could reuse pipes (selecting them),
    # but who cares
    feedback_queue = Queue()
    for i in range(nworkers):
        ws = WorkerState()
        workers.append(ws)
        ws.worker_id = i
        node_idx = i % (nnodes)
        ws.node_id, ws.connstr = (nodes[node_idx][0], nodes[node_idx][1])
        ws.parent_conn, ws.child_conn = Pipe()
        ws.feedback_queue = feedback_queue
        ws.child_handle = Process(target=worker_main, args=(ws, args))
        ws.child_handle.start()
        ws.child_conn.close()  # close child fd

    scatter_data(file_path, workers, nworkers, args.quote, args.escape)

    for i in range(nworkers):
        workers[i].parent_conn.close()

    finished_workers = 0
    workstat = [{'worker_id': i} for i in range(nworkers)]
    while finished_workers < nworkers:
        m = feedback_queue.get()
        if type(m) is str and m == 'STOP':
            finished_workers += 1
        else:
            if 'numxacts' in workstat[m.worker_id]:
                workstat[m.worker_id]['numxacts'] += 1
            else:
                workstat[m.worker_id]['numxacts'] = 1
            if 'rowcount' in workstat[m.worker_id]:
                workstat[m.worker_id]['rowcount'] += m.rowcount
            else:
                workstat[m.worker_id]['rowcount'] = m.rowcount

    for i in range(nworkers):
        workers[i].child_handle.join()

    for i in range(nworkers):
        print("Worker {} completed {} xacts with total rowcount {}".format(i, workstat[i]['numxacts'], workstat[i]['rowcount']))

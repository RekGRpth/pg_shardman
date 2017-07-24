/* -------------------------------------------------------------------------
 *
 * shard.c
 *		Sharding commands implementation.
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "libpq-fe.h"
#include "lib/ilist.h"

#include <unistd.h>
#include <time.h>
#include <limits.h>
#include <sys/epoll.h>

#include "shard.h"
#include "timeutils.h"

/* epoll max events */
#define MAX_EVENTS 64

/* Bitmask for ensure_pqconn */
#define ENSURE_PQCONN_SRC (1 << 0)
#define ENSURE_PQCONN_DST (1 << 1)

/* final result of 1 master partition move */
typedef enum
{
	MOVEMPART_IN_PROGRESS,
	MOVEMPART_FAILED,
	MOVEMPART_SUCCESS
} MoveMPartRes;

/* result of one iteration of master partition moving */
typedef enum
{
	EXECMOVEMPART_EPOLL, /* add me to epoll on epolled_fd on EPOLLIN */
	EXECMOVEMPART_WAKEMEUP, /* wake me up again on waketm */
	EXECMOVEMPART_DONE /* the work is done, never invoke me again */
} ExecMoveMPartRes;

/* Current step of 1 master partition move */
typedef enum
{
	MOVEMPARTSTEP_START_TABLESYNC,
	MOVEMPARTSTEP_WAIT_TABLESYNC
} MoveMPartStep;

typedef struct
{
	const char *part_name; /* partition name */
	int32 src_node; /* node we are moving partition from */
	int32 dst_node; /* node we are moving partition to */
	const char *src_connstr;
	const char *dst_connstr;
	/* wake me up at waketm to do the job. Try to keep it zero when invalid	*/
	struct timespec waketm;
	/* We need to epoll only on socket with dst to wait for copy */
	 /* exec_move_mpart sets fd here when it wants to be wakened by epoll */
	int fd_to_epoll;
	int fd_in_epoll_set; /* socket *currently* in epoll set. -1 of none */
	PGconn *src_conn; /* connection to src */
	PGconn *dst_conn; /* connection to dst */

	/*
	 * The following strs are constant during execution; we allocate them
	 * once in init func and they disappear with cmd mem ctxt
	 */
	char *logname; /* name of publication, repslot and subscription */
	char *dst_drop_sub_sql; /* sql to drop sub on dst node */
	char *src_create_pub_and_rs_sql; /* create publ and repslot on src */
	char *relation; /* name of sharded relation */
	char *dst_create_tab_and_sub_sql; /* create table and sub on dst */

	MoveMPartStep curstep; /* current step */
	ExecMoveMPartRes exec_res; /* result of the last iteration */
	MoveMPartRes res; /* result of the whole move */
} MoveMPartState;

typedef struct
{
	slist_node list_node;
	MoveMPartState *mmps;
} MoveMPartStateNode;

static void init_mmp_state(MoveMPartState *mmps, const char *part_name,
						   int32 dst_node);
static void finalize_mmp_state(MoveMPartState *mmps);
static void move_mparts(MoveMPartState *mmpss, int nparts);
static int calc_timeout(slist_head *timeout_states);
static void epoll_subscribe(int epfd, MoveMPartState *mmps);
static void exec_move_mpart(MoveMPartState *mmps);
static int start_tablesync(MoveMPartState *mmpts);
static int ensure_pqconn(MoveMPartState *mmpts, int nodes);
static int ensure_pqconn_intern(PGconn **conn, const char *connstr,
								MoveMPartState *mmps);
static void reset_pqconn(PGconn **conn);
static void reset_pqconn_and_res(PGconn **conn, PGresult *res);
static void configure_retry(MoveMPartState *mmpts, int millis);
static struct timespec timespec_now_plus_millis(int millis);

/*
 * Steps are:
 * - Ensure table is not partitioned already;
 * - Partition table and get sql to create it;
 * - Add records about new table and partitions;
 */
void
create_hash_partitions(Cmd *cmd)
{
	int32 node_id = atoi(cmd->opts[0]);
	const char *relation = cmd->opts[1];
	const char *expr = cmd->opts[2];
	int partitions_count = atoi(cmd->opts[3]);
	char *connstr;
	PGconn *conn = NULL;
	PGresult *res = NULL;
	char *sql;
	uint64 table_exists;
	char *create_table_sql;

	shmn_elog(INFO, "Sharding table %s on node %d", relation, node_id);

	/* Check that table with such name is not already sharded */
	sql = psprintf(
		"select relation from shardman.tables where relation = '%s'",
		relation);
	table_exists = void_spi(sql);
	if (table_exists)
	{
		shmn_elog(WARNING, "table %s already sharded, won't partition it.",
				  relation);
		update_cmd_status(cmd->id, "failed");
		return;
	}
	/* connstr mem freed with ctxt */
	if ((connstr = get_worker_node_connstr(node_id)) == NULL)
	{
		shmn_elog(WARNING, "create_hash_partitions failed, no such worker node: %d",
				  node_id);
		update_cmd_status(cmd->id, "failed");
		return;
	}

	/* Note that we have to run statements in separate transactions, otherwise
	 * we have a deadlock between pathman and pg_dump.
	 * pfree'd with ctxt
	 */
	sql = psprintf(
		"begin; select create_hash_partitions('%s', '%s', %d); end;"
		"select shardman.gen_create_table_sql('%s', '%s');",
		relation, expr, partitions_count,
		relation, connstr);

	/* Try to execute command indefinitely until it succeeded or canceled */
	while (!got_sigusr1 && !got_sigterm)
	{
		conn = PQconnectdb(connstr);
		if (PQstatus(conn) != CONNECTION_OK)
		{
			shmn_elog(NOTICE, "Connection to node failed: %s",
					  PQerrorMessage(conn));
			goto attempt_failed;
		}

		/* Partition table and get sql to create it */
		res = PQexec(conn, sql);
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			shmn_elog(NOTICE, "Failed to partition table and get sql to create it: %s",
					  PQerrorMessage(conn));
			goto attempt_failed;
		}
		create_table_sql = PQgetvalue(res, 0, 0);

		/* TODO: if master fails at this moment (which is extremely unlikely
		 * though), after restart it will try to partition table again and
		 * fail. We should check if the table is already partitioned and don't
		 * do that again, except for, probably, the case when it was
		 * partitioned by someone else.
		 */
		/*
		 * Insert table to 'tables' table (no pun intended), insert partitions
		 * and mark partitioning cmd as successfull
		 */
		sql = psprintf("insert into shardman.tables values"
					   " ('%s', '%s', %d, $create_table$%s$create_table$, %d);"
					   " update shardman.cmd_log set status = 'success'"
					   " where id = %ld;",
					   relation, expr, partitions_count, create_table_sql,
					   node_id, cmd->id);
		void_spi(sql);
		pfree(sql);

		PQclear(res); /* can't free any earlier, it stores sql */
		PQfinish(conn);

		/* done */
		elog(INFO, "Table %s successfully partitioned", relation);
		return;

attempt_failed: /* clean resources, sleep, check sigusr1 and try again */
		if (res != NULL)
			PQclear(res);
		if (conn != NULL)
			PQfinish(conn);

		shmn_elog(LOG, "Attempt to execute create_hash_partitions failed,"
				  " sleeping and retrying");
		/* TODO: sleep using waitlatch? */
		pg_usleep(shardman_cmd_retry_naptime * 1000L);
	}
	check_for_sigterm();

	cmd_canceled(cmd);
}

/*
 * Move master partition to specified node. We
 * - Disable subscription on destination, otherwise we can't drop rep slot on
     source.
 * - Idempotently create publication and repl slot on source.
 * - Idempotently create table and async subscription on destination.
 *   We use async subscription, because sync would block table while copy is
 *   in progress. But with async, we have to lock the table after initial sync.
 * - Now inital copy has started, remember that at least in ram to retry
 *   from this point if network fails.
 * - Sleep & check in connection to the dest waiting for completion of the
 *   initial sync. Later this should be substituted with listen/notify.
 * - When done, lock writes (better lock reads too) on source and remember
 *   current wal lsn on it.
 * - Now final sync has started, remember that at least in ram.
 * - Sleep & check in connection to dest waiting for completion of final sync,
 *   i.e. when received_lsn is equal to remembered lsn on src.
 * - Now update metadata on master, mark cmd as complete and we are done.
 *   src table will be dropped via metadata update
 *
 *  If we don't save progress (whether initial sync started or done, lsn,
 *  etc), we have to start everything from the ground if master reboots. This
 *  is arguably fine. There is also a very small chance that the command will
 *  complete but fail before status is set to 'success', and after reboot will
 *  fail because the partition was already moved.
 *
 */
void
move_mpart(Cmd *cmd)
{
	char *part_name = cmd->opts[0];
	int32 dst_node = atoi(cmd->opts[1]);

	MoveMPartState *mmps = palloc(sizeof(MoveMPartState));
	init_mmp_state(mmps, part_name, dst_node);

	move_mparts(mmps, 1);
	check_for_sigterm();
	if (got_sigusr1)
	{
		cmd_canceled(cmd);
		return;
	}

	Assert(mmps->res != MOVEMPART_IN_PROGRESS);
	if (mmps->res == MOVEMPART_FAILED)
		update_cmd_status(cmd->id, "failed");
	else if (mmps->res == MOVEMPART_SUCCESS)
		update_cmd_status(cmd->id, "success");
}


/*
 * Fill MoveMPartState, retrieving needed data. If something goes wrong, we
 * don't bother to fill the rest of fields.
 */
void
init_mmp_state(MoveMPartState *mmps, const char *part_name, int32 dst_node)
{
	int e;

	mmps->part_name = part_name;
	if ((mmps->src_node = get_partition_owner(part_name)) == -1)
	{
		shmn_elog(WARNING, "Partition %s doesn't exist, not moving it",
				  part_name);
		mmps->res = MOVEMPART_FAILED;
		return;
	}
	mmps->dst_node = dst_node;

	/* src_connstr is surely not NULL since src_node is referenced by
	   part_name */
	mmps->src_connstr = get_worker_node_connstr(mmps->src_node);
	mmps->dst_connstr = get_worker_node_connstr(mmps->dst_node);
	if (mmps->dst_connstr == NULL)
	{
		shmn_elog(WARNING, "Node %d doesn't exist, not moving %s to it",
				  mmps->dst_node, part_name);
		mmps->res = MOVEMPART_FAILED;
		return;
	}

	/* Task is ready to be processed right now */
	if ((e = clock_gettime(CLOCK_MONOTONIC, &mmps->waketm)) == -1)
	{
		shmn_elog(FATAL, "clock_gettime failed, %s", strerror(e));
	}
	mmps->fd_to_epoll = -1;
	mmps->fd_in_epoll_set = -1;

	mmps->src_conn = NULL;
	mmps->dst_conn = NULL;

	/* constant strings */
	mmps->logname = psprintf("shardman_copy_%s_%d_%d",
							 mmps->part_name, mmps->src_node, mmps->dst_node);
	mmps->dst_drop_sub_sql = psprintf(
		"drop subscription if exists %s cascade;", mmps->logname);
	/*
	 * Note that we run stmts in separate txns: repslot can't be created in in
	 * transaction that performed writes
	 */
	mmps->src_create_pub_and_rs_sql = psprintf(
		"begin; drop publication if exists %s cascade;"
		" create publication %s for table %s; end;"
		" select shardman.create_repslot('%s');",
		mmps->logname, mmps->logname, mmps->part_name, mmps->logname
		);
	mmps->relation = get_partition_relation(part_name);
	mmps->dst_create_tab_and_sub_sql = psprintf(
		"drop table if exists %s cascade;"
		/*
		 * TODO: we are mimicking pathman's partition creation here. At least
		 * one difference is that we don't copy foreign keys, so this should
		 * be fixed. For example, we could directly call pathman's
		 * create_single_partition_internal func here, though currently it is
		 * static. We could also just use old empty partition and not remove
		 * it, but considering (in very far perspective) ALTER TABLE this is
		 * wrong approach.
		 */
		" create table %s (like %s including defaults including indexes"
		" including storage);"
		" drop subscription if exists %s cascade;"
		" create subscription %s connection '%s' publication %s with"
		"   (create_slot = false, slot_name = '%s');",
		mmps->part_name,
		mmps->part_name, mmps->relation,
		mmps->logname,
		mmps->logname, mmps->src_connstr, mmps->logname, mmps->logname);

	mmps->curstep = MOVEMPARTSTEP_START_TABLESYNC;
	mmps->res = MOVEMPART_IN_PROGRESS;
}

/*
 * Close pq connections, if any.
 */
static void finalize_mmp_state(MoveMPartState *mmps)
{
	if (mmps->src_conn != NULL)
		reset_pqconn(&mmps->src_conn);
	if (mmps->dst_conn != NULL)
		reset_pqconn(&mmps->dst_conn);
}

/*
 * Move partitions as specified in move_mpart_states array. Results (and
 * general state is saved in this array too. Tries to move all parts until
 * all have failed/succeeded or sigusr1/sigterm is caugth.
 *
 */
void
move_mparts(MoveMPartState *mmpss, int nparts)
{
	/* list of sleeping mmp states we need to wake after specified timeout */
	slist_head timeout_states = SLIST_STATIC_INIT(timeout_states);
	slist_mutable_iter iter;
	/* at least one task will require our attention at waketm */
	struct timespec waketm;
	struct timespec curtm;
	int timeout;
	int unfinished_moves = 0; /* number of not yet failed or succeeded tasks */
	int i;
	int e;
	int epfd;
	struct epoll_event evlist[MAX_EVENTS];

	/* In the beginning, all tasks are ready for execution, so wake tm is right
	 * is actually current time. We also need to put all tasks to the
	 * timeout_states list to invoke them.
	 */
	if ((e = clock_gettime(CLOCK_MONOTONIC, &waketm)) == -1)
		shmn_elog(FATAL, "clock_gettime failed, %s", strerror(e));
	for (i = 0; i < nparts; i++)
	{
		if (mmpss[i].res != MOVEMPART_FAILED)
		{
			MoveMPartStateNode *mmps_node = palloc(sizeof(MoveMPartStateNode));
			elog(DEBUG2, "Adding task %s to timeout list", mmpss[i].part_name);
			mmps_node->mmps = &mmpss[i];
			slist_push_head(&timeout_states, &mmps_node->list_node);
			unfinished_moves++;
		}
	}

	if ((epfd = epoll_create1(0)) == -1)
		shmn_elog(FATAL, "epoll_create1 failed");

	/* TODO: check for signals */
	while (unfinished_moves > 0 && !got_sigusr1 && !got_sigterm)
	{
		timeout = calc_timeout(&timeout_states);
		e = epoll_wait(epfd, evlist, MAX_EVENTS, timeout);
		if (e == -1)
		{
			if (errno == EINTR)
				continue;
			else
				shmn_elog(FATAL, "epoll_wait failed, %s", strerror(e));
		}

		/* Run all tasks for which it is time to wake */
		slist_foreach_modify(iter, &timeout_states)
		{
			MoveMPartStateNode *mmps_node =
				slist_container(MoveMPartStateNode, list_node, iter.cur);
			MoveMPartState *mmps = mmps_node->mmps;
			if ((e = clock_gettime(CLOCK_MONOTONIC, &curtm)) == -1)
				shmn_elog(FATAL, "clock_gettime failed, %s", strerror(e));

			if (timespeccmp(mmps->waketm, curtm) <= 0)
			{
				shmn_elog(DEBUG1, "%s is ready for exec", mmps->part_name);
				exec_move_mpart(mmps);
				switch (mmps->exec_res)
				{
					case EXECMOVEMPART_WAKEMEUP:
						/* We need to wake this task again, to keep it in
						 * in the list and just continue */
						continue;

					case EXECMOVEMPART_EPOLL:
						/* Task wants to be wakened by epoll */
						epoll_subscribe(epfd, mmps);
						break;

					case EXECMOVEMPART_DONE:
						/* Task is done, decrement the counter */
						unfinished_moves--;
						break;
				}
				/* If we are still here, remove node from timeouts_list */
				slist_delete_current(&iter);
				/* And free node */
				pfree(mmps_node);
			}
		}
	}

	/*
	 * Free list. This not necessary though, we are finishing cmd and
	 * everything will be freed soon.
	 */
	slist_foreach_modify(iter, &timeout_states)
	{
		MoveMPartStateNode *mmps_node =
				slist_container(MoveMPartStateNode, list_node, iter.cur);
		slist_delete_current(&iter);
		pfree(mmps_node);
	}
	/* But this is important, as libpq manages memory on its own */
	for (i = 0; i < nparts; i++)
		finalize_mmp_state(&mmpss[i]);
	close(epfd);
}

/*
 * Calculate when we need to wake if no epoll events are happening.
 * Returned value is ready for epoll_wait.
 */
int
calc_timeout(slist_head *timeout_states)
{
	slist_iter iter;
	int e;
	struct timespec curtm;
	int timeout;
	/* could use timespec field for this, but that's more readable */
	bool waketm_set = false;
	struct timespec waketm; /* min of all waketms */

	/* calc min waketm */
	slist_foreach(iter, timeout_states)
	{
		MoveMPartStateNode *mmps_node =
			slist_container(MoveMPartStateNode, list_node, iter.cur);
		MoveMPartState *mmps = mmps_node->mmps;

		/* If waketm is not set, what this node does in this list? */
		Assert(mmps->waketm.tv_nsec != 0);
		if (!waketm_set || timespeccmp(mmps->waketm, waketm) < 0)
		{
			shmn_elog(DEBUG1, "Waketm updated, old %d s, new %d s",
					  waketm_set ? (int) waketm.tv_sec : 0,
					  (int) mmps->waketm.tv_sec);
			waketm = mmps->waketm;
			waketm_set = true;
		}

	}

	/* now calc timeout */
	if (!waketm_set)
		return -1;

	if ((e = clock_gettime(CLOCK_MONOTONIC, &curtm)) == -1)
			shmn_elog(FATAL, "clock_gettime failed, %s", strerror(e));
	if (timespeccmp(waketm, curtm) <= 0)
	{
		shmn_elog(DEBUG1, "Non-negative timeout, waking immediately");
		return 0;
	}

	timeout = Max(0, timespec_diff_millis(waketm, curtm));
	shmn_elog(DEBUG1, "New timeout is %d ms", timeout);
	return timeout;
}

/*
 * Ensure that mmps is registered in epoll and set proper mode.
 * We never remove fds from epoll, they should be removed automatically when
 * closed.
 */
void
epoll_subscribe(int epfd, MoveMPartState *mmps)
{
	struct epoll_event ev;
	int e;

	ev.data.ptr = mmps;
	ev.events = EPOLLIN | EPOLLONESHOT;
	Assert(mmps->fd_to_epoll != -1);
	if (mmps->fd_to_epoll == mmps->fd_in_epoll_set)
	{
		if ((e = epoll_ctl(epfd, EPOLL_CTL_MOD, mmps->fd_to_epoll, &ev)) == -1)
			shmn_elog(FATAL, "epoll_ctl failed, %s", strerror(e));
	}
	else
	{
		if ((e = epoll_ctl(epfd, EPOLL_CTL_ADD, mmps->fd_to_epoll, &ev)) == -1)
			shmn_elog(FATAL, "epoll_ctl failed, %s", strerror(e));
		mmps->fd_in_epoll_set = mmps->fd_to_epoll;
	}
	shmn_elog(DEBUG1, "socket for task %s added to epoll", mmps->part_name);
}

/*
 * Actually run MoveMPart state machine. On return, mmps values say when (if
 * ever) we want to be executed again.
 */
void
exec_move_mpart(MoveMPartState *mmps)
{
	/* Mark waketm as invalid for safety */
	mmps->waketm = (struct timespec) {0};

	if (mmps->curstep == MOVEMPARTSTEP_START_TABLESYNC)
	{
		if (start_tablesync(mmps) == -1)
			return;
		else
			mmps->curstep =	MOVEMPARTSTEP_WAIT_TABLESYNC;
	}

	shmn_elog(DEBUG1, "Partition %s is moved", mmps->part_name);
	mmps->res =	MOVEMPART_SUCCESS;
	mmps->exec_res = EXECMOVEMPART_DONE;
}

/*
 * Set up logical replication between src and dst. If anything goes wrong,
 * it configures mmps properly and returns -1, otherwise 0.
 */
int
start_tablesync(MoveMPartState *mmps)
{
	PGresult *res;

	if (ensure_pqconn(mmps, ENSURE_PQCONN_SRC | ENSURE_PQCONN_DST) == -1)
		return -1;

	res = PQexec(mmps->dst_conn, mmps->dst_drop_sub_sql);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		shmn_elog(NOTICE, "Failed to drop sub on dst: %s",
				  PQerrorMessage(mmps->dst_conn));
		reset_pqconn_and_res(&mmps->dst_conn, res);
		configure_retry(mmps, shardman_cmd_retry_naptime);
		return -1;
	}
	PQclear(res);
	shmn_elog(DEBUG1, "mmp %s: sub on dst dropped, if any", mmps->part_name);

	res = PQexec(mmps->src_conn, mmps->src_create_pub_and_rs_sql);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		shmn_elog(NOTICE, "Failed to create pub and repslot on src: %s",
				  PQerrorMessage(mmps->src_conn));
		reset_pqconn_and_res(&mmps->src_conn, res);
		configure_retry(mmps, shardman_cmd_retry_naptime);
		return -1;
	}
	PQclear(res);
	shmn_elog(DEBUG1, "mmp %s: pub and rs recreated on src", mmps->part_name);

	res = PQexec(mmps->dst_conn, mmps->dst_create_tab_and_sub_sql);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		shmn_elog(NOTICE, "Failed to recreate table & sub on dst: %s",
				  PQerrorMessage(mmps->dst_conn));
		reset_pqconn_and_res(&mmps->dst_conn, res);
		configure_retry(mmps, shardman_cmd_retry_naptime);
		return -1;
	}
	PQclear(res);
	shmn_elog(DEBUG1, "mmp %s: table & sub created on dst, tablesync started",
			  mmps->part_name);

	return 0;
}

/*
 * Ensure that pq connection to src and dst node is CONNECTION_OK. nodes
 * is a bitmask specifying with which nodes -- src, dst or both -- connection
 * must be ensured. -1 is returned if we have failed to establish connection;
 * mmps is then configured to sleep retry time. 0 is returned if ok.
 */
int
ensure_pqconn(MoveMPartState *mmps, int nodes)
{
	if ((nodes & ENSURE_PQCONN_SRC) &&
		(ensure_pqconn_intern(&mmps->src_conn, mmps->src_connstr, mmps) == -1))
		return -1;
	if ((nodes & ENSURE_PQCONN_DST) &&
		(ensure_pqconn_intern(&mmps->dst_conn, mmps->dst_connstr, mmps) == -1))
		return -1;
	return 0;
}

/*
 * Working horse of ensure_pqconn
 */
int
ensure_pqconn_intern(PGconn **conn, const char *connstr,
					   MoveMPartState *mmps)
{
	if (*conn != NULL &&
		PQstatus(*conn) != CONNECTION_OK)
	{
		reset_pqconn(conn);
	}
	if (*conn == NULL)
	{
		*conn = PQconnectdb(connstr);
		if (PQstatus(*conn) != CONNECTION_OK)
		{
			shmn_elog(NOTICE, "Connection to node failed: %s",
					  PQerrorMessage(*conn));
			reset_pqconn(conn);
			configure_retry(mmps, shardman_cmd_retry_naptime);
			return -1;
		}
		shmn_elog(DEBUG1, "Connection to %s established", connstr);
	}
	return 0;
}

/*
 * Finish pq connection and set ptr to NULL. You must be sure that the
 * connection exists!
 */
void
reset_pqconn(PGconn **conn) { PQfinish(*conn); *conn = NULL; }
/* Same, but also clear res. You must be sure it exists */
void
reset_pqconn_and_res(PGconn **conn, PGresult *res)
{
	PQclear(res); reset_pqconn(conn);
}


/*
 * Configure mmps so that main loop wakes us again after given retry millis.
 */
void configure_retry(MoveMPartState *mmps, int millis)
{
	shmn_elog(DEBUG1, "Moving mpart %s: sleeping %d ms and retrying",
			  mmps->part_name, millis);
	mmps->waketm = timespec_now_plus_millis(millis);
	mmps->exec_res = EXECMOVEMPART_WAKEMEUP;
}

/*
 * Get current time + given milliseconds. Fails with PG elog(FATAL) if gettime
 * failed. Not very generic, yes, but exactly what we need.
 */
struct timespec timespec_now_plus_millis(int millis)
{
	struct timespec t;
	int e;

	if ((e = clock_gettime(CLOCK_MONOTONIC, &t)) == -1)
		shmn_elog(FATAL, "clock_gettime failed, %s", strerror(e));

	return timespec_add_millis(t, millis);
}

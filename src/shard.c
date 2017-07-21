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

typedef enum
{
	MOVEMPART_IN_PROGRESS,
	MOVEMPART_FAILED,
	MOVEMPART_SUCCESS
} MoveMPartResult;

/* result of one iteration of processing */
typedef enum
{
	EXECMOVEMPART_EPOLL, /* add me to epoll on epolled_fd on EPOLLIN */
	EXECMOVEMPART_WAKEMEUP, /* wake me up again on waketm */
	EXECMOVEMPART_DONE /* the work is done, never invoke me again */
} ExecMoveMPartRes;

typedef struct
{
	const char *part_name; /* partition name */
	int32 src_node; /* node we are moving partition from */
	int32 dst_node; /* node we are moving partition to */
	char *src_connstr;
	char *dst_connstr;
	struct timespec waketm; /* wake me up at waketm to do the job */
	/* We need to epoll only on socket with dst to wait for copy */
	 /* exec_move_mpart sets fd here when it wants to be wakened by epoll */
	int fd_to_epoll;
	int fd_in_epoll_set; /* socket *currently* in epoll set. -1 of none */
	MoveMPartResult result;
} MoveMPartState;

typedef struct
{
	slist_node list_node;
	MoveMPartState *mmps;
} MoveMPartStateNode;

static void init_mmp_state(MoveMPartState *mmps, const char *part_name,
						   int32 dst_node);
static void move_mparts(MoveMPartState *mmpss, int nparts);
static int calc_timeout(struct timespec waketm, bool waketm_set);
static void update_waketm(struct timespec *waketm, bool *waketm_set,
						  MoveMPartState *mmps);
static void epoll_subscribe(int epfd, MoveMPartState *mmps);
static ExecMoveMPartRes exec_move_mpart(MoveMPartState *mmps);

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
	 * we have a deadlock between pathman and pg_dump */
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
 *
 *  If we don't save progress (whether initial sync started or done, lsn,
 *  etc), we have to start everything from the ground if master reboots. This
 *  is arguably fine.
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
		mmps->result = MOVEMPART_FAILED;
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
		mmps->result = MOVEMPART_FAILED;
		return;
	}

	/* Task is ready to be processed right now */
	if ((e = clock_gettime(CLOCK_MONOTONIC, &mmps->waketm)) == -1)
	{
		shmn_elog(FATAL, "clock_gettime failed, %s", strerror(e));
	}
	mmps->fd_to_epoll = -1;
	mmps->fd_in_epoll_set = -1;

	mmps->result = MOVEMPART_IN_PROGRESS;
}

/*
 * Move partitions as specified in move_mpart_states list
 */
void
move_mparts(MoveMPartState *mmpss, int nparts)
{
	/* list of sleeping mmp states we need to wake after specified timeout */
	slist_head timeout_states = SLIST_STATIC_INIT(timeout_states);
	slist_mutable_iter iter;
	/* at least one task will require our attention at waketm */
	struct timespec waketm;
	/* Yes, we could use field of waketm for that. */
	bool waketm_set;
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
	waketm_set = true;
	for (i = 0; i < nparts; i++)
	{
		if (mmpss[i].result != MOVEMPART_FAILED)
		{
			MoveMPartStateNode *mmps_node = palloc(sizeof(MoveMPartStateNode));
			elog(DEBUG4, "Adding task %s to timeout list", mmpss[i].part_name);
			mmps_node->mmps = &mmpss[i];
			slist_push_head(&timeout_states, &mmps_node->list_node);
			unfinished_moves++;
		}
	}

	if ((epfd = epoll_create1(0)) == -1)
		shmn_elog(FATAL, "epoll_create1 failed");

	/* TODO: check for signals */
	while (unfinished_moves > 0)
	{
		timeout = calc_timeout(waketm, waketm_set);
		e = epoll_wait(epfd, evlist, MAX_EVENTS, timeout);
		if (e == -1)
		{
			if (errno == EINTR)
				continue;
			else
				shmn_elog(FATAL, "epoll_wait failed, %s", strerror(e));
		}

		/* Run all tasks for which it is time to wake */
		waketm_set = false; /* reset waketm */
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
				switch (exec_move_mpart(mmps))
				{
					case EXECMOVEMPART_WAKEMEUP:
						/* We need to wake this task again, update waketm and
						 * keep it in the list */
						update_waketm(&waketm, &waketm_set, mmps);
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
				/* And free memory */
				pfree(mmps_node);
			}
		}
	}

	close(epfd);
}

/*
 * Calculate when we need to wake if no epoll events are happening.
 * Returned value is ready for epoll_wait.
 */
int
calc_timeout(struct timespec waketm, bool waketm_set)
{
	int e;
	struct timespec curtm;
	int timeout;

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
	shmn_elog(DEBUG1, "Timeout is %d", timeout);
	return timeout;
}

/*
 * Update min waketm
 */
void
update_waketm(struct timespec *waketm, bool *waketm_set, MoveMPartState *mmps)
{
	if (!(*waketm_set) || timespeccmp(mmps->waketm, *waketm) < 0)
	{
		shmn_elog(DEBUG1, "Waketm updated, old s %d, new s %d",
				  (int) waketm->tv_sec, (int) mmps->waketm.tv_sec);
		*waketm_set = true;
		*waketm = mmps->waketm;
	}
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
 * Actually run MoveMPart state machine. Return value says when (if ever)
 * we want to be executed again.
 */
ExecMoveMPartRes
exec_move_mpart(MoveMPartState *mmps)
{
	shmn_elog(DEBUG1, "Partition %s is moved", mmps->part_name);
	return	EXECMOVEMPART_DONE;
}

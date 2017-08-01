/* -------------------------------------------------------------------------
 *
 * shard.c
 *		Sharding commands implementation.
 *
 * Partitions moving/copying is implemented via LR: we start initial tablesync,
 * wait it for finish, then make src read-only and wait until dst will get
 * current src's lsn.
 *
 * Since we want to execute several actions in parallel, e.g. move partitions,
 * but shardlord works only on one user command at time, we divide commands
 * into 'tasks', e.g. move one partition. Every task it atomic in a sense that
 * it either completes fully or not completes at all (though in latter case we
 * currently leave some garbage here and there which should be cleaned).
 * Parallel execution of tasks is accomplished via event loop: we work on task
 * until it says 'I am done, don't wake me again', 'Wake me again after n
 * sec', or 'Wake me again when data on socket x arrives'. Currently I plan to
 * support the following types of tasks: moving primary, moving replica and
 * creating replicas. Because of parallel execution we may face dependency
 * issues: for example, if we move primary and at the same time add replica
 * by copying this primary to some node, replica might lost some data which
 * as written at new primary location when LR channel between new primary and
 * and replica was not yet established. To simplify things, we will not allow
 * parallel execution of copy part tasks involving the same src partition.
 *
 * We have other issues as well. Imagine the following nodes with primary part
 * on A and replica on B:
 * A --> B
 * |     |
 * C --- D
 * We move in parallel primary (Pr) from A to C and replica (Rp) from B to
 * D. Rr has moved first, Pr second, A quickly learns about this and drops
 * partition & repslot since it has moved to C. Now slow D learns what
 * happened; since Rr move was first, it creates subscription pointing to the
 * table on A, but the repslot doesn't exist anymore, so we will receive a
 * bunch of errors in the log. Happily, this doesn't mean that CREATE
 * SUBSCRIPTION fails, so things will get fixed eventually.
 *
 * As with most actions, we can create/alter/drop pubs, subs and repslots in
 * two ways: via triggers on tables with metadata and manually via libpq.
 * The first is more handy, but dangerous: if pub node crashed, create
 * subscription will fail. We need either patch LR to overcome this or add
 * wrapper which will continiously try to create subscription if it fails.
 *
 * General copy partition implementation:
 * - Disable subscription on destination, otherwise we can't drop rep slot on
 *   source.
 * - Idempotently create publication and repl slot on source.
 * - Idempotently create table and async subscription on destination.
 *   We use async subscription, because sync would block table while copy is
 *   in progress. But with async, we have to lock the table after initial sync.
 * - Now inital copy has started.
 * - Sleep & check in connection to the dest waiting for completion of the
 *   initial sync. Later this should be substituted with listen/notify.
 * - When done, lock writes (better lock reads too to avoid stale reads)
 *	 on source and remember pg_current_wal_lsn() on it.
 * - Now final sync has started.
 * - Sleep & check in connection to dest waiting for completion of final sync,
 *   i.e. when received_lsn is equal to remembered lsn on src. This is harder
 *   to replace with notify, but we can try that too.
 * - Done. src table drop and foreign settings rebuilding is done via metadata
 *   update.
 *
 *  If we don't save progress (whether initial sync started or done, lsn,
 *  etc), we have to start everything from the ground if master reboots. This
 *  is arguably fine.
 *
 *  Short description of all tasks:
 *  move_primary:
 *    copy part, update metadata
 *    On metadata update:
 *    on src node, drop lr copy stuff, create foreign table and replace
 *      table with it, drop table. Drop primary lr stuff.
 *    on dst node, replace foreign table with fresh copy (lock it until
 *      sync_standby_names updated?), drop the former. drop lr copy stuff.
 *      Create primary lr stuff (including sync_standby_names)
 *    On node with replica (if exists) alter sub and alter fdw server.
 *    on others, alter fdw server.
 *
 *  About fdws on replicas: we have to keep partition of parent table as fdw,
 *  because otherwise we would not be able to write anything to it. On the
 *  other hand, keeping the whole list of replicas is a bit excessive and
 *  slower in case of primary failure: we need actually only primary and
 *  ourself.
 *
 *  add_replica:
 *    copy part from the last replica (because only the last replica knows
 *      when it has created sync lr channel and can make table writable again).
 *      Make dst table read-only for all but lr workers, update metadata.
 *    On metadata update:
 *    on (old) last replica, alter cp lr channel to make it sync (and rename),
 *      make table writable.
 *    on node with fresh replica, rename lr channel, alter fdw server.
 *    on others, alter fdw server.
 *
 *  move_replica:
 *    copy part. Make dst table read-only for all but lr worker, update
 *    metadata.
 *    On metadata update:
 *    On src, drop lr copy stuff, alter fdw server. Drop lr pub (if any) and
 *      sub stuff. Drop table.
 *    On dst, drop lr copy stuff, create lr pub & sync sub, alter fdw server.
 *    On previous part node, alter lr channel
 *    On following part node (if any), recreate sub.
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "libpq-fe.h"
#include "access/xlogdefs.h"
#include "utils/pg_lsn.h"
#include "utils/builtins.h"
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

/* Type of task involving partition copy */
typedef enum
{
	COPYPARTTASK_MOVE_PRIMARY,
	COPYPARTTASK_MOVE_REPLICA,
	COPYPARTTASK_ADD_REPLICA
} CopyPartTaskType;

/* final result of 1 one task */
typedef enum
{
	TASK_IN_PROGRESS,
	TASK_FAILED,
	TASK_SUCCESS
} TaskRes;

/* result of one iteration of master partition moving */
typedef enum
{
	TASK_EPOLL, /* add me to epoll on epolled_fd on EPOLLIN */
	TASK_WAKEMEUP, /* wake me up again on waketm */
	TASK_DONE /* the work is done, never invoke me again */
} ExecTaskRes;

/*
 *  Current step of 1 master partition move. See comments to corresponding
 *  funcs, e.g. start_tablesync.
 */
typedef enum
{
	COPYPART_START_TABLESYNC,
	COPYPART_START_FINALSYNC,
	COPYPART_FINALIZE,
	COPYPART_DONE
} CopyPartStep;

/* State of copy part task */
typedef struct
{
	CopyPartTaskType type;
	/* wake me up at waketm to do the job. Try to keep it zero when invalid	*/
	struct timespec waketm;
	/* We need to epoll only on socket with dst to wait for copy */
	 /* exec_copypart sets fd here when it wants to be wakened by epoll */
	int fd_to_epoll;
	int fd_in_epoll_set; /* socket *currently* in epoll set. -1 of none */

	const char *part_name; /* partition name */
	int32 src_node; /* node we are copying partition from */
	int32 dst_node; /* node we are copying partition to */
	const char *src_connstr;
	const char *dst_connstr;
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
	char *substate_sql; /* get current state of subscription */
	char *readonly_sql; /* make src table read-only */
	char *received_lsn_sql; /* get last received lsn on dst */
	char *update_metadata_sql;

	XLogRecPtr sync_point; /* when dst reached this point, it is synced */
	CopyPartStep curstep; /* current step */
	ExecTaskRes exec_res; /* result of the last iteration */
	TaskRes res; /* result of the whole move */
} CopyPartState;

typedef struct
{
	slist_node list_node;
	CopyPartState *cps;
} CopyPartStateNode;

static void init_cp_state(CopyPartState *cps, const char *part_name,
						   int32 dst_node);
static void init_mpp_state(CopyPartState *cps, const char *part_name,
						   int32 dst_node);
static void finalize_cp_state(CopyPartState *cps);
static void exec_tasks(CopyPartState **tasks, int ntasks);
static int calc_timeout(slist_head *timeout_states);
static void epoll_subscribe(int epfd, CopyPartState *cps);
static void exec_task(CopyPartState *cps);
static void exec_cp(CopyPartState *cps);
static void exec_move_primary(CopyPartState *cps);
static void exec_add_replica(CopyPartState *cps);
static void exec_move_replica(CopyPartState *cps);
static int cp_start_tablesync(CopyPartState *cpts);
static int cp_start_finalsync(CopyPartState *cpts);
static int cp_finalize(CopyPartState *cpts);
static int ensure_pqconn(CopyPartState *cpts, int nodes);
static int ensure_pqconn_intern(PGconn **conn, const char *connstr,
								CopyPartState *cps);
static void reset_pqconn(PGconn **conn);
static void reset_pqconn_and_res(PGconn **conn, PGresult *res);
static void configure_retry(CopyPartState *cpts, int millis);
static struct timespec timespec_now_plus_millis(int millis);
struct timespec timespec_now(void);

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
 * Move primary partition.
 */
void
move_primary(Cmd *cmd)
{
	char *part_name = cmd->opts[0];
	int32 dst_node = atoi(cmd->opts[1]);

	CopyPartState **tasks = palloc(sizeof(CopyPartState*));
	CopyPartState *cps = palloc(sizeof(CopyPartState));
	tasks[0] = cps;
	init_mpp_state(cps, part_name, dst_node);

	exec_tasks(tasks, 1);
	check_for_sigterm();
	if (got_sigusr1)
	{
		cmd_canceled(cmd);
		return;
	}

	Assert(cps->res != TASK_IN_PROGRESS);
	if (cps->res == TASK_FAILED)
		update_cmd_status(cmd->id, "failed");
	else if (cps->res == TASK_SUCCESS)
		update_cmd_status(cmd->id, "success");
}

/*
 * Fill CopyPartState for moving primary partition
 */
void
init_mpp_state(CopyPartState *cps, const char *part_name, int32 dst_node)
{
	if ((cps->src_node = get_primary_owner(part_name)) == -1)
	{
		shmn_elog(WARNING, "Partition %s doesn't exist, not moving it",
				  part_name);
		cps->res = TASK_FAILED;
		return;
	}
	cps->update_metadata_sql = psprintf(
		"update shardman.partitions set owner = %d where part_name = '%s'"
		" and num = 0;",
		dst_node, part_name);
	cps->type = COPYPARTTASK_MOVE_PRIMARY;
	/* The rest fields are common among copy part tasks */
	init_cp_state(cps, part_name, dst_node);
}

/*
 * Fill CopyPartState, retrieving needed data. If something goes wrong, we
 * don't bother to fill the rest of fields and mark task as failed.
 */
void
init_cp_state(CopyPartState *cps, const char *part_name, int32 dst_node)
{
	/* Task is ready to be processed right now */
	cps->waketm = timespec_now();
	cps->fd_to_epoll = -1;
	cps->fd_in_epoll_set = -1;

	cps->part_name = part_name;
	cps->dst_node = dst_node;
	/* src_connstr is surely not NULL since src_node is referenced by
	   part_name */
	cps->src_connstr = get_worker_node_connstr(cps->src_node);
	cps->dst_connstr = get_worker_node_connstr(cps->dst_node);
	if (cps->dst_connstr == NULL)
	{
		shmn_elog(WARNING, "Node %d doesn't exist, not copying %s to it",
				  cps->dst_node, part_name);
		cps->res = TASK_FAILED;
		return;
	}

	cps->src_conn = NULL;
	cps->dst_conn = NULL;

	/* constant strings */
	cps->logname = psprintf("shardman_copy_%s_%d_%d",
							 cps->part_name, cps->src_node, cps->dst_node);
	cps->dst_drop_sub_sql = psprintf(
		"drop subscription if exists %s cascade;", cps->logname);
	/*
	 * Note that we run stmts in separate txns: repslot can't be created in in
	 * transaction that performed writes
	 */
	cps->src_create_pub_and_rs_sql = psprintf(
		"begin; drop publication if exists %s cascade;"
		" create publication %s for table %s; end;"
		" select shardman.create_repslot('%s');",
		cps->logname, cps->logname, cps->part_name, cps->logname
		);
	cps->relation = get_partition_relation(part_name);
	Assert(cps->relation != NULL);
	cps->dst_create_tab_and_sub_sql = psprintf(
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
		cps->part_name,
		cps->part_name, cps->relation,
		cps->logname,
		cps->logname, cps->src_connstr, cps->logname, cps->logname);
	cps->substate_sql = psprintf(
		"select srsubstate from pg_subscription_rel srel join pg_subscription"
		" s on srel.srsubid = s.oid where subname = '%s';",
		cps->logname
		);
	cps->readonly_sql = psprintf(
		"select shardman.readonly_table_on('%s')", cps->part_name
		);
	cps->received_lsn_sql = psprintf(
		"select received_lsn from pg_stat_subscription where subname = '%s'",
		cps->logname
		);

	cps->curstep = COPYPART_START_TABLESYNC;
	cps->res = TASK_IN_PROGRESS;
}

/*
 * Close pq connections, if any.
 */
static void finalize_cp_state(CopyPartState *cps)
{
	if (cps->src_conn != NULL)
		reset_pqconn(&cps->src_conn);
	if (cps->dst_conn != NULL)
		reset_pqconn(&cps->dst_conn);
}

/*
 * Execute tasks specified in 'tasks' array of ptrs to CopyPartState
 * structs. Currently the only tasks we support involve copying parts; later,
 * if needed, we can easily generalize this by excluding common task state
 * from CopyPartState to separate struct and inheriting from it.  Results (and
 * general state) is saved in this array too. Executes tasks until all have
 * have failed/succeeded or sigusr1/sigterm is caugth.
 *
 */
void
exec_tasks(CopyPartState **tasks, int ntasks)
{
	/* list of sleeping cp states we need to wake after specified timeout */
	slist_head timeout_states = SLIST_STATIC_INIT(timeout_states);
	slist_mutable_iter iter;
	/* at least one task will require our attention at waketm */
	struct timespec curtm;
	int timeout;
	int unfinished_tasks = 0; /* number of not yet failed or succeeded tasks */
	int i;
	int e;
	int epfd;
	struct epoll_event evlist[MAX_EVENTS];

	/*
	 * In the beginning, all tasks are ready for execution, so we need to put
	 * all tasks to the timeout_states list to invoke them.
	 */
	for (i = 0; i < ntasks; i++)
	{
		/* TODO: make sure one part is touched only by one task */
		if (tasks[i]->res != TASK_FAILED)
		{
			CopyPartStateNode *cps_node = palloc(sizeof(CopyPartStateNode));
			elog(DEBUG2, "Adding task %s to timeout lst", tasks[i]->part_name);
			cps_node->cps = tasks[i];
			slist_push_head(&timeout_states, &cps_node->list_node);
			unfinished_tasks++;
		}
	}

	if ((epfd = epoll_create1(0)) == -1)
		shmn_elog(FATAL, "epoll_create1 failed");

	while (unfinished_tasks > 0 && !got_sigusr1 && !got_sigterm)
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
			CopyPartStateNode *cps_node =
				slist_container(CopyPartStateNode, list_node, iter.cur);
			CopyPartState *cps = cps_node->cps;
			curtm = timespec_now();

			if (timespeccmp(cps->waketm, curtm) <= 0)
			{
				shmn_elog(DEBUG1, "%s is ready for exec", cps->part_name);
				exec_task(cps);
				switch (cps->exec_res)
				{
					case TASK_WAKEMEUP:
						/* We need to wake this task again, to keep it in
						 * in the list and just continue */
						continue;

					case TASK_EPOLL:
						/* Task wants to be wakened by epoll */
						epoll_subscribe(epfd, cps);
						break;

					case TASK_DONE:
						/* Task is done, decrement the counter */
						unfinished_tasks--;
						break;
				}
				/* If we are still here, remove node from timeouts_list */
				slist_delete_current(&iter);
				/* And free node */
				pfree(cps_node);
			}
		}
	}

	/*
	 * Free list. This not necessary though, we are finishing cmd and
	 * everything will be freed soon.
	 */
	slist_foreach_modify(iter, &timeout_states)
	{
		CopyPartStateNode *cps_node =
				slist_container(CopyPartStateNode, list_node, iter.cur);
		slist_delete_current(&iter);
		pfree(cps_node);
	}
	/* But this is important, as libpq manages memory on its own */
	for (i = 0; i < ntasks; i++)
		finalize_cp_state(tasks[i]);
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
	struct timespec curtm;
	int timeout;
	/* could use timespec field for this, but that's more readable */
	bool waketm_set = false;
	struct timespec waketm; /* min of all waketms */

	/* calc min waketm */
	slist_foreach(iter, timeout_states)
	{
		CopyPartStateNode *cps_node =
			slist_container(CopyPartStateNode, list_node, iter.cur);
		CopyPartState *cps = cps_node->cps;

		/* If waketm is not set, what this node does in this list? */
		Assert(cps->waketm.tv_nsec != 0);
		if (!waketm_set || timespeccmp(cps->waketm, waketm) < 0)
		{
			shmn_elog(DEBUG5, "Waketm updated, old %d s, new %d s",
					  waketm_set ? (int) waketm.tv_sec : 0,
					  (int) cps->waketm.tv_sec);
			waketm = cps->waketm;
			waketm_set = true;
		}

	}

	/* now calc timeout */
	if (!waketm_set)
		return -1;

	curtm = timespec_now();
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
 * Ensure that cps is registered in epoll and set proper mode.
 * We never remove fds from epoll, they should be removed automatically when
 * closed.
 */
void
epoll_subscribe(int epfd, CopyPartState *cps)
{
	struct epoll_event ev;
	int e;

	ev.data.ptr = cps;
	ev.events = EPOLLIN | EPOLLONESHOT;
	Assert(cps->fd_to_epoll != -1);
	if (cps->fd_to_epoll == cps->fd_in_epoll_set)
	{
		if ((e = epoll_ctl(epfd, EPOLL_CTL_MOD, cps->fd_to_epoll, &ev)) == -1)
			shmn_elog(FATAL, "epoll_ctl failed, %s", strerror(e));
	}
	else
	{
		if ((e = epoll_ctl(epfd, EPOLL_CTL_ADD, cps->fd_to_epoll, &ev)) == -1)
			shmn_elog(FATAL, "epoll_ctl failed, %s", strerror(e));
		cps->fd_in_epoll_set = cps->fd_to_epoll;
	}
	shmn_elog(DEBUG1, "socket for task %s added to epoll", cps->part_name);
}

/*
 * One iteration of task execution
 */
void
exec_task(CopyPartState *cps)
{
	switch (cps->type)
	{
		case COPYPARTTASK_MOVE_PRIMARY:
			exec_move_primary(cps);
			break;

		case COPYPARTTASK_ADD_REPLICA:
			exec_add_replica(cps);
			break;

		case COPYPARTTASK_MOVE_REPLICA:
			exec_move_replica(cps);
			break;
	}
}

/*
 * One iteration of move primary task execution
 */
void
exec_move_primary(CopyPartState *cps)
{
	if (cps->curstep != COPYPART_DONE)
	{
		exec_cp(cps);
		if (cps->curstep != COPYPART_DONE)
			return;
	}

	void_spi(cps->update_metadata_sql);
	cps->res = TASK_SUCCESS;
	cps->exec_res = TASK_DONE;
}

/*
 * One iteration of add replica task execution
 */
void
exec_add_replica(CopyPartState *cps)
{

}

/*
 * One iteration of move replica task execution
 */
void
exec_move_replica(CopyPartState *cps)
{

}

/*
 * Actually run CopyPartState state machine. On return, cps values say when (if
 * ever) we want to be executed again.
 */
void
exec_cp(CopyPartState *cps)
{
	/* Mark waketm as invalid for safety */
	cps->waketm = (struct timespec) {0};

	if (cps->curstep == COPYPART_START_TABLESYNC)
	{
		if (cp_start_tablesync(cps) == -1)
			return;
	}
	if (cps->curstep == COPYPART_START_FINALSYNC)
	{
		if (cp_start_finalsync(cps) == -1)
			return;
	}
	if (cps->curstep == COPYPART_FINALIZE)
		cp_finalize(cps);
	return;
}

/*
 * Set up logical replication between src and dst. If anything goes wrong,
 * it configures cps properly and returns -1, otherwise 0.
 */
int
cp_start_tablesync(CopyPartState *cps)
{
	PGresult *res;

	if (ensure_pqconn(cps, ENSURE_PQCONN_SRC | ENSURE_PQCONN_DST) == -1)
		return -1;

	res = PQexec(cps->dst_conn, cps->dst_drop_sub_sql);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		shmn_elog(NOTICE, "Failed to drop sub on dst: %s",
				  PQerrorMessage(cps->dst_conn));
		reset_pqconn_and_res(&cps->dst_conn, res);
		configure_retry(cps, shardman_cmd_retry_naptime);
		return -1;
	}
	PQclear(res);
	shmn_elog(DEBUG1, "cp %s: sub on dst dropped, if any", cps->part_name);

	res = PQexec(cps->src_conn, cps->src_create_pub_and_rs_sql);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		shmn_elog(NOTICE, "Failed to create pub and repslot on src: %s",
				  PQerrorMessage(cps->src_conn));
		reset_pqconn_and_res(&cps->src_conn, res);
		configure_retry(cps, shardman_cmd_retry_naptime);
		return -1;
	}
	PQclear(res);
	shmn_elog(DEBUG1, "cp %s: pub and rs recreated on src", cps->part_name);

	res = PQexec(cps->dst_conn, cps->dst_create_tab_and_sub_sql);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		shmn_elog(NOTICE, "Failed to recreate table & sub on dst: %s",
				  PQerrorMessage(cps->dst_conn));
		reset_pqconn_and_res(&cps->dst_conn, res);
		configure_retry(cps, shardman_cmd_retry_naptime);
		return -1;
	}
	PQclear(res);
	shmn_elog(DEBUG1, "cp %s: table & sub created on dst, tablesync started",
			  cps->part_name);

	cps->curstep = COPYPART_START_FINALSYNC;
	return 0;
}

/*
 * - wait until initial sync is done;
 * - make src read only and save its pg_current_wal() in cps;
 * - now we are ready to wait for final sync
 * Returns -1 if anything goes wrong and 0 otherwise. current wal is saved
 * in cps.
 */
int
cp_start_finalsync(CopyPartState *cps)
{
	PGresult *res;
	int ntups;
	char substate;
	char *sync_point;

	if (ensure_pqconn(cps, ENSURE_PQCONN_SRC | ENSURE_PQCONN_DST) == -1)
		return -1;

	res = PQexec(cps->dst_conn, cps->substate_sql);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		shmn_elog(NOTICE, "Failed to learn sub status on dst: %s",
				  PQerrorMessage(cps->dst_conn));
		reset_pqconn_and_res(&cps->dst_conn, res);
		configure_retry(cps, shardman_cmd_retry_naptime);
		return -1;
	}
	ntups = PQntuples(res);
	if (ntups != 1)
	{
		shmn_elog(WARNING, "cp %s: num of subrels != 1", cps->part_name);
		/*
		 * Since several or 0 subrels is absolutely wrong situtation, we start
		 * from the beginning.
		 */
		cps->curstep =	COPYPART_START_TABLESYNC;
		configure_retry(cps, shardman_cmd_retry_naptime);
		return -1;
	}
	substate = PQgetvalue(res, 0, 0)[0];
	if (substate != 'r')
	{
		shmn_elog(DEBUG1, "cp %s: init sync is not yet finished, its state"
				  " is %c", cps->part_name, substate);
		configure_retry(cps, shardman_poll_interval);
		return -1;
	}
	shmn_elog(DEBUG1, "cp %s: init sync finished", cps->part_name);
	PQclear(res);

	res = PQexec(cps->src_conn, cps->readonly_sql);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		shmn_elog(NOTICE, "Failed to make src table read only: %s",
				  PQerrorMessage(cps->src_conn));
		reset_pqconn_and_res(&cps->src_conn, res);
		configure_retry(cps, shardman_cmd_retry_naptime);
		return -1;
	}
	shmn_elog(DEBUG1, "cp %s: src made read only", cps->part_name);
	PQclear(res);

	res = PQexec(cps->src_conn, "select pg_current_wal_lsn();");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		shmn_elog(NOTICE, "Failed to get current lsn on src: %s",
				  PQerrorMessage(cps->src_conn));
		reset_pqconn_and_res(&cps->src_conn, res);
		configure_retry(cps, shardman_cmd_retry_naptime);
		return -1;
	}
	sync_point = PQgetvalue(res, 0, 0);
    cps->sync_point = DatumGetLSN(DirectFunctionCall1Coll(pg_lsn_in, InvalidOid,
											   CStringGetDatum(sync_point)));
	shmn_elog(DEBUG1, "cp %s: sync lsn is %s", cps->part_name, sync_point);
	PQclear(res);

	cps->curstep = COPYPART_FINALIZE;
	return 0;
}

/*
 * Wait until final sync is done and update metadata. Returns -1 if anything
 * goes wrong and 0 otherwise.
 */
int
cp_finalize(CopyPartState *cps)
{

	PGresult *res;
	XLogRecPtr received_lsn;
	char *received_lsn_str;

	if (ensure_pqconn(cps, ENSURE_PQCONN_DST) == -1)
		return -1;

	res = PQexec(cps->dst_conn, cps->received_lsn_sql);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		shmn_elog(NOTICE, "Failed to learn received_lsn on dst: %s",
				  PQerrorMessage(cps->dst_conn));
		reset_pqconn_and_res(&cps->dst_conn, res);
		configure_retry(cps, shardman_cmd_retry_naptime);
		return -1;
	}
	received_lsn_str = PQgetvalue(res, 0, 0);
	shmn_elog(DEBUG1, "cp %s: received_lsn is %s", cps->part_name,
			  received_lsn_str);
	received_lsn = DatumGetLSN(DirectFunctionCall1Coll(
								   pg_lsn_in, InvalidOid,
								   CStringGetDatum(received_lsn_str)));
	PQclear(res);
	if (received_lsn < cps->sync_point)
	{
		shmn_elog(DEBUG1, "cp %s: final sync is not yet finished,"
				  "received_lsn is %lu, but we wait for %lu",
				  cps->part_name, received_lsn, cps->sync_point);
		configure_retry(cps, shardman_poll_interval);
		return -1;
	}

	cps->curstep = COPYPART_DONE;
	shmn_elog(DEBUG1, "Partition %s %d->%d successfully copied",
			  cps->part_name, cps->src_node, cps->dst_node);
	return 0;
}

/*
 * Ensure that pq connection to src and dst node is CONNECTION_OK. nodes
 * is a bitmask specifying with which nodes -- src, dst or both -- connection
 * must be ensured. -1 is returned if we have failed to establish connection;
 * cps is then configured to sleep retry time. 0 is returned if ok.
 */
int
ensure_pqconn(CopyPartState *cps, int nodes)
{
	if ((nodes & ENSURE_PQCONN_SRC) &&
		(ensure_pqconn_intern(&cps->src_conn, cps->src_connstr, cps) == -1))
		return -1;
	if ((nodes & ENSURE_PQCONN_DST) &&
		(ensure_pqconn_intern(&cps->dst_conn, cps->dst_connstr, cps) == -1))
		return -1;
	return 0;
}

/*
 * Working horse of ensure_pqconn
 */
int
ensure_pqconn_intern(PGconn **conn, const char *connstr,
					   CopyPartState *cps)
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
			configure_retry(cps, shardman_cmd_retry_naptime);
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
 * Configure cps so that main loop wakes us again after given retry millis.
 */
void configure_retry(CopyPartState *cps, int millis)
{
	shmn_elog(DEBUG1, "Copying mpart %s: sleeping %d ms and retrying",
			  cps->part_name, millis);
	cps->waketm = timespec_now_plus_millis(millis);
	cps->exec_res = TASK_WAKEMEUP;
}

/*
 * Get current CLOCK_MONOTONIC time. Fails with PG elog(FATAL) if gettime
 * failed.
 */
struct timespec timespec_now(void)
{
	int e;
	struct timespec t;

	if ((e = clock_gettime(CLOCK_MONOTONIC, &t)) == -1)
		shmn_elog(FATAL, "clock_gettime failed, %s", strerror(e));

	return t;
}

/*
 * Get current time + given milliseconds. Fails with PG elog(FATAL) if gettime
 * failed.
 */
struct timespec timespec_now_plus_millis(int millis)
{
	struct timespec t = timespec_now();
	return timespec_add_millis(t, millis);
}

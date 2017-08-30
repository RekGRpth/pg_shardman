/* -------------------------------------------------------------------------
 *
 * copypart.c
 *		Implementation of sharding commands involving partition copy.
 *
 * Partitions moving/copying is implemented via LR: we start initial tablesync,
 * wait it for finish, then make src read-only and wait until dst will get
 * current src's lsn.
 *
 * Since we want to execute several actions in parallel, e.g. move partitions,
 * but shardlord works only on one user command at time, we divide commands
 * into 'tasks', e.g. move one partition. Every task is atomic in a sense that
 * it either completes fully or not completes at all (though in latter case we
 * currently leave some garbage here and there which should be cleaned).
 * Parallel execution of tasks is accomplished via event loop: we work on task
 * until it says 'I am done, don't wake me again', 'Wake me again after n
 * sec', or 'Wake me again when data on socket x arrives'. Currently the
 * following types of tasks are supported: moving part (primary or replica)
 * and creating replicas. Because of parallel execution we may face dependency
 * issues: for example, if we move primary and at the same time add replica by
 * copying this primary to some node, replica might lost some data which has
 * written at new primary location when LR channel between new primary and and
 * replica was not yet established. To simplify things, we should not allow
 * parallel execution of copy part tasks involving the same src partition, but
 * this is not yet checked.
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
 * table on A, but the repslot doesn't exist anymore, so we will see a
 * bunch of errors in the log. Happily, this doesn't mean that CREATE
 * SUBSCRIPTION fails, so things will get fixed eventually.
 *
 * As with most actions, we can create/alter/drop pubs, subs and repslots in
 * two ways: via triggers on tables with metadata and manually via libpq.  The
 * first is more handy, but dangerous: if pub node crashed, create
 * subscription will fail. We need either patch LR to overcome this or add
 * wrapper which will continiously try to create subscription if it fails.
 * Besides, there is no way to create logical replication slot if current trxn
 * had written something, and so it is impossible to do that from trigger on
 * update. The moral is that we manage LR only manually.
 *
 * As always, implementations must be written atomically, so that if anything
 * reboots, things are not broken. This requires special attention while
 * handling LR channels: it means that we can't touch old LR channels while
 * metadata is not yet updated, and we update metadata only when all new
 * channels are built. So we configure new channels first manually, then
 * update metadata, and finally destroy old channels in update metadata
 * triggers.

 * Often, while altering LR channel, we need to change only publisher or only
 * subscriber, or rename endpoints. One might think that we could reuse sub or
 * pub/repslot in such cases. No, it is a bad idea. First of all, tt is
 * impossible to rename logical repslot, so we are drop old and create new one
 * if we need to rename it. Then, we can't reuse old replication slot if we
 * change subscription, because when we create new sub, old is normally alive
 * (because of the atomicity), and two subs per one replication slot doesn't sound
 * good. Renaming subs is not easy too:
 * - It is not easier than creating a new one: we have to rename sub, alter
 *   sub's slot_name, alter sub's publication, probably update sub application
 *   name, probably run REFRESH (which requires alive pub just as CREATE
 *   SUBSCRIPTION) and hope that everything will be ok. Not sure about
 *   refreshing, though -- I don't know is it ok not doing it if tables didn't
 *   change. Doc says it should be executed.
 * - Since it is not possible to rename repslot and and it is not possible to
 *   specify since which lsn start replication, tables must be synced anyway
 *   during these operations, so what the point of reusing old sub?
 *
 *  About fdws on replicas: we have to keep partition of parent table as fdw,
 *  because otherwise we would not be able to write anything to it. On the
 *  other hand, keeping the whole list of replicas is a bit excessive and
 *  slower in case of primary failure: we need actually only primary and
 *  ourself.
 *
 *  Currently we don't save progress of separate tasks (e.g. for copy part
 *  whether initial sync started or done, lsn, etc), so we have to start
 *  everything from the ground if shardlord reboots. This is arguably fine.
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

#include "copypart.h"
#include "timeutils.h"

/* epoll max events */
#define MAX_EVENTS 64

/* Bitmask for ensure_pqconn */
#define ENSURE_PQCONN_SRC (1 << 0)
#define ENSURE_PQCONN_DST (1 << 1)

typedef struct
{
	slist_node list_node;
	CopyPartState *cps;
} CopyPartStateNode;

static void init_cp_state(CopyPartState *cps);
static void finalize_cp_state(CopyPartState *cps);
static int calc_timeout(slist_head *timeout_states);
static void epoll_subscribe(int epfd, CopyPartState *cps);
static void exec_task(CopyPartState *cps);
static void exec_cp(CopyPartState *cps);
static void exec_move_part(MovePartState *cps);
static void exec_create_replica(CreateReplicaState *cps);
static int mp_rebuild_lr(MovePartState *cps);
static int cr_rebuild_lr(CreateReplicaState *cps);
static int cp_start_tablesync(CopyPartState *cpts);
static int cp_start_finalsync(CopyPartState *cpts);
static int cp_finalize(CopyPartState *cpts);
static int ensure_pqconn_cp(CopyPartState *cpts, int nodes);
static int ensure_pqconn(PGconn **conn, const char *connstr,
								CopyPartState *cps);
static void configure_retry(CopyPartState *cpts, int millis);
static struct timespec timespec_now_plus_millis(int millis);
struct timespec timespec_now(void);

/*
 * Fill MovePartState for moving partition. If src_node is
 * SHMN_INVALID_NODE_ID, assume primary partition must be moved. If something
 * goes wrong, we don't bother to fill the rest of fields and mark task as
 * failed.
 */
void
init_mp_state(MovePartState *mps, const char *part_name, int32 src_node,
			  int32 dst_node)
{
	/* Set up fields neccesary to call init_cp_state */
	mps->cp.part_name = part_name;
	if (src_node == SHMN_INVALID_NODE_ID)
	{
		if ((mps->cp.src_node = get_primary_owner(part_name)) ==
			SHMN_INVALID_NODE_ID)
		{
			shmn_elog(WARNING, "Partition %s doesn't exist, not moving it",
					  part_name);
			mps->cp.res = TASK_FAILED;
			return;
		}
		mps->cp.type = COPYPARTTASK_MOVE_PRIMARY;
		mps->prev_node = SHMN_INVALID_NODE_ID;
	}
	else
	{
		bool part_exists;
		/*
		 * Make sure that part exists on src node and get prev at the same
		 * time to see whether it is a primary or no.
		 */
		mps->prev_node = get_prev_node(part_name, src_node, &part_exists);
		if (!part_exists)
		{
			shmn_elog(WARNING, "There is no partition %s on node %d, not moving it",
					  part_name, src_node);
			mps->cp.res = TASK_FAILED;
			return;
		}
		mps->cp.src_node = src_node;
		if (mps->prev_node == SHMN_INVALID_NODE_ID)
		{
			mps->cp.type = COPYPARTTASK_MOVE_PRIMARY;
			mps->prev_node = SHMN_INVALID_NODE_ID;
		}
		else
		{
			mps->cp.type = COPYPARTTASK_MOVE_REPLICA;
			mps->prev_connstr = get_worker_node_connstr(mps->prev_node);
		}
	}
	mps->cp.dst_node = dst_node;

	/* Fields common among copy part tasks */
	init_cp_state((CopyPartState *) mps);
	if (mps->cp.res == TASK_FAILED)
		return;

	if ((mps->next_node = get_next_node(mps->cp.part_name, mps->cp.src_node))
		!= SHMN_INVALID_NODE_ID)
	{
		/*
		 * This part has replica, so after moving part we have to
		 * reconfigure LR channel properly.
		 */
		mps->next_connstr = get_worker_node_connstr(mps->next_node);
	}

	mps->cp.update_metadata_sql = psprintf(
		"update shardman.partitions set owner = %d where part_name = '%s'"
		" and owner = %d;"
		" update shardman.partitions set nxt = %d where part_name = '%s'"
		" and nxt = %d;" /* prev replica */
		" update shardman.partitions set prv = %d where part_name = '%s'"
		" and prv = %d;", /* next replica */
		mps->cp.dst_node, part_name, mps->cp.src_node,
		mps->cp.dst_node, part_name, mps->cp.src_node,
		mps->cp.dst_node, part_name, mps->cp.src_node);

	if (mps->prev_node != SHMN_INVALID_NODE_ID)
	{
		mps->prev_sql = psprintf(
			"select shardman.part_moved_prev('%s', %d, %d);",
			part_name, mps->cp.src_node, mps->cp.dst_node);
	}
	mps->dst_sql = psprintf(
		"select shardman.part_moved_dst('%s', %d, %d);",
		part_name, mps->cp.src_node, mps->cp.dst_node);
	if (mps->next_node != SHMN_INVALID_NODE_ID)
	{
		mps->next_sql = psprintf(
			"select shardman.part_moved_next('%s', %d, %d);",
			part_name, mps->cp.src_node, mps->cp.dst_node);
	}
}

/*
 * Fill CopyPartState for creating replica. If something goes wrong, we don't
 * bother to fill the rest of fields and mark task as failed.
 */
void
init_cr_state(CreateReplicaState *crs, const char *part_name, int32 dst_node)
{
	/* Set up fields neccesary to call init_cp_state */
	crs->cp.dst_node = dst_node;
	crs->cp.part_name = part_name;
	if ((crs->cp.src_node = get_reptail_owner(part_name)) == SHMN_INVALID_NODE_ID)
	{
		shmn_elog(WARNING, "Primary part %s doesn't exist, not creating"
				  "replica for it it", part_name);
		crs->cp.res = TASK_FAILED;
		return;
	}

	/* Fields common among copy part tasks */
	init_cp_state((CopyPartState *) crs);
	if (crs->cp.res == TASK_FAILED)
		return;

	crs->cp.update_metadata_sql = psprintf(
		"insert into shardman.partitions values "
		" ('%s', %d, %d, NULL, '%s');"
		" update shardman.partitions set nxt = %d where part_name = '%s' and "
		" owner = %d",
		part_name, dst_node, crs->cp.src_node, crs->cp.relation,
		dst_node, part_name, crs->cp.src_node);
	crs->cp.type = COPYPARTTASK_CREATE_REPLICA;

	crs->drop_cp_sub_sql = psprintf(
		"select shardman.replica_created_drop_cp_sub('%s', %d, %d);",
		part_name, crs->cp.src_node, crs->cp.dst_node);
	/*
	 * Separate trxn for ensure_sync_standby as in init_mp_state. It is
	 * interesting that while I got expected behaviour (hanged transaction) in
	 * move_part if ensure_sync_standby was executed in one trxn with create
	 * repslot and pub, here I didn't. Probably it is committing so fast that
	 * settings are not getting reloaded, but not sure why.
	 */
	crs->create_data_pub_sql = psprintf(
		"select shardman.replica_created_create_data_pub('%s', %d, %d);",
		part_name, crs->cp.src_node, crs->cp.dst_node);
	crs->create_data_sub_sql = psprintf(
		"select shardman.replica_created_create_data_sub('%s', %d, %d);",
		part_name, crs->cp.src_node, crs->cp.dst_node);
}

/*
 * Fill CopyPartState, retrieving needed data. If something goes wrong, we
 * don't bother to fill the rest of fields and mark task as failed.
 * src_node, dst_node and part_name must be already set when called. src_node
 * and dst_node must exits.
 */
void
init_cp_state(CopyPartState *cps)
{
	uint64 shard_exists;
	char *sql;

	Assert(cps->src_node != 0);
	Assert(cps->dst_node != 0);
	Assert(cps->part_name != NULL);

	/* Check that table with such name is not already exists on dst node */
	sql = psprintf(
		"select owner from shardman.partitions where part_name = '%s' and owner = %d",
		cps->part_name, cps->dst_node);
	shard_exists = void_spi(sql);
	if (shard_exists)
	{
		shmn_elog(WARNING,
				  "Shard %s already exists on node %d, won't copy it from %d.",
				  cps->part_name, cps->dst_node, cps->src_node);
		cps->res = TASK_FAILED;
		return;
	}

	Assert(cps->part_name != NULL);
	/* Task is ready to be processed right now */
	cps->waketm = timespec_now();
	cps->fd_to_epoll = -1;
	cps->fd_in_epoll_set = -1;

	cps->src_connstr = get_worker_node_connstr(cps->src_node);
	Assert(cps->src_connstr != NULL);
	cps->dst_connstr = get_worker_node_connstr(cps->dst_node);
	Assert(cps->dst_connstr != NULL);

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
	cps->relation = get_partition_relation(cps->part_name);
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
	if (cps->type == COPYPARTTASK_MOVE_PRIMARY ||
		cps->type == COPYPARTTASK_MOVE_REPLICA)
	{
		MovePartState *mps = (MovePartState *) cps;
		if (mps->prev_conn != NULL)
			reset_pqconn(&mps->prev_conn);
		if (mps->next_conn != NULL)
			reset_pqconn(&mps->next_conn);
	}
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

	while (unfinished_tasks > 0 && !signal_pending())
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
						/* We need to wake this task again, so keep it in
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

	/* Free timeout_states list */
	slist_foreach_modify(iter, &timeout_states)
	{
		CopyPartStateNode *cps_node =
				slist_container(CopyPartStateNode, list_node, iter.cur);
		slist_delete_current(&iter);
		pfree(cps_node);
	}
	/* libpq manages memory on its own */
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
		Assert(!(cps->waketm.tv_sec == 0 && cps->waketm.tv_nsec == 0));
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
		case COPYPARTTASK_CREATE_REPLICA:
			exec_create_replica((CreateReplicaState *) cps);
			break;

		case COPYPARTTASK_MOVE_PRIMARY:
		case COPYPARTTASK_MOVE_REPLICA:
			exec_move_part((MovePartState *) cps);
			break;
	}
}

/*
 * One iteration of move partition task execution.
 *
 * Maximum 4 nodes are actively involved here: src, dst, previous replica (or
 * primary) and next replica. The whole task workflow:
 * - copy part
 * - create pub, repslot, turn on sync rep for prev -> dst channel
 * - create pub, repslot, turn on sync rep for dst -> next channel
 * - create sub for prev -> dst channel
 * - create sub from dst -> next channel
 * - update metadata, in triggers:
 *   * Update fdw connstrings;
 *   * Replace foreign table with new part on dst (dropping the former) and
       old part with foreign on src (dropping the former),
 *   * Drop all old LR stuff via update metadata triggers.
 *   * Replication channel used for copy is dropped here too.
 */
void
exec_move_part(MovePartState *mps)
{
	exec_cp((CopyPartState *) mps);
	if (mps->cp.curstep != COPYPART_DONE)
		return;

	if (((mps->next_node != SHMN_INVALID_NODE_ID) ||
		 mps->prev_node != SHMN_INVALID_NODE_ID) && (mp_rebuild_lr(mps) == -1))
		return;

	void_spi(mps->cp.update_metadata_sql);
	shmn_elog(LOG, "Part move %s: %d -> %d successfully done",
			  mps->cp.part_name, mps->cp.src_node, mps->cp.dst_node);
	mps->cp.res = TASK_SUCCESS;
	mps->cp.exec_res = TASK_DONE;
}

/*
 * Reconfigure LR channel for moved primary: prev to moved, moved to next or
 * both, if they exist.
 *
 * We execute code on nodes in the following order: prev, dst, next, so that
 * every time we create sub, pub already exists.
 */
int
mp_rebuild_lr(MovePartState *mps)
{
	PGresult *res;

	if (mps->prev_node != SHMN_INVALID_NODE_ID)
	{
		if (ensure_pqconn(&mps->prev_conn, mps->prev_connstr,
					   (CopyPartState *) mps) == -1)
			return -1;
		res = PQexec(mps->prev_conn, mps->prev_sql);
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			shmn_elog(NOTICE, "Moving part %s: failed to configure LR on prev replica: %s",
					  mps->cp.part_name, PQerrorMessage(mps->prev_conn));
			reset_pqconn_and_res(&mps->prev_conn, res);
			configure_retry((CopyPartState *) mps, shardman_cmd_retry_naptime);
			return -1;
		}
		PQclear(res);
		shmn_elog(DEBUG1, "mp %s: LR conf on prev done", mps->cp.part_name);
	}

	if (ensure_pqconn_cp((CopyPartState *) mps,
						 ENSURE_PQCONN_DST) == -1)
		return -1;
	res = PQexec(mps->cp.dst_conn, mps->dst_sql);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		shmn_elog(NOTICE, "Moving part %s: failed to configure LR on dst : %s",
				  mps->cp.part_name, PQerrorMessage(mps->cp.dst_conn));
		reset_pqconn_and_res(&mps->cp.dst_conn, res);
		configure_retry((CopyPartState *) mps, shardman_cmd_retry_naptime);
		return -1;
	}
	PQclear(res);
	shmn_elog(DEBUG1, "mp %s: LR conf on dst done", mps->cp.part_name);

	if (mps->next_node != SHMN_INVALID_NODE_ID)
	{
		if (ensure_pqconn(&mps->next_conn, mps->next_connstr,
						  (CopyPartState *) mps) == -1)
			return -1;
		res = PQexec(mps->next_conn, mps->next_sql);
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			shmn_elog(NOTICE, "Moving part %s: failed to configure LR on next replica: %s",
					  mps->cp.part_name, PQerrorMessage(mps->next_conn));
			reset_pqconn_and_res(&mps->next_conn, res);
			configure_retry((CopyPartState *) mps, shardman_cmd_retry_naptime);
			return -1;
		}
		PQclear(res);
		shmn_elog(DEBUG1, "mp %s: LR conf on next done", mps->cp.part_name);
	}

	return 0;
}

/*
 * One iteration of add replica task execution.
 *
 * Only two nodes involved here, old and new tail of replica chain.
 */
void
exec_create_replica(CreateReplicaState *crs)
{
	exec_cp((CopyPartState *) crs);
	if (crs->cp.curstep != COPYPART_DONE)
		return;

	if (cr_rebuild_lr(crs) == -1)
		return;

	void_spi(crs->cp.update_metadata_sql);
	shmn_elog(LOG, "Creating replica %s on node %d successfully done",
			  crs->cp.part_name, crs->cp.dst_node);
	crs->cp.res = TASK_SUCCESS;
	crs->cp.exec_res = TASK_DONE;
}

/*
 * Reconfigure LR channels for freshly created replica.
 *
 * TODO: simplify things and drop cp channel in triggers, or better let cp
 * part code itself do that.
 *
 * Work to do in general is described below. We execute them in steps written
 * in parentheses so that every time we create sub, pub is already exist and
 * every time we drop pub, sub is already dropped.
 * On old tail node, i.e. src:
 * - Drop repslot & pub used for copy (create_data_pub)
 * - Create repslot & pub for new data channel (create_data_pub)
 * - Make it synchronous; make table writable. (create_data_pub)
 * On new tail node, i.e. dst:
 * - Make table read-only for all but apply workers (drop_cp_sub)
 * - Drop sub used for copy (drop_cp_sub)
 * - Create sub for new data channel. (create_data_sub)
 */
int
cr_rebuild_lr(CreateReplicaState *crs)
{
	PGresult *res;

	if (ensure_pqconn_cp((CopyPartState *) crs,
						 ENSURE_PQCONN_SRC | ENSURE_PQCONN_DST) == -1)
		return -1;

	res = PQexec(crs->cp.dst_conn, crs->drop_cp_sub_sql);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		shmn_elog(NOTICE, "Creating replica %s: failed to configure LR, step 1: %s",
				  crs->cp.part_name, PQerrorMessage(crs->cp.dst_conn));
		reset_pqconn_and_res(&crs->cp.dst_conn, res);
		configure_retry((CopyPartState *) crs, shardman_cmd_retry_naptime);
		return -1;
	}
	PQclear(res);
	shmn_elog(DEBUG1, "cr %s: drop_cp_sub done", crs->cp.part_name);

	res = PQexec(crs->cp.src_conn, crs->create_data_pub_sql);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		shmn_elog(NOTICE, "Creating replica %s: failed to configure LR, step 2: %s",
				  crs->cp.part_name, PQerrorMessage(crs->cp.src_conn));
		reset_pqconn_and_res(&crs->cp.src_conn, res);
		configure_retry((CopyPartState *) crs, shardman_cmd_retry_naptime);
		return -1;
	}
	PQclear(res);
	shmn_elog(DEBUG1, "cr %s: create_data_pub done", crs->cp.part_name);

	res = PQexec(crs->cp.dst_conn, crs->create_data_sub_sql);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		shmn_elog(NOTICE, "Creating replica %s: failed to configure LR, step 3: %s",
				  crs->cp.part_name, PQerrorMessage(crs->cp.dst_conn));
		reset_pqconn_and_res(&crs->cp.dst_conn, res);
		configure_retry((CopyPartState *) crs, shardman_cmd_retry_naptime);
		return -1;
	}
	PQclear(res);
	shmn_elog(DEBUG1, "cr %s: create_data_sub done", crs->cp.part_name);

	return 0;
}

/*
 * Actually run CopyPartState state machine. On return, cps values say when
 * (if ever) we want to be executed again.
 *
 * Workflow is:
 * - Disable subscription on destination, otherwise we can't drop rep slot on
 *   source.
 * - Idempotently create publication and repl slot on source.
 * - Idempotently create table and async subscription on destination.
 *   We use async subscription, because sync would block table while copy is
 *   in progress. But with async, we have to lock the table after initial sync.
 * - Now inital copy has started.
 * - Sleep & check in connection to the dest waiting for completion of the
 *   initial sync. Later this should be substituted with listen/notify, we use
 *   epoll here for precisely for this reason, but this is not currently
 *   implemented, we need to add hook on initial tablesync completion.
 * - When done, lock writes (better lock reads too to avoid stale reads, in
 *	 fact) on source and remember pg_current_wal_lsn() on it.
 * - Now final sync has started.
 * - Sleep & check in connection to dest waiting for completion of final sync,
 *   i.e. when received_lsn is equal to remembered lsn on src. This is harder
 *   to replace with notify, but we can try that too.
 * - Done. After successfull execution, we are left with two copies of the
 *   table with src one locked for writes and with LR channel configured
 *   between them. TODO: drop channel here, because we don't reuse it anyway.
 *   Currently we drop the channel in metadata update triggers.
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

	if (ensure_pqconn_cp(cps, ENSURE_PQCONN_SRC | ENSURE_PQCONN_DST) == -1)
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

	if (ensure_pqconn_cp(cps, ENSURE_PQCONN_SRC | ENSURE_PQCONN_DST) == -1)
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

	if (ensure_pqconn_cp(cps, ENSURE_PQCONN_DST) == -1)
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
	shmn_elog(DEBUG1, "Partition %s %d -> %d successfully copied",
			  cps->part_name, cps->src_node, cps->dst_node);
	return 0;
}

/*
 * Ensure that pq connection to is CONNECTION_OK. nodes is a bitmask
 * specifying with which nodes connection must be ensured, src, dst or
 * bouth. -1 is returned if we have failed to establish connection; cps is
 * then configured to sleep retry time. 0 is returned if ok.
 */
int
ensure_pqconn_cp(CopyPartState *cps, int nodes)
{
	if ((nodes & ENSURE_PQCONN_SRC) &&
		(ensure_pqconn(&cps->src_conn, cps->src_connstr, cps) == -1))
		return -1;
	if ((nodes & ENSURE_PQCONN_DST) &&
		(ensure_pqconn(&cps->dst_conn, cps->dst_connstr, cps) == -1))
		return -1;
	return 0;
}

/*
 * Make sure that given conn is CONNECTION_OK, reconnect if not, and configure
 * cps to sleep if we can't.
 */
int
ensure_pqconn(PGconn **conn, const char *connstr,
			  CopyPartState *cps)
{
	if (*conn != NULL &&
		PQstatus(*conn) != CONNECTION_OK)
	{
		reset_pqconn(conn);
	}
	if (*conn == NULL)
	{
		Assert(connstr != NULL);
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
 * Configure cps so that main loop wakes us again after given retry millis.
 */
void configure_retry(CopyPartState *cps, int millis)
{
	shmn_elog(DEBUG1, "Copying part %s: sleeping %d ms and retrying",
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

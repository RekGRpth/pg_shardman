/* -------------------------------------------------------------------------
 *
 * shard.c
 *		Implementation of sharding commands.
 *
 * Copyright (c) 2017, Postgres Professional
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include <time.h>

#include "copypart.h"
#include "pg_shardman.h"
#include "shard.h"

static void cmd_single_task_exec_finished(Cmd *cmd, CopyPartState *cps);

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
	if ((connstr = get_node_connstr(node_id, SNT_WORKER)) == NULL)
	{
		shmn_elog(WARNING, "create_hash_partitions failed, no such worker node: %d",
				  node_id);
		update_cmd_status(cmd->id, "failed");
		return;
	}

	/*
	 * Note that we have to run statements in separate transactions, otherwise
	 * we have a deadlock between pathman and pg_dump. pfree'd with ctxt
	 */
	sql = psprintf(
		"begin; select shardman.drop_parts('%s', '%d');"
		" select create_hash_partitions('%s', '%s', %d); end;"
		"select shardman.gen_create_table_sql('%s', '%s');",
		relation, partitions_count,
		relation, expr, partitions_count,
		relation, connstr);

	/* Try to execute command indefinitely until it succeeded or canceled */
	while (1948)
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

		/* TODO: if lord fails at this moment (which is extremely unlikely
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
		SHMN_CHECK_FOR_INTERRUPTS_CMD(cmd);
	}
}

/* Update status of cmd consisting of single task after exec_tasks finishes */
void
cmd_single_task_exec_finished(Cmd *cmd, CopyPartState *cps)
{
	SHMN_CHECK_FOR_INTERRUPTS_CMD(cmd);

	Assert(cps->res != TASK_IN_PROGRESS);
	if (cps->res == TASK_FAILED)
		update_cmd_status(cmd->id, "failed");
	else if (cps->res == TASK_SUCCESS)
		update_cmd_status(cmd->id, "success");
}

/*
 * Move single partition.
 */
void
move_part(Cmd *cmd)
{
	char *part_name = cmd->opts[0];
	int32 dst_node = atoi(cmd->opts[1]);
	int32 src_node = cmd->opts[2] ? atoi(cmd->opts[2]) : SHMN_INVALID_NODE_ID;

	CopyPartState **tasks = palloc(sizeof(CopyPartState*));
	MovePartState *mps = palloc0(sizeof(MovePartState));
	init_mp_state(mps, part_name, src_node, dst_node);

	tasks[0] = (CopyPartState *) mps;
	exec_tasks(tasks, 1);
	cmd_single_task_exec_finished(cmd, (CopyPartState *) mps);
}

/*
 * Create single replica
 */
void
create_replica(Cmd *cmd)
{
	char *part_name = cmd->opts[0];
	int32 dst_node = atoi(cmd->opts[1]);

	CopyPartState **tasks = palloc(sizeof(CopyPartState*));
	/* palloc0 is important to set ptrs to NULL */
	CreateReplicaState *crs = palloc0(sizeof(CreateReplicaState));
	tasks[0] = (CopyPartState *) crs;
	init_cr_state(crs, part_name, dst_node);

	exec_tasks(tasks, 1);
	cmd_single_task_exec_finished(cmd, (CopyPartState *) crs);
}

/*
 * Dummiest way to distribute partitions evenly in round-robin fashion. Since
 * we ignore current distribution, some of the moves will most probably fail,
 * but the result should be more or less even.
 */
void
rebalance(Cmd *cmd)
{
	char *relation = cmd->opts[0];
	uint64 num_workers;
	int32 *workers = get_workers(&num_workers);
	uint64 worker_idx;
	uint64 num_parts;
	Partition *parts = get_parts(relation, &num_parts);
	uint64 part_idx;
	CopyPartState **tasks = palloc(sizeof(CopyPartState*) * num_parts);

	if (num_workers == 0)
	{
		elog(WARNING, "Table %s will not be rebalanced: no active workers",
			 relation);
		update_cmd_status(cmd->id, "failed");
		return;
	}

	if (num_parts == 0)
	{
		elog(WARNING, "Table %s will not be rebalanced: no such table",
			 relation);
		update_cmd_status(cmd->id, "failed");
		return;
	}

	for (part_idx = 0, worker_idx = 0; part_idx < num_parts; part_idx++)
	{
		Partition part = parts[part_idx];
		int32 worker;
		MovePartState *mps = palloc0(sizeof(MovePartState));

		worker = workers[worker_idx];
		worker_idx = (worker_idx + 1) % num_workers;

		init_mp_state(mps, part.part_name, part.owner, worker);
		tasks[part_idx] = (CopyPartState *) mps;
	}

	exec_tasks(tasks, num_parts);
	SHMN_CHECK_FOR_INTERRUPTS_CMD(cmd);

	shmn_elog(INFO, "Relation %s rebalanced", relation);
	update_cmd_status(cmd->id, "done");
}

/*
 * Add replicas to parts of given relation until we reach replevel replicas
 * for each one. Worker nodes are choosen in random manner.
 */
void
set_replevel(Cmd *cmd)
{
	char *relation = cmd->opts[0];
	uint32 replevel = atoi(cmd->opts[1]);
	uint64 num_workers;
	int32 *workers = get_workers(&num_workers);
	uint64 num_parts;
	RepCount *repcounts = NULL;
	uint64 part_idx;
	CopyPartState **tasks = NULL;
	int ntasks;
	int i;

	pg_usleep(10 * 1000000L); /* Wait 10 seconds to make asynchronous replication complete it work and all trigers fired */

	if (num_workers == 0)
	{
		elog(WARNING, "Set replevel on table %s failed: no active workers",
			 relation);
		update_cmd_status(cmd->id, "failed");
		return;
	}

	if (replevel > num_workers - 1)
	{
		elog(WARNING, "Set replevel on table %s: using replevel %ld instead of"
			 "%d as we have only %ld active workers",
			 relation, num_workers - 1, replevel, num_workers);
		replevel = num_workers - 1;
	}

	srand(time(NULL));
	/*
	 * We can add only one replica per part in one exec_tasks; loop until
	 * required number of replicas are reached.
	 */
	while (1948)
	{
		repcounts = get_repcount(relation, &num_parts);
		tasks = palloc(sizeof(CopyPartState*) * num_parts);
		ntasks = 0;

		for (part_idx = 0; part_idx < num_parts; part_idx++)
		{
			RepCount rc = repcounts[part_idx];
			if (rc.count < replevel)
			{
				CreateReplicaState *crs = palloc0(sizeof(CreateReplicaState));
				int32 dst_node;

				do {
					dst_node = workers[rand() % num_workers];
				} while (node_has_partition(dst_node, rc.part_name));

				init_cr_state(crs, rc.part_name, dst_node);

				tasks[ntasks] = (CopyPartState *) crs;
				ntasks++;
				shmn_elog(DEBUG1, "Adding replica for shard %s on node %d",
						  rc.part_name, dst_node);
			}
		}

		if (ntasks == 0)
			break;

		exec_tasks(tasks, ntasks);
		SHMN_CHECK_FOR_INTERRUPTS_CMD(cmd);

		pfree(repcounts);
		for (i = 0; i < ntasks; i++)
			pfree(tasks[i]);
		pfree(tasks);
	}

	shmn_elog(INFO, "Relation %s now has at least %d replicas", relation,
			  replevel);
	update_cmd_status(cmd->id, "success");
}

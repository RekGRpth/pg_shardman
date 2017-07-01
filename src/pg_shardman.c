/* -------------------------------------------------------------------------
 *
 * shardmaster.c
 *		Background worker executing sharding tasks.
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/time.h> /* For 'select' portability */
#include <sys/select.h>

/* These are always necessary for a bgworker */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

#include "utils/guc.h"
#include "utils/snapmgr.h"
#include "executor/spi.h"
#include "access/xact.h"
#include "libpq-fe.h"




/* ensure that extension won't load against incompatible version of Postgres */
PG_MODULE_MAGIC;

typedef struct Cmd
{
	int64 id;
	char *cmd_type;
	char *status;
} Cmd;

extern void _PG_init(void);
extern void shardmaster_main(Datum main_arg);

static Cmd *next_cmd(void);
static void update_cmd_status(int64 id, const char *new_status);

/* GUC variables */
static bool shardman_master = false;
static char *shardman_master_dbname = "postgres";

/*
 * Entrypoint of the module. Define variables and register background worker.
 */
void
_PG_init()
{
	BackgroundWorker shardmaster_worker;
	DefineCustomBoolVariable("shardman.master",
							 "This node is the master?",
							 NULL,
							 &shardman_master,
							 false,
							 PGC_POSTMASTER,
							 0,
							 NULL, NULL, NULL);

	DefineCustomStringVariable(
		"shardman.master_dbname",
		"Name of the database with extension on master node, shardmaster bgw"
		"will connect to it",
		NULL,
		&shardman_master_dbname,
		"postgres",
		PGC_POSTMASTER,
		0,
		NULL, NULL, NULL
		);

	/* register shardmaster */
	sprintf(shardmaster_worker.bgw_name, "shardmaster");
	shardmaster_worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	shardmaster_worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	shardmaster_worker.bgw_restart_time = 1;
	sprintf(shardmaster_worker.bgw_library_name, "pg_shardman");
	sprintf(shardmaster_worker.bgw_function_name, "shardmaster_main");
	shardmaster_worker.bgw_notify_pid = 0;
	RegisterBackgroundWorker(&shardmaster_worker);
}

/*
 * shardmaster bgw starts here
 */
void
shardmaster_main(Datum main_arg)
{
	Cmd *cmd;
	PGconn     *conn;
	const char *conninfo;
	elog(LOG, "Shardmaster started");

	/* Connect to the database to use SPI*/
	BackgroundWorkerInitializeConnection(shardman_master_dbname, NULL);

	conninfo = psprintf("dbname = %s", shardman_master_dbname);
	conn = PQconnectdb(conninfo);
	/* Check to see that the backend connection was successfully made */
	if (PQstatus(conn) != CONNECTION_OK)
		elog(FATAL, "Connection to database failed: %s",
			 PQerrorMessage(conn));


	/* pg_usleep(10000000L); */
	while ((cmd = next_cmd()) != NULL)
	{
		elog(LOG, "Working on command %ld, %s", cmd->id, cmd->cmd_type);
		update_cmd_status(cmd->id, "success");
	}


	/* while (1948) */
	/* { */

	/* } */
	PQfinish(conn);
	proc_exit(0);
}

/*
 * Retrieve next cmd to work on -- uncompleted command with min id.
 * Returns NULL if queue is empty. Memory is allocated in the current cxt.
 */
Cmd *
next_cmd(void)
{
	const char *cmd_sql;
	Cmd *cmd = NULL;
	MemoryContext oldcxt = CurrentMemoryContext;
	int e;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	cmd_sql = "select * from shardman.cmd_log t1 join"
		" (select MIN(id) id from shardman.cmd_log where status = 'waiting' OR"
		" status = 'in progress') t2 using (id);";
	e = SPI_execute(cmd_sql, true, 0);
	if (e < 0)
	{
		elog(FATAL, "Stmt failed: %s", cmd_sql);
	}

	if (SPI_processed > 0)
	{
		HeapTuple tuple = SPI_tuptable->vals[0];
		TupleDesc rowdesc = SPI_tuptable->tupdesc;
		bool isnull;
		const char *cmd_type = (SPI_getvalue(tuple, rowdesc,
											 SPI_fnumber(rowdesc, "cmd_type")));

		MemoryContext spicxt = MemoryContextSwitchTo(oldcxt);
		cmd = palloc(sizeof(Cmd));
		cmd->id = DatumGetInt64(SPI_getbinval(tuple, rowdesc,
											  SPI_fnumber(rowdesc, "id"),
											  &isnull));
		cmd->cmd_type = pstrdup(cmd_type);
		MemoryContextSwitchTo(spicxt);
	}

	PopActiveSnapshot();
	SPI_finish();
	CommitTransactionCommand();

	return cmd;
}

/*
 * Update command status
 */
void
update_cmd_status(int64 id, const char *new_status)
{
	char *sql;
	int e;
	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	sql = psprintf("update shardman.cmd_log set status = '%s' where id = %ld;",
				   new_status, id);
	e = SPI_exec(sql, 0);
	if (e < 0)
	{
		elog(FATAL, "Stmt failed: %s", sql);
	}
	pfree(sql);

	PopActiveSnapshot();
	SPI_finish();
	CommitTransactionCommand();
}

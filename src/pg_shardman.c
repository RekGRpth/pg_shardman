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
#include "commands/extension.h"
#include "libpq-fe.h"

#include "pg_shardman.h"


/* ensure that extension won't load against incompatible version of Postgres */
PG_MODULE_MAGIC;

typedef struct Cmd
{
	int64 id;
	char *cmd_type;
	char *status;
	char **opts; /* array of n options, opts[n] is NULL */
} Cmd;

static Cmd *next_cmd(void);
static void update_cmd_status(int64 id, const char *new_status);
static PGconn *listen_cmd_log_inserts(void);
static void wait_notify(PGconn *conn);
static void shardmaster_sigterm(SIGNAL_ARGS);
static void shardmaster_sigusr1(SIGNAL_ARGS);
static void pg_shardman_installed(void);

/* flags set by signal handlers */
static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sigusr1 = false;

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

	if (!process_shared_preload_libraries_in_progress)
	{
		ereport(ERROR, (errmsg("pg_shardman can only be loaded via shared_preload_libraries"),
						errhint("Add pg_shardman to shared_preload_libraries.")));
	}

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

	if (shardman_master)
	{
		/* register shardmaster */
		sprintf(shardmaster_worker.bgw_name, "shardmaster");
		shardmaster_worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
			BGWORKER_BACKEND_DATABASE_CONNECTION;
		shardmaster_worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
		shardmaster_worker.bgw_restart_time = 10;
		sprintf(shardmaster_worker.bgw_library_name, "pg_shardman");
		sprintf(shardmaster_worker.bgw_function_name, "shardmaster_main");
		shardmaster_worker.bgw_notify_pid = 0;
		RegisterBackgroundWorker(&shardmaster_worker);
	}
}

/*
 * shardmaster bgw starts here
 */
void
shardmaster_main(Datum main_arg)
{
	Cmd *cmd;
	PGconn     *conn;
	elog(LOG, "Shardmaster started");

	/* Connect to the database to use SPI*/
	BackgroundWorkerInitializeConnection(shardman_master_dbname, NULL);
	/* sanity check */
	pg_shardman_installed();

	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGTERM, shardmaster_sigterm);
	pqsignal(SIGUSR1, shardmaster_sigusr1);
    /* We're now ready to receive signals */
    BackgroundWorkerUnblockSignals();

	conn = listen_cmd_log_inserts();

	/* main loop */
	while (!got_sigterm)
	{
		/* TODO: new mem ctxt for every command */
		while ((cmd = next_cmd()) != NULL)
		{
			update_cmd_status(cmd->id, "in progress");
			elog(LOG, "Working on command %ld, %s, opts are", cmd->id, cmd->cmd_type);
			for (char **opts = cmd->opts; *opts; opts++)
				elog(LOG, "%s", *opts);
			update_cmd_status(cmd->id, "success");
		}
		wait_notify(conn);
		if (got_sigusr1)
		{
			elog(LOG, "SIGUSR1 arrived, aborting current command");
			update_cmd_status(cmd->id, "canceled");
			got_sigusr1 = false;
		}
	}

	elog(LOG, "Shardmaster received SIGTERM, exiting");
	PQfinish(conn);
	proc_exit(0);
}

/*
 * Open libpq connection to our server and start listening to cmd_log inserts
 * notifications.
 */
PGconn *
listen_cmd_log_inserts(void)
{
	PGconn *conn;
	char *conninfo;
	PGresult   *res;

	conninfo = psprintf("dbname = %s", shardman_master_dbname);
	conn = PQconnectdb(conninfo);
	pfree(conninfo);
	/* Check to see that the backend connection was successfully made */
	if (PQstatus(conn) != CONNECTION_OK)
		elog(FATAL, "Connection to database failed: %s",
			 PQerrorMessage(conn));

	res = PQexec(conn, "LISTEN shardman_cmd_log_update");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		elog(FATAL, "LISTEN command failed: %s", PQerrorMessage(conn));
	}
    PQclear(res);

	return conn;
}

/*
 * Wait until NOTIFY or signal arrives. If select is alerted, but there are
 * no notifcations, we also return.
 */
void
wait_notify(PGconn *conn)
{
	int			sock;
	fd_set		input_mask;
    PGnotify   *notify;

	sock = PQsocket(conn);
	if (sock < 0)
		elog(FATAL, "Couldn't get sock from pgconn");

	FD_ZERO(&input_mask);
	FD_SET(sock, &input_mask);

	if (select(sock + 1, &input_mask, NULL, NULL, NULL) < 0)
	{
		if (errno == EINTR)
			return; /* signal has arrived */
		elog(FATAL, "select() failed: %s", strerror(errno));
	}

	PQconsumeInput(conn);
	/* eat all notifications at once */
	while ((notify = PQnotifies(conn)) != NULL)
	{
		elog(LOG, "NOTIFY %s received from backend PID %d",
			 notify->relname, notify->be_pid);
		PQfreemem(notify);
	}

	return;
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

	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	cmd_sql = "select * from shardman.cmd_log t1 join"
		" (select MIN(id) id from shardman.cmd_log where status = 'waiting' OR"
		" status = 'in progress') t2 using (id);";
	e = SPI_execute(cmd_sql, true, 0);
	if (e < 0)
		elog(FATAL, "Stmt failed: %s", cmd_sql);

	if (SPI_processed > 0)
	{
		HeapTuple tuple = SPI_tuptable->vals[0];
		TupleDesc rowdesc = SPI_tuptable->tupdesc;
		bool isnull;
		uint64 i;

		/* copy the command itself to callee context */
		MemoryContext spicxt = MemoryContextSwitchTo(oldcxt);
		cmd = palloc(sizeof(Cmd));
		cmd->id = DatumGetInt64(SPI_getbinval(tuple, rowdesc,
											  SPI_fnumber(rowdesc, "id"),
											  &isnull));
		cmd->cmd_type = SPI_getvalue(tuple, rowdesc,
									 SPI_fnumber(rowdesc, "cmd_type"));
		MemoryContextSwitchTo(spicxt);

		/* Now get options. cmd_sql will be freed by SPI_finish */
		cmd_sql = psprintf("select opt from shardman.cmd_opts where"
						   " cmd_id = %ld order by id;", cmd->id);
		e = SPI_execute(cmd_sql, true, 0);
		if (e < 0)
			elog(FATAL, "Stmt failed: %s", cmd_sql);

		MemoryContextSwitchTo(oldcxt);
		/* +1 for NULL in the end */
		cmd->opts = palloc((SPI_processed + 1) * sizeof(char*));
		for (i = 0; i < SPI_processed; i++)
		{
			tuple = SPI_tuptable->vals[i];
			rowdesc = SPI_tuptable->tupdesc;
			cmd->opts[i] = SPI_getvalue(tuple, rowdesc,
										SPI_fnumber(rowdesc, "opt"));
		}
		cmd->opts[i] = NULL;

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
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	sql = psprintf("update shardman.cmd_log set status = '%s' where id = %ld;",
				   new_status, id);
	e = SPI_exec(sql, 0);
	pfree(sql);
	if (e < 0)
	{
		elog(FATAL, "Stmt failed: %s", sql);
	}

	PopActiveSnapshot();
	SPI_finish();
	CommitTransactionCommand();
}

/*
 * Verify that extension is installed locally. We must be connected to db at
 * this point
 */
static void
pg_shardman_installed(void)
{
	bool installed = true;

	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	if (get_extension_oid("pg_shardman", true) == InvalidOid)
	{
		installed = false;
		elog(WARNING, "pg_shardman library is preloaded, but extenstion is not created");
	}
	PopActiveSnapshot();
	CommitTransactionCommand();

	/* shardmaster won't run without extension */
	if (!installed)
		proc_exit(1);
}

/*
 * Signal handler for SIGTERM
 *		Set a flag to let the main loop to terminate.
 */
static void
shardmaster_sigterm(SIGNAL_ARGS)
{
	got_sigterm = true;
}

/*
 * Signal handler for SIGUSR1
 *		Set a flag to let the main loop to terminate.
 */
static void
shardmaster_sigusr1(SIGNAL_ARGS)
{
	got_sigusr1 = true;
}

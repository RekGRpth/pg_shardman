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
static void publicate_metadata(void);
static void wait_notify(PGconn *conn);
static void shardmaster_sigterm(SIGNAL_ARGS);
static void shardmaster_sigusr1(SIGNAL_ARGS);
static void check_for_sigterm(void);
static void pg_shardman_installed_local(void);
static void add_node(Cmd *cmd);
static bool node_in_cluster(int id);

/* flags set by signal handlers */
static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sigusr1 = false;

/* GUC variables */
static bool shardman_master = false;
static char *shardman_master_dbname = "postgres";
static int shardman_cmd_retry_naptime = 10000;

/* Just global vars. */
/* Connection to local server for LISTEN notifications. Is is global for easy
 * cleanup after receiving SIGTERM.
 */
static PGconn *conn = NULL;

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

	DefineCustomIntVariable("shardman.cmd_retry_naptime",
							"Sleep time in millisec between retrying to execute failing command",
							NULL,
							&shardman_cmd_retry_naptime,
							10000,
							0,
							INT_MAX,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

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
	/* TODO: clean up publications if we were master before */
}

/*
 * shardmaster bgw starts here
 */
void
shardmaster_main(Datum main_arg)
{
	Cmd *cmd;
	shmn_elog(LOG, "Shardmaster started");

	/* Connect to the database to use SPI*/
	BackgroundWorkerInitializeConnection(shardman_master_dbname, NULL);
	/* sanity check */
	pg_shardman_installed_local();

	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGTERM, shardmaster_sigterm);
	pqsignal(SIGUSR1, shardmaster_sigusr1);
    /* We're now ready to receive signals */
    BackgroundWorkerUnblockSignals();

	publicate_metadata();
	conn = listen_cmd_log_inserts();

	/* main loop */
	while (1948)
	{
		/* TODO: new mem ctxt for every command */
		while ((cmd = next_cmd()) != NULL)
		{
			update_cmd_status(cmd->id, "in progress");
			shmn_elog(LOG, "Working on command %ld, %s, opts are",
				 cmd->id, cmd->cmd_type);
			for (char **opts = cmd->opts; *opts; opts++)
				shmn_elog(LOG, "%s", *opts);
			if (strcmp(cmd->cmd_type, "add_node") == 0)
				add_node(cmd);
			else
				shmn_elog(FATAL, "Unknown cmd type %s", cmd->cmd_type);
		}
		wait_notify(conn);
		check_for_sigterm();
	}

}

/*
 * Create publication on tables with metadata.
 */
void
publicate_metadata(void)
{
	const char *cmd_sql = "select shardman.create_meta_pub();";
	int e;

	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	e = SPI_execute(cmd_sql, true, 0);
	if (e < 0)
		shmn_elog(FATAL, "Stmt failed: %s", cmd_sql);

	PopActiveSnapshot();
	SPI_finish();
	CommitTransactionCommand();
}

/*
 * Open libpq connection to our server and start listening to cmd_log inserts
 * notifications.
 */
PGconn *
listen_cmd_log_inserts(void)
{
	char *conninfo;
	PGresult   *res;

	conninfo = psprintf("dbname = %s", shardman_master_dbname);
	conn = PQconnectdb(conninfo);
	pfree(conninfo);
	/* Check to see that the backend connection was successfully made */
	if (PQstatus(conn) != CONNECTION_OK)
		shmn_elog(FATAL, "Connection to local database failed: %s",
			 PQerrorMessage(conn));

	res = PQexec(conn, "LISTEN shardman_cmd_log_update");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		shmn_elog(FATAL, "LISTEN command failed: %s", PQerrorMessage(conn));
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
		shmn_elog(FATAL, "Couldn't get sock from pgconn");

	FD_ZERO(&input_mask);
	FD_SET(sock, &input_mask);

	if (select(sock + 1, &input_mask, NULL, NULL, NULL) < 0)
	{
		if (errno == EINTR)
			return; /* signal has arrived */
		shmn_elog(FATAL, "select() failed: %s", strerror(errno));
	}

	PQconsumeInput(conn);
	/* eat all notifications at once */
	while ((notify = PQnotifies(conn)) != NULL)
	{
		shmn_elog(LOG, "NOTIFY %s received from backend PID %d",
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
		shmn_elog(FATAL, "Stmt failed: %s", cmd_sql);

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
			shmn_elog(FATAL, "Stmt failed: %s", cmd_sql);

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
		shmn_elog(FATAL, "Stmt failed: %s", sql);
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
pg_shardman_installed_local(void)
{
	bool installed = true;

	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	if (get_extension_oid("pg_shardman", true) == InvalidOid)
	{
		installed = false;
		shmn_elog(WARNING, "pg_shardman library is preloaded, but extenstion is not created");
	}
	PopActiveSnapshot();
	CommitTransactionCommand();

	/* shardmaster won't run without extension */
	/* TODO: unregister bgw? */
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

/*
 * Cleanup and exit in case of SIGTERM
 */
static void
check_for_sigterm(void)
{
	if (got_sigterm)
	{
		shmn_elog(LOG, "Shardmaster received SIGTERM, exiting");
		if (conn != NULL)
			PQfinish(conn);
		proc_exit(0);
	}
}

/*
 * Adding node consists of
 * - verifying that the node is not present in the cluster at the moment
 * - subscription creation
 * - setting node id
 * - adding node to 'nodes' table
 */
static void add_node(Cmd *cmd)
{
	PGconn *conn = NULL;
	const char *conninfo = cmd->opts[0];
    PGresult *res = NULL;
	bool pg_shardman_installed;
	int node_id;

	shmn_elog(LOG, "Adding node %s", conninfo);
	/* Try to execute command indefinitely until it succeeded or canceled */
	while (!got_sigusr1 && !got_sigterm)
	{
		conn = PQconnectdb(conninfo);
		if (PQstatus(conn) != CONNECTION_OK)
		{
			shmn_elog(NOTICE, "Connection to add_node node failed: %s",
				 PQerrorMessage(conn));
			goto attempt_failed;
		}

		/* Check if our extension is installed on the node */
		res = PQexec(conn,
					 "select installed_version from pg_available_extensions"
					 " where name = 'pg_shardman';");
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			shmn_elog(NOTICE, "Failed to check whether pg_shardman is installed on"
				 " node to add%s", PQerrorMessage(conn));
			goto attempt_failed;
		}
		pg_shardman_installed = PQntuples(res) == 1 && !PQgetisnull(res, 0, 0);
		PQclear(res);

		if (pg_shardman_installed)
		{
			/* extension is installed, so we have to check whether this node
			 * is already in the cluster */
			res = PQexec(conn, "select shardman.get_node_id();");
			if (PQresultStatus(res) != PGRES_TUPLES_OK)
			{
				shmn_elog(NOTICE, "Failed to get node id, %s", PQerrorMessage(conn));
				goto attempt_failed;
			}
			node_id = atoi(PQgetvalue(res, 0, 0));
			PQclear(res);
			if (node_in_cluster(node_id))
			{
				shmn_elog(WARNING, "node %d with connstring %s is already in cluster,"
					 " won't add it.", node_id, conninfo);
				PQfinish(conn);
				update_cmd_status(cmd->id, "failed");
				return;
			}
		}

		/* Now, when we are sure that node is not in the cluster, we reinstall
		 * the extension to reset its state, whether is was installed before
		 * or not.
		 */
		res = PQexec(conn, "drop extension if exists pg_shardman; "
					 " create extension pg_shardman;");
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			shmn_elog(NOTICE, "Failed to reinstall pg_shardman, %s", PQerrorMessage(conn));
			goto attempt_failed;
		}
		PQclear(res);

		/* TODO */

		/* done */
		PQfinish(conn);
		update_cmd_status(cmd->id, "success");
		return;

attempt_failed: /* clean resources, sleep, check sigusr1 and try again */
		if (res != NULL)
			PQclear(res);
		if (conn != NULL)
			PQfinish(conn);

		shmn_elog(LOG, "Attempt to execute add_node failed, sleeping and retrying");
		pg_usleep(shardman_cmd_retry_naptime * 1000L);
	}

	check_for_sigterm();

	/* Command canceled via sigusr1 */
	shmn_elog(LOG, "Command %ld canceled", cmd->id);
	update_cmd_status(cmd->id, "canceled");
	got_sigusr1 = false;
	return;
}

/*
 * Returns true, if node 'id' is in the cluster, false otherwise.
 */
static bool
node_in_cluster(int id)
{
	int e;
	const char *sql = "select id from shardman.nodes;";
	bool res = false;
	HeapTuple tuple;
	TupleDesc rowdesc;
	uint64 i;
	bool isnull;

	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	e = SPI_execute(sql, true, 0);
	if (e < 0)
		shmn_elog(FATAL, "Stmt failed: %s", sql);

	rowdesc = SPI_tuptable->tupdesc;
	for (i = 0; i < SPI_processed; i++)
	{
		tuple = SPI_tuptable->vals[i];
		if (id == DatumGetInt32(SPI_getbinval(tuple, rowdesc,
											  SPI_fnumber(rowdesc, "id"),
											  &isnull)))
			res = true;
	}

	PopActiveSnapshot();
	SPI_finish();
	CommitTransactionCommand();

	return res;
}

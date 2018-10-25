#include <getopt.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <libpq-fe.h>

#define SQLSIZE 1024
#define GIDSIZE 200

#define PQresetres(resptr) \
	do { \
	PQclear(resptr); \
	resptr = NULL; \
} while (0)

typedef struct LoaderOptions
{
	char *lord_connstring;
	char *table_name;
	char *file_path;
	int nconn;
	int rows_per_xact;
	int no_twophase;
	char delimiterc;
	char quotec;
	char escapec;
	char *direct_connstr;
	bool print_progress;
	int report_every_rows;
} LoaderOptions;
static LoaderOptions lopt;

typedef struct Node
{
	int id;
	char *connstr;
} Node;

typedef struct ConnState
{
	int node_id;
	PGconn *conn;
	int row_count;
	int total_row_count;
	int xact_count;
	char *sys_id;
} ConnState;

typedef struct LoaderState
{
	Node *nodes;
	int numnodes;
	ConnState *conns;
	int master_node_id; /* for shared table */
	char *master_node_connstr; /* not alloced separately */
	char copy_sql[SQLSIZE];
} LoaderState;

static void lord_failure(char *msg, PGconn *lord_conn)
{
	fprintf(stderr, "%s:\n %s", msg, PQerrorMessage(lord_conn));
	PQfinish(lord_conn);
	/* yeah, pqresult might leak */
	exit(1);
}

/* Learn nodes and table */
static void learn_nodes_and_table(LoaderState *lstate)
{
	PGconn *lord_conn;
	PGresult* res;
	char sql[SQLSIZE];
	int i;

	/* to be able to work with standalone instance */
	if (lopt.direct_connstr != NULL)
		return;

	lord_conn = PQconnectdb(lopt.lord_connstring);
	if (PQstatus(lord_conn) != CONNECTION_OK)
	{
		lord_failure("Failed to connect to shardlord", lord_conn);
    }

	res = PQexec(lord_conn, "select id, connection_string from shardman.nodes");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		lord_failure("Failed to retrieve nodes info", lord_conn);
	}
	lstate->numnodes = PQntuples(res);
	if (lstate->numnodes < 1)
	{
		lord_failure("No nodes in the cluster", lord_conn);
	}
	lstate->nodes = calloc(lstate->numnodes, sizeof(Node)); /* zeroing conns */
	for (i = 0; i < lstate->numnodes; i++)
	{
		lstate->nodes[i].id = atoi(PQgetvalue(res, i, 0));
		lstate->nodes[i].connstr = strdup(PQgetvalue(res, i, 1));
	}
	PQclear(res);

	snprintf(sql, SQLSIZE, "select master_node from shardman.tables where relation = '%s'", lopt.table_name);
	res = PQexec(lord_conn, sql);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		lord_failure("Failed to retrieve table info", lord_conn);
	}
	if (PQntuples(res) != 1)
	{
		fprintf(stderr, "Table %s is not sharded/shared\n", lopt.table_name);
		PQfinish(lord_conn);
		exit(1);
	}
	if (PQgetisnull(res, 0, 0))
		lstate->master_node_id = 0; /* sharded table */
	else
		lstate->master_node_id = atoi(PQgetvalue(res, 0, 0));
	PQfinish(lord_conn);

	/* In case of shared table, find connstr of master */
	if (lstate->master_node_id != 0)
	{
		for (i = 0; i < lstate->numnodes; i++)
		{
			if (lstate->nodes[i].id == lstate->master_node_id)
				lstate->master_node_connstr = lstate->nodes[i].connstr;
		}
	}
}

static void cleanup_and_exit(LoaderState *lstate)
{
	int i;
	for (i = 0; i < lopt.nconn; i++)
	{
		PQfinish(lstate->conns[i].conn);
		lstate->conns[i].conn = NULL;
	}
	exit(1);
}

/* Connect and start copy */
static void
prepare_connections(LoaderState *lstate)
{
	int i;
	int node_idx = 0;
	char sql[SQLSIZE];
	PGresult* res;

	snprintf(lstate->copy_sql, SQLSIZE, "copy %s from stdin (format csv, delimiter '%c', quote '%c', escape '%c')",
			 lopt.table_name, lopt.delimiterc, lopt.quotec, lopt.escapec);
	lstate->conns = calloc(lopt.nconn, sizeof(ConnState));
	for (i = 0; i < lopt.nconn; i++)
	{
		ConnState *conn_state = &lstate->conns[i];

		if (lopt.direct_connstr != NULL)
		{
			conn_state->node_id = -1;
			conn_state->conn = PQconnectdb(lopt.direct_connstr);
		}
		else if (lstate->master_node_connstr != NULL) /* shared table */
		{
			conn_state->node_id = lstate->master_node_id;
			conn_state->conn = PQconnectdb(lstate->master_node_connstr);
		}
		else
		{
			conn_state->node_id = lstate->nodes[node_idx].id;
			conn_state->conn = PQconnectdb(lstate->nodes[node_idx].connstr);
			node_idx = (node_idx + 1) % lstate->numnodes;
		}
		if (PQstatus(conn_state->conn) != CONNECTION_OK)
		{
			fprintf(stderr, "Failed to connect to node %d: %s",
					conn_state->node_id,
					PQerrorMessage(conn_state->conn));
			cleanup_and_exit(lstate);
		}

		res = PQexec(conn_state->conn, "select system_identifier from pg_control_system()");
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			fprintf(stderr, "%s", PQerrorMessage(conn_state->conn));
			cleanup_and_exit(lstate);
		}
		conn_state->sys_id = strdup(PQgetvalue(res, 0, 0));
		PQclear(res);

		res = PQexec(conn_state->conn, "begin");
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
		{
			fprintf(stderr, "%s", PQerrorMessage(conn_state->conn));
			cleanup_and_exit(lstate);
		}
		PQclear(res);

		res = PQexec(conn_state->conn, lstate->copy_sql);
		if (PQresultStatus(res) != PGRES_COPY_IN)
		{
			fprintf(stderr, "Failed to start copy on node %d: %s",
					conn_state->node_id,
					PQerrorMessage(conn_state->conn));
			PQclear(res);
			cleanup_and_exit(lstate);
		}
		PQclear(res);
	}
}

static void
report_progress(int total_rows, int total_xacts, time_t starttm)
{
	time_t curtm;
	long diffsec;
	char humandiff[128];
	long hours, minutes, seconds;

	time(&curtm);
	diffsec = (long) difftime(curtm, starttm);
	hours = diffsec / 3600;
	minutes = diffsec / 60;
	seconds = diffsec % 60;
	if (hours != 0)
		snprintf(humandiff, sizeof(humandiff), "%ldh %ldm %lds", hours, minutes, seconds);
	else if (minutes != 0)
		snprintf(humandiff, sizeof(humandiff), "%ldm %lds", minutes, seconds);
	else
		snprintf(humandiff, sizeof(humandiff), "%lds", seconds);

	printf("%d rows sent, %d xacts performed, %s elapsed\n",
		   total_rows, total_xacts, humandiff);
}

/* returns true if ok, false otherwise */
static int
do_copy(LoaderState *lstate)
{
	char *line;
	size_t readbufallocated = 0;
	ssize_t linelen;
	FILE *stream;
	int ok = true;
	int cs_idx = 0;
	char gid[GIDSIZE];
	char sql[SQLSIZE];
	PGresult* res = NULL;
	int total_rows = 0;
	int total_xacts = 0;
	int i;
	bool fin_ok = true;
	time_t starttm;

	if (lopt.file_path != NULL)
		stream = fopen(lopt.file_path, "r");
	else
		stream = stdin;
	if (stream == NULL)
	{
		perror("Failed to open CSV file");
		cleanup_and_exit(lstate);
	}

	time(&starttm);
	/* Doesn't work with CR/LF in data or not-CR line endings */
	while ((linelen = getline(&line, &readbufallocated, stream)) != -1)
	{
		ConnState *conn_state = &lstate->conns[cs_idx];

		if (PQputCopyData(conn_state->conn, line, linelen) <= 0)
		{
			fprintf(stderr, "PQputCopyData failed: %s",
					PQerrorMessage(conn_state->conn));
			ok = false;
			break;
		}

		conn_state->row_count++;
		conn_state->total_row_count++;
		total_rows++;
		if (conn_state->row_count >= lopt.rows_per_xact)
		{
			/* Time to switch xact */
			conn_state->xact_count++;
			total_xacts++;
			conn_state->row_count = 0;
			if (PQputCopyEnd(conn_state->conn, NULL) <= 0)
			{
				fprintf(stderr, "PQputCopyEnd failed: %s",
						PQerrorMessage(conn_state->conn));
				ok = false;
				break;
			}
			res = PQgetResult(conn_state->conn);
			if (PQresultStatus(res) != PGRES_COMMAND_OK)
			{
				fprintf(stderr, "PQgetResut after copy end failed %s",
						   PQerrorMessage(conn_state->conn));
				ok = false;
				break;
			}
			PQresetres(res);
			if (lopt.no_twophase)
			{
				res = PQexec(conn_state->conn, "commit");
				if (PQresultStatus(res) != PGRES_COMMAND_OK)
				{
					fprintf(stderr, "%s", PQerrorMessage(conn_state->conn));
					ok = false;
					break;
				}
				PQresetres(res);
			}
			else
			{
				/*
				 * Try to respect shardman's gid format so recover_xacts can
				 * handle it.
				 * This actually won't work in most cases because we don't
				 * know xid and number of participants, but it is easy to
				 * rollback/commit these xacts manually.
				 */
				snprintf(gid, GIDSIZE, "pgfdw:%ld:%s:0:0:0:%d_shmnloader_%s:%d:%d",
						 (long) time(NULL),
						 conn_state->sys_id,
						 lstate->numnodes,
						 lopt.table_name,
						 cs_idx,
						 conn_state->xact_count);
				snprintf(sql, SQLSIZE, "prepare transaction '%s'", gid);
				res = PQexec(conn_state->conn, sql);
				if (PQresultStatus(res) != PGRES_COMMAND_OK)
				{
					fprintf(stderr, "%s", PQerrorMessage(conn_state->conn));
					ok = false;
					break;
				}
			}

			res = PQexec(conn_state->conn, "begin");
			if (PQresultStatus(res) != PGRES_COMMAND_OK)
			{
				fprintf(stderr, "%s", PQerrorMessage(conn_state->conn));
				ok = false;
				break;
			}
			PQresetres(res);

			res = PQexec(conn_state->conn, lstate->copy_sql);
			if (PQresultStatus(res) != PGRES_COPY_IN)
			{
				fprintf(stderr, "Failed to start copy on node %d: %s",
						conn_state->node_id,
						PQerrorMessage(conn_state->conn));
				ok = false;
				break;
			}
			PQresetres(res);
		}

		cs_idx = (cs_idx + 1) % lopt.nconn;
		if (lopt.print_progress && total_rows % lopt.report_every_rows == 0)
			report_progress(total_rows, total_xacts, starttm);
	}

	free(line);
	if (lopt.file_path != NULL)
		fclose(stream);

	/* The last xact */
	if (ok)
	{
		for (i = 0; i < lopt.nconn; i++)
		{
			ConnState *conn_state = &lstate->conns[i];

			conn_state->xact_count++;
			total_xacts++;
			if (PQputCopyEnd(conn_state->conn, NULL) <= 0)
			{
				fprintf(stderr, "PQputCopyEnd failed: %s",
						PQerrorMessage(conn_state->conn));
				ok = false;
				break;
			}
			res = PQgetResult(conn_state->conn);
			if (PQresultStatus(res) != PGRES_COMMAND_OK)
			{
				fprintf(stderr, "PQgetResut after copy end failed %s",
						   PQerrorMessage(conn_state->conn));
				ok = false;
				break;
			}
			PQresetres(res);

			if (lopt.no_twophase)
			{
				res = PQexec(conn_state->conn, "commit");
				if (PQresultStatus(res) != PGRES_COMMAND_OK)
				{
					fprintf(stderr, "%s", PQerrorMessage(conn_state->conn));
					ok = false;
					break;
				}
				PQresetres(res);
			}
			else
			{
				/*
				 * Try to respect shardman's gid format so recover_xacts can
				 * handle it.
				 * This actually won't work in most cases because we don't
				 * know xid and number of participants, but it is easy to
				 * rollback/commit these xacts manually.
				 */
				snprintf(gid, GIDSIZE, "pgfdw:%ld:%s:0:0:0:%d_shmnloader_%s:%d:%d",
						 (long) time(NULL),
						 conn_state->sys_id,
						 lstate->numnodes,
						 lopt.table_name,
						 i,
						 conn_state->xact_count);
				snprintf(sql, SQLSIZE, "prepare transaction '%s'", gid);
				res = PQexec(conn_state->conn, sql);
				if (PQresultStatus(res) != PGRES_COMMAND_OK)
				{
					fprintf(stderr, "%s", PQerrorMessage(conn_state->conn));
					ok = false;
					break;
				}
			}
		}
	}

	/*
	 * Close old connections anyway; simpler to connect again than ensure they
	 * are healthy and not in COPY
	 */
	for (i = 0; i < lopt.nconn; i++)
	{
		ConnState *conn_state = &lstate->conns[i];

		PQfinish(conn_state->conn);
		conn_state->conn = NULL;
	}

	if (lopt.print_progress)
		report_progress(total_rows, total_xacts, starttm);
	if (lopt.no_twophase)
	{
		if (ok)
		{
			printf("All transactions were successfully committed. Completed %d xacts with %d rows\n", total_xacts, total_rows);
			exit(0);
		}
		else
		{
			printf("Something went wrong, examine the logs. Completed %d xacts with %d rows\n", total_xacts, total_rows);
			exit(1);
		}
	}

	if (ok)
		printf("All transactions were successfully prepared with gid containing \"shmn_loader_%s\". Completed %d xacts with %d rows. Proceeding to commit them...\n",
			   lopt.table_name, total_xacts, total_rows);
	else
		printf("Some errors occured. Trying to rollback all xacts containing \"shmn_loader_%s\" in gid...\n", lopt.table_name, total_xacts, total_rows);

	snprintf(sql, SQLSIZE, "select gid from pg_prepared_xacts where gid ~ '.*shmnloader_%s.*'", lopt.table_name);
	/* Doesn't work with direct connstr */
	for (i = 0; i < lstate->numnodes; i++)
	{
		PGconn *conn;
		PGresult* res;
		PGresult* res_gids;
		Node *node = &lstate->nodes[i];
		int j;
		char sqlfin[SQLSIZE];
		int node_id;

		conn = PQconnectdb(node->connstr);
		if (PQstatus(conn) != CONNECTION_OK)
		{
			fin_ok = false;
			fprintf(stderr, "Failed to connect to node %d: %s",
					node->id, PQerrorMessage(conn));
			PQfinish(conn);
			continue;
		}

		res_gids = PQexec(conn, sql);
		if (PQresultStatus(res_gids) != PGRES_TUPLES_OK)
		{
			fin_ok = false;
			fprintf(stderr, "%s", PQerrorMessage(conn));
			PQclear(res_gids);
			PQfinish(conn);
			continue;
		}

		for (j = 0; j < PQntuples(res_gids); j++)
		{
			char *pgid = PQgetvalue(res_gids, j, 0);

			snprintf(sqlfin, SQLSIZE, "%s prepared '%s'", ok ? "commit" : "rollback",
					 pgid);

			res = PQexec(conn, sqlfin);
			if (PQresultStatus(res) != PGRES_COMMAND_OK)
			{
				fin_ok = false;
				fprintf(stderr, "Failed to finish xact: %s",
						PQerrorMessage(conn));
			}
			PQclear(res);
		}
		PQclear(res_gids);
		PQfinish(conn);
	}

	if (fin_ok)
		printf("Done\n");
	else
		printf("Something went wrong\n");
}

static void
usage(const char *progname)
{
	printf("%s Dummy loader for shardman\n\n", progname);
	printf("Usage:\n  %s [OPTION]... <lord_connstring> <table_name> \n\n", progname);
	printf("Options:\n");
	printf("  -c, --num-conn=NUM       number of connections, default 1\n");
	printf("  -r, --rows-per-xact=NUM  rows per xact, default 4095\n");
	printf("  --no-twophase            don't use 2PC\n");
	printf("  -d, --delimiter=DELIM    delimiter, default ','\n");
	printf("  -q, --quote=QUOTE        quote, default '\"'\n");
	printf("  -e, --escape=ESC         escape, default '\"'\n");
	printf("  -P, --print-progress	   print progress);\n");
	printf("  --report-every-rows=NUM  print progress every NUM rows, default 10000);\n");
	printf("  -f, --file-path=FILEPATH path to CSV file; if not given, read from stdin.\n");
	printf("  -?, --help               show this help, then exit\n");
}

int main(int argc, char **argv)
{
	char c;
	LoaderState lstate;

	/* Parse options */
	static struct option long_options[] = {
		{"help", no_argument, NULL, 'h'},
		/* getopt_long returns 'c' since flag is NULL */
		{"num-conn", required_argument, NULL, 'c'},
		/* 1 will be stored in lopt.no_twophase; getopt_long will return 0 */
		{"no-twophase", no_argument, &lopt.no_twophase, 1},
		{"delimiter", required_argument, NULL, 'd'},
		{"quote", required_argument, NULL, 'q'},
		{"escape", required_argument, NULL, 'e'},
		{"print-progress", no_argument, NULL, 'P'},
		{"report-every-rows", required_argument, NULL, 2},
		{"file-path", required_argument, NULL, 'f'},
		/* hidden opts */
		{"direct-connstr", required_argument, NULL, 3},
		{NULL, 0, NULL, 0} /* the last element must be zeros */
	};

	/* defaults */
	lopt.no_twophase = 0;
	lopt.direct_connstr = NULL;
	lopt.nconn = 1;
	lopt.rows_per_xact = 4095;
	lopt.delimiterc = ',';
	lopt.quotec = '"';
	lopt.escapec = '"';
	lopt.print_progress = false;
	lopt.report_every_rows = 10000;
	lopt.file_path = NULL;

	optind = 1;
	/* : after opt means it has argument */
	while ((c = getopt_long(argc, argv, "hc:r:d:q:e:Pf:", long_options, NULL)) != -1)
	{
		switch (c)
		{
			case '?':
				fprintf(stderr, "Try \"%s --help\" for more information.\n", argv[0]);
				exit(1);
			case 'h':
				usage(argv[0]);
				exit(0);

			case 'c':
				lopt.nconn = atoi(optarg);
				break;
			case 'r':
				lopt.rows_per_xact = atoi(optarg);
				break;
			case 'd':
				lopt.delimiterc = optarg[0];
				break;
			case 'q':
				lopt.quotec = optarg[0];
				break;
			case 'e':
				lopt.escapec = optarg[0];
				break;
			case 'P':
				lopt.print_progress = true;
				break;
			case 'f':
				lopt.file_path = optarg;
				break;

			case 0:
				/* This covers long options without short form, saved directly */
				break;

			/* options without short form not saved directly */
			case 2:
				lopt.report_every_rows = atoi(optarg);
				break;
			case 3:
				lopt.direct_connstr = optarg;
				break;

			default:
				/* getopt_long already issued a suitable error message */
				fprintf(stderr, "Try \"%s --help\" for more information.\n", argv[0]);
				exit(1);
		}
	}

	/* Positional args */
	if (argc - optind < 2)
	{
		fprintf(stderr, "Not enough positional arguments. Try \"%s --help\" for more information.\n", argv[0]);
		exit(1);
	}
	lopt.lord_connstring = argv[optind++];
	lopt.table_name = argv[optind++];

	memset(&lstate, 0, sizeof(LoaderState));
	learn_nodes_and_table(&lstate);

	prepare_connections(&lstate);
	do_copy(&lstate);

	return 0;
}

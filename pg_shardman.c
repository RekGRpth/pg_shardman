/* -------------------------------------------------------------------------
 *
 * pg_shardman.c
 *
 * Copyright (c) 2017, Postgres Professional
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/pg_type.h"
#include "commands/event_trigger.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/latch.h"
#include "utils/guc.h"
#include "utils/rel.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

/* ensure that extension won't load against incompatible version of Postgres */
PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(shardlord_connection_string);
PG_FUNCTION_INFO_V1(synchronous_replication);
PG_FUNCTION_INFO_V1(is_shardlord);
PG_FUNCTION_INFO_V1(broadcast);
PG_FUNCTION_INFO_V1(reconstruct_table_attrs);
PG_FUNCTION_INFO_V1(pq_conninfo_parse);
PG_FUNCTION_INFO_V1(get_system_identifier);
PG_FUNCTION_INFO_V1(reset_synchronous_standby_names_on_commit);

/* GUC variables */
static bool is_lord;
static bool sync_replication;
static char *shardlord_connstring;

extern void _PG_init(void);

static bool reset_ssn_callback_set = false;
static bool reset_ssn_requested = false;

static void reset_ssn_xact_callback(XactEvent event, void *arg);

/*
 * Entrypoint of the module. Define GUCs.
 */
void
_PG_init()
{
	DefineCustomBoolVariable(
		"shardman.sync_replication",
		"Toggle synchronous replication",
		NULL,
		&sync_replication,
		false,
		PGC_SUSET,
		0,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"shardman.shardlord",
		"This node is the shardlord?",
		NULL,
		&is_lord,
		false,
		PGC_SUSET,
		0,
		NULL, NULL, NULL);

	DefineCustomStringVariable(
		"shardman.shardlord_connstring",
		"Active only if shardman.shardlord is on. Connstring to reach shardlord from"
		"worker nodes to set up logical replication",
		NULL,
		&shardlord_connstring,
		"",
		PGC_SUSET,
		0,
		NULL, NULL, NULL);
}

Datum
shardlord_connection_string(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(shardlord_connstring));
}

Datum
synchronous_replication(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(sync_replication);
}

Datum
is_shardlord(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(is_lord);
}

/*
 * Wait until PQgetResult would certainly be non-blocking. Returns true if
 * everything is ok, false on error.
 */
static bool
wait_command_completion(PGconn* conn)
{
	while (PQisBusy(conn))
	{
		/* Sleep until there's something to do */
		int wc = WaitLatchOrSocket(MyLatch,
								   WL_LATCH_SET | WL_SOCKET_READABLE,
								   PQsocket(conn),
#if defined (PGPRO_EE)
								   false,
#endif
								   -1L, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		CHECK_FOR_INTERRUPTS();

		/* Data available in socket? */
		if (wc & WL_SOCKET_READABLE)
		{
			if (!PQconsumeInput(conn))
				return false;
		}
	}
	return true;
}

typedef struct
{
	PGconn* con;
	char*   sql;
	int     node;
} Channel;

Datum
broadcast(PG_FUNCTION_ARGS)
{
	char* sql_full = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char* cmd = pstrdup(sql_full);
	bool  ignore_errors = PG_GETARG_BOOL(1);
	bool  two_phase = PG_GETARG_BOOL(2);
	bool  sync_commit_on = PG_GETARG_BOOL(3);
	bool  sequential = PG_GETARG_BOOL(4);
	bool  super_connstr = PG_GETARG_BOOL(5);
	char* iso_level = (PG_GETARG_POINTER(6) != NULL) ?
		text_to_cstring(PG_GETARG_TEXT_PP(6)) : NULL;
	char* sep;
	char* sql;
	PGresult *res;
	char* fetch_node_connstr;
	int   rc;
	int   node_id;
	int	  n;
	char* conn_str;
	int   n_cmds = 0;
	int   i;
	int n_cons = 1024; /* num of channels allocated currently */
	Channel* chan;
	PGconn* con;
	StringInfoData resp;
	StringInfoData fin_sql;

	char const* errstr = "";

	elog(DEBUG1, "Broadcast commmand '%s'",  cmd);

	initStringInfo(&resp);

	SPI_connect();
	chan = (Channel*) palloc(sizeof(Channel) * n_cons);

	/* Open connections and send all queries */
	while ((sep = strchr(cmd, *cmd == '{' ? '}' : ';')) != NULL)
	{
		*sep = '\0';

		if (*cmd == '{')
			cmd += 1;
		rc = sscanf(cmd, "%d:%n", &node_id, &n);
		if (rc != 1) {
			elog(ERROR, "SHARDMAN: Invalid command string: '%s' in '%s'",
				 cmd, sql_full);
		}
		sql = cmd + n; /* eat node id and colon */
		cmd = sep + 1;
		if (node_id != 0)
		{
			fetch_node_connstr = psprintf(
				"select %sconnection_string from shardman.nodes where id=%d",
				super_connstr ? "super_" : "", node_id);
			if (SPI_exec(fetch_node_connstr, 0) < 0 || SPI_processed != 1)
			{
				elog(ERROR, "SHARDMAN: Failed to fetch connection string for node %d",
					 node_id);
			}
			pfree(fetch_node_connstr);

			conn_str = SPI_getvalue(SPI_tuptable->vals[0],
									SPI_tuptable->tupdesc, 1);
		}
		else
		{
			if (shardlord_connstring == NULL || *shardlord_connstring == '\0')
			{
				elog(ERROR, "SHARDMAN: Shardlord connection string was not specified in configuration file");
			}
			conn_str = shardlord_connstring;
		}
		if (n_cmds >= n_cons)
		{
			chan = (Channel*) repalloc(chan, sizeof(Channel) * (n_cons *= 2));
		}

		con = PQconnectdb(conn_str);
		chan[n_cmds].con = con;
		chan[n_cmds].node = node_id;
		chan[n_cmds].sql = sql;
		n_cmds += 1;

		if (PQstatus(con) != CONNECTION_OK)
		{
			if (ignore_errors)
			{
				errstr = psprintf("%s<error>%d:Connection failure: %s</error>",
								  errstr, node_id, PQerrorMessage(con));
				chan[n_cmds-1].sql = NULL;
				continue;
			}
			errstr = psprintf("Failed to connect to node %d: %s", node_id,
							  PQerrorMessage(con));
			goto cleanup;
		}
		/* Build the actual sql to send, mem freed with ctxt */
		initStringInfo(&fin_sql);
		if (!sync_commit_on)
			appendStringInfoString(&fin_sql, "SET SESSION synchronous_commit TO local; ");
		if (iso_level)
			appendStringInfo(&fin_sql, "BEGIN TRANSACTION ISOLATION LEVEL %s; ", iso_level);
		appendStringInfoString(&fin_sql, sql);
		appendStringInfoChar(&fin_sql, ';'); /* it was removed after strchr */
		if (two_phase)
			appendStringInfoString(&fin_sql, "PREPARE TRANSACTION 'shardlord';");
		else if (iso_level)
			appendStringInfoString(&fin_sql, "END;");

		elog(DEBUG1, "Sending command '%s' to node %d", fin_sql.data, node_id);
		if (!PQsendQuery(con, fin_sql.data)
			|| (sequential && !wait_command_completion(con)))
		{
			if (ignore_errors)
			{
				errstr = psprintf("%s<error>%d:Failed to send query '%s': %s</error>",
								  errstr, node_id, fin_sql.data, PQerrorMessage(con));
				chan[n_cmds-1].sql = NULL;
				continue;
			}
			errstr = psprintf("Failed to send query '%s' to node %d: %s'", fin_sql.data,
							  node_id, PQerrorMessage(con));
			goto cleanup;
		}
	}

	if (*cmd != '\0')
	{
		elog(ERROR, "SHARDMAN: Junk at end of command list: %s", cmd);
	}

	/*
	 * Now collect results
	 */
	for (i = 0; i < n_cmds; i++)
	{
		PGresult* next_res;
		PGresult* res = NULL;
		ExecStatusType status;

		con = chan[i].con;

		if (chan[i].sql == NULL)
		{
			/* Ignore commands which were not sent */
			continue;
		}

		/* Skip all but the last result */
		while ((next_res = PQgetResult(con)) != NULL)
		{
			if (res != NULL)
			{
				PQclear(res);
			}
			res = next_res;
		}

		if (res == NULL)
		{
			if (ignore_errors)
			{
				errstr = psprintf("%s<error>%d:Failed to received response for '%s': %s</error>",
								  errstr, chan[i].node, chan[i].sql, PQerrorMessage(con));
				continue;
			}
			errstr = psprintf("Failed to receive response for query %s from node %d: %s",
							  chan[i].sql, chan[i].node, PQerrorMessage(con));
			goto cleanup;
		}

		/* Ok, result was successfully fetched, add it to resp */
		status = PQresultStatus(res);
		if (status != PGRES_TUPLES_OK && status != PGRES_COMMAND_OK)
		{
			if (ignore_errors)
			{
				errstr = psprintf("%s<error>%d:Command %s failed: %s</error>",
								  errstr, chan[i].node, chan[i].sql, PQerrorMessage(con));
				PQclear(res);
				continue;
			}
			errstr = psprintf("Command %s failed at node %d: %s",
							  chan[i].sql, chan[i].node, PQerrorMessage(con));
			PQclear(res);
			goto cleanup;
		}
		if (i != 0)
		{
			appendStringInfoChar(&resp, ',');
		}
		if (status == PGRES_TUPLES_OK)
		{
			if (PQntuples(res) != 1 || PQgetisnull(res, 0, 0))
			{
				if (ignore_errors)
				{
					appendStringInfoString(&resp, "?");
					elog(WARNING, "SHARDMAN: Query '%s' doesn't return single tuple at node %d",
						 chan[i].sql, chan[i].node);
				}
				else
				{
					errstr = psprintf("Query '%s' doesn't return single tuple at node %d",
									  chan[i].sql, chan[i].node);
					PQclear(res);
					goto cleanup;
				}
			}
			else
			{
				appendStringInfo(&resp, "%s", PQgetvalue(res, 0, 0));
			}
		}
		else
		{
			appendStringInfo(&resp, "%d", PQntuples(res));
		}
		PQclear(res);
	}

  cleanup:
	for (i = 0; i < n_cmds; i++)
	{
		con = chan[i].con;
		if (two_phase)
		{
			if (*errstr)
			{
				res = PQexec(con, "ROLLBACK PREPARED 'shardlord'");
				if (PQresultStatus(res) != PGRES_COMMAND_OK)
				{
					elog(WARNING, "SHARDMAN: Rollback of 2PC failed at node %d: %s",
						 chan[i].node, PQerrorMessage(con));
				}
				PQclear(res);
			}
			else
			{
				res = PQexec(con, "COMMIT PREPARED 'shardlord'");
				if (PQresultStatus(res) != PGRES_COMMAND_OK)
				{
					elog(WARNING, "SHARDMAN: Commit of 2PC failed at node %d: %s",
						 chan[i].node, PQerrorMessage(con));
				}
				PQclear(res);
			}
		}
		PQfinish(con);
	}

	if (*errstr)
	{
		if (ignore_errors)
		{
			resetStringInfo(&resp);
			appendStringInfoString(&resp, errstr);
			elog(WARNING, "SHARDMAN: %s", errstr);
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
					 errmsg("SHARDMAN: %s", errstr)));
		}
	}

	pfree(chan);
	SPI_finish();

	PG_RETURN_TEXT_P(cstring_to_text(resp.data));
}

/*
 * Generate CREATE TABLE sql for relation via pg_dump. We use it for root
 * (parent) tables because pg_dump dumps all the info -- indexes, constrains,
 * defaults, everything. Parameter is not REGCLASS because pg_dump can't
 * handle oids anyway. Connstring must be proper libpq connstring, it is feed
 * to pg_dump.
 * TODO: actually we should have muchmore control on what is dumped, so we
 * need to copy-paste parts of messy pg_dump or collect the needed data
 * manually walking over catalogs.
 */
PG_FUNCTION_INFO_V1(gen_create_table_sql);
Datum
gen_create_table_sql(PG_FUNCTION_ARGS)
{
	char pg_dump_path[MAXPGPATH];
	/* let the mmgr free that */
	char *relation = text_to_cstring(PG_GETARG_TEXT_PP(0));
	const size_t chunksize = 5; /* read max that bytes at time */
	/* how much already allocated *including header* */
	size_t pallocated = VARHDRSZ + chunksize;
	text *sql = (text *) palloc(pallocated);
	char *ptr = VARDATA(sql); /* ptr to first free byte */
	char *cmd;
	FILE *fp;
	size_t bytes_read;

	SET_VARSIZE(sql, VARHDRSZ);

	/* find pg_dump location querying pg_config */
	SPI_connect();
	if (SPI_execute("select setting from pg_config where name = 'BINDIR';",
					true, 0) < 0)
		elog(FATAL, "SHARDMAN: Failed to query pg_config");
	strcpy(pg_dump_path,
		   SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));
	SPI_finish();
	join_path_components(pg_dump_path, pg_dump_path, "pg_dump");
	canonicalize_path(pg_dump_path);

	cmd = psprintf("%s -t '%s' --no-owner --schema-only --dbname='%s' 2>&1",
				   pg_dump_path, relation, shardlord_connstring);

	if ((fp = popen(cmd, "r")) == NULL)
	{
		elog(ERROR, "SHARDMAN: Failed to run pg_dump, cmd %s", cmd);
	}

	while ((bytes_read = fread(ptr, sizeof(char), chunksize, fp)) != 0)
	{
		SET_VARSIZE(sql, VARSIZE_ANY(sql) + bytes_read);
		if (pallocated - VARSIZE_ANY(sql) < chunksize)
		{
			pallocated *= 2;
			sql = (text *) repalloc(sql, pallocated);
		}
		/* since we realloc, can't just += bytes_read here */
		ptr = VARDATA(sql) + VARSIZE_ANY_EXHDR(sql);
	}

	if (pclose(fp))	{
		elog(ERROR, "SHARDMAN: pg_dump exited with error status, output was\n%scmd was \n%s",
			 text_to_cstring(sql), cmd);
	}

	PG_RETURN_TEXT_P(sql);
}

/*
 * Reconstruct attrs part of CREATE TABLE stmt, e.g. (i int NOT NULL, j int).
 * The only constraint reconstructed is NOT NULL.
 */
Datum
reconstruct_table_attrs(PG_FUNCTION_ARGS)
{
	StringInfoData query;
	Oid	relid = PG_GETARG_OID(0);
	Relation local_rel = heap_open(relid, AccessExclusiveLock);
	TupleDesc local_descr = RelationGetDescr(local_rel);
	int i;

	initStringInfo(&query);
	appendStringInfoChar(&query, '(');

	for (i = 0; i < local_descr->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(local_descr, i);

		if (i != 0)
			appendStringInfoString(&query, ", ");

		/* NAME TYPE[(typmod)] [NOT NULL] [COLLATE "collation"] */
		appendStringInfo(&query, "%s %s%s%s",
						 quote_identifier(NameStr(attr->attname)),
						 format_type_extended(attr->atttypid, attr->atttypmod,
											  FORMAT_TYPE_TYPEMOD_GIVEN |
											  FORMAT_TYPE_FORCE_QUALIFY),
						 (attr->attnotnull ? " NOT NULL" : ""),
						 (attr->attcollation ?
						  psprintf(" COLLATE \"%s\"",
								   get_collation_name(attr->attcollation)) :
						  ""));
	}

	appendStringInfoChar(&query, ')');

	/* Let xact unlock this */
	heap_close(local_rel, NoLock);
	PG_RETURN_TEXT_P(cstring_to_text(query.data));
}

/*
 * Basically, this is an sql wrapper around PQconninfoParse. Given libpq
 * connstring, it returns a pair of keywords and values arrays with valid
 * nonempty options.
 */
Datum
pq_conninfo_parse(PG_FUNCTION_ARGS)
{
	TupleDesc            tupdesc;
	/* array of keywords and array of vals as in PQconninfoOption */
	Datum		values[2];
	bool		nulls[2] = { false, false };
	ArrayType *keywords; /* array of keywords */
	ArrayType *vals; /* array of vals */
	text **keywords_txt; /* we construct array of keywords from it */
	text **vals_txt; /* array of vals constructed from it */
	Datum *elems; /* just to convert text * to it */
	int32 text_size;
	int numopts = 0;
	int i;
	size_t len;
	int16 typlen;
	bool typbyval;
	char typalign;
	char *pqerrmsg;
	char *errmsg_palloc;
	char *conninfo = text_to_cstring(PG_GETARG_TEXT_PP(0));
	PQconninfoOption *opts = PQconninfoParse(conninfo, &pqerrmsg);
	PQconninfoOption *opt;
	HeapTuple res_heap_tuple;

	if (pqerrmsg != NULL)
	{
		/* free malloced memory to avoid leakage */
		errmsg_palloc = pstrdup(pqerrmsg);
		PQfreemem((void *) pqerrmsg);
		elog(ERROR, "SHARDMAN: PQconninfoParse failed: %s", errmsg_palloc);
	}

	/* compute number of opts and allocate text ptrs */
	for (opt = opts; opt->keyword != NULL; opt++)
	{
		/* We are interested only in filled values */
		if (opt->val != NULL)
			numopts++;
	}
	keywords_txt = palloc(numopts * sizeof(text*));
	vals_txt = palloc(numopts * sizeof(text*));

	/* Fill keywords and vals */
	for (opt = opts, i = 0; opt->keyword != NULL; opt++)
	{
		if (opt->val != NULL)
		{
			len = strlen(opt->keyword);
			text_size = VARHDRSZ + len;
			keywords_txt[i] = (text *) palloc(text_size);
			SET_VARSIZE(keywords_txt[i], text_size);
			memcpy(VARDATA(keywords_txt[i]), opt->keyword, len);

			len = strlen(opt->val);
			text_size = VARHDRSZ + len;
			vals_txt[i] = (text *) palloc(text_size);
			SET_VARSIZE(vals_txt[i], text_size);
			memcpy(VARDATA(vals_txt[i]), opt->val, len);
			i++;
		}
	}

	/* Now construct arrays */
	elems = (Datum*) palloc(numopts * sizeof(Datum));
	/* get info about text type, we will pass it to array constructor */
	get_typlenbyvalalign(TEXTOID, &typlen, &typbyval, &typalign);

	/* cast text * to datums for purity and construct array */
	for (i = 0; i < numopts; i++) {
		elems[i] = PointerGetDatum(keywords_txt[i]);
	}
	keywords = construct_array(elems, numopts, TEXTOID, typlen, typbyval,
							   typalign);
	/* same for valus */
	for (i = 0; i < numopts; i++) {
		elems[i] = PointerGetDatum(vals_txt[i]);
	}
	vals = construct_array(elems, numopts, TEXTOID, typlen, typbyval,
							   typalign);

	/* prepare to form the tuple */
	values[0] = PointerGetDatum(keywords);
	values[1] = PointerGetDatum(vals);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));
	BlessTupleDesc(tupdesc); /* Inshallah */

	PQconninfoFree(opts);
	res_heap_tuple = heap_form_tuple(tupdesc, values, nulls);
	PG_RETURN_DATUM(HeapTupleGetDatum(res_heap_tuple));
}

Datum
get_system_identifier(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64(GetSystemIdentifier());
}

/*
 * Execute "ALTER SYSTEM SET synchronous_standby_names = '' on commit"
 */
Datum
reset_synchronous_standby_names_on_commit(PG_FUNCTION_ARGS)
{
	if (!reset_ssn_callback_set)
		RegisterXactCallback(reset_ssn_xact_callback, NULL);
	reset_ssn_requested = true;
	PG_RETURN_VOID();
}

static void
reset_ssn_xact_callback(XactEvent event, void *arg)
{
	if (reset_ssn_requested)
	{
		/* I just wanted to practice a bit with PG nodes and lists */
		A_Const *aconst = makeNode(A_Const);
		List *set_stmt_args = list_make1(aconst);
		VariableSetStmt setstmt;
		AlterSystemStmt altersysstmt;

		aconst->val.type = T_String;
		aconst->val.val.str = ""; /* set it to empty value */
		aconst->location = -1;

		setstmt.type = T_VariableSetStmt;
		setstmt.kind = VAR_SET_VALUE;
		setstmt.name = "synchronous_standby_names";
		setstmt.args = set_stmt_args;

		altersysstmt.type = T_AlterSystemStmt;
		altersysstmt.setstmt = &setstmt;
		AlterSystemSetConfigFile(&altersysstmt);
		pg_reload_conf(NULL);

		list_free_deep(setstmt.args);
		reset_ssn_requested = false;
	}
}

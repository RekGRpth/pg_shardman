/* -------------------------------------------------------------------------
 *
 * pg_shardman.c
 *
 * Copyright (c) 2017, Postgres Professional
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "pgstat.h"
#include "utils/guc.h"
#include "utils/rel.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "catalog/pg_type.h"
#include "access/htup_details.h"
#include "storage/latch.h"

/* ensure that extension won't load against incompatible version of Postgres */
PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(shardlord_connection_string);
PG_FUNCTION_INFO_V1(synchronous_replication);
PG_FUNCTION_INFO_V1(broadcast);
PG_FUNCTION_INFO_V1(reconstruct_table_attrs);
PG_FUNCTION_INFO_V1(pq_conninfo_parse);

/* GUC variables */
static bool is_shardlord;
static bool sync_replication;
static char *shardlord_connstring;

extern void _PG_init(void);

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
		&is_shardlord,
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

static bool
wait_command_completion(PGconn* conn)
{
	while (PQisBusy(conn))
	{
		/* Sleep until there's something to do */
		int wc = WaitLatchOrSocket(MyLatch,
								   WL_LATCH_SET | WL_SOCKET_READABLE,
								   PQsocket(conn),
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
	return  true;
}

Datum
broadcast(PG_FUNCTION_ARGS)
{
	char* sql = text_to_cstring(PG_GETARG_TEXT_PP(0));
	bool  ignore_errors = PG_GETARG_BOOL(1);
	bool  two_phase = PG_GETARG_BOOL(2);
	bool  sync_commit_on = PG_GETARG_BOOL(3);
	bool  sequential = PG_GETARG_BOOL(4);
	bool  super_connstr = PG_GETARG_BOOL(5);
	char* sep;
	PGresult *res;
	char* fetch_node_connstr;
	int   rc;
	int   node_id;
	int   n;
	char* conn_str;
	int   n_cmds = 0;
	int   i;
	int n_cons = 1024;
	PGconn** conn;
	StringInfoData resp;

	char* errmsg = NULL;

	elog(DEBUG1, "Broadcast commmand '%s'",  sql);

	SPI_connect();

	conn = (PGconn**) palloc(sizeof(PGconn*) * n_cons);
	initStringInfo(&resp);

	while ((sep = strchr(sql, *sql == '{' ? '}' : ';')) != NULL)
	{
		*sep = '\0';

		if (*sql == '{')
			sql += 1;
		rc = sscanf(sql, "%d:%n", &node_id, &n);
		if (rc != 1) {
			elog(ERROR, "SHARDMAN: Invalid command string: %s", sql);
		}
		sql += n;
		if (node_id != 0)
		{
			if (super_connstr)
				fetch_node_connstr = psprintf(
					"select super_connection_string from shardman.nodes where id=%d",
					node_id);
			else
				fetch_node_connstr = psprintf(
					"select connection_string from shardman.nodes where id=%d",
					node_id);
			if (SPI_exec(fetch_node_connstr, 0) < 0 || SPI_processed != 1)
			{
				elog(ERROR, "SHARDMAN: Failed to fetch connection string for node %d",
					 node_id);
			}
			pfree(fetch_node_connstr);

			conn_str = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
		}
		else
		{
			conn_str = shardlord_connstring;
		}
		if (n_cmds >= n_cons)
		{
			conn = (PGconn**) repalloc(conn, sizeof(PGconn*) * (n_cons *= 2));
		}

		conn[n_cmds] = PQconnectdb(conn_str);
		if (PQstatus(conn[n_cmds++]) != CONNECTION_OK)
		{
			if (ignore_errors)
			{
				errmsg = psprintf("%s%d:Connection failure: %s;",
								  errmsg ? errmsg : "", node_id,
								  PQerrorMessage(conn[n_cmds - 1]));
				continue;
			}
			errmsg = psprintf("Failed to connect to node %d: %s", node_id,
							  PQerrorMessage(conn[n_cmds-1]));
			goto cleanup;
		}
		if (!sync_commit_on)
		{
			if (two_phase)
			{
				sql = psprintf("SET SESSION synchronous_commit TO local; BEGIN; %s; PREPARE TRANSACTION 'shardlord';", sql);
			}
			else
			{
				sql = psprintf("SET SESSION synchronous_commit TO local; %s", sql);
			}
		}
		elog(DEBUG1, "Send command '%s' to node %d", sql, node_id);
		if (!PQsendQuery(conn[n_cmds - 1], sql)
			|| (sequential && !wait_command_completion(conn[n_cmds - 1])))
		{
			if (ignore_errors)
			{
				errmsg = psprintf("%s%d:Failed to send query '%s': %s'",
								  errmsg ? errmsg : "", node_id, sql,
								  PQerrorMessage(conn[n_cmds-1]));
				continue;
			}
			errmsg = psprintf("Failed to send query '%s' to node %d: %s'", sql,
							  node_id, PQerrorMessage(conn[n_cmds-1]));
			goto cleanup;
		}
		if (!sync_commit_on)
			pfree(sql);

		sql = sep + 1;
	}

	if (*sql != '\0')
	{
		elog(ERROR, "SHARDMAN: Junk at end of command list: %s", sql);
	}

	for (i = 0; i < n_cmds; i++)
	{
		PGresult* next_res;
		PGresult* res = NULL;
		ExecStatusType status;

		while ((next_res = PQgetResult(conn[i])) != NULL)
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
				errmsg = psprintf("%s%d:Failed to received response for '%s': %s", errmsg ? errmsg : "", node_id, sql, PQerrorMessage(conn[i]));
				continue;
			}
			errmsg = psprintf("Failed to receive response for query %s from node %d: %s", sql, node_id, PQerrorMessage(conn[i]));
			goto cleanup;
		}

		/* Ok, result was successfully fetched */
		status = PQresultStatus(res);
		if (status != PGRES_TUPLES_OK && status != PGRES_COMMAND_OK)
		{
			if (ignore_errors)
			{
				errmsg = psprintf("%s%d:Command %s failed: %s", errmsg ? errmsg : "", node_id, sql, PQerrorMessage(conn[i]));
				PQclear(res);
				continue;
			}
			errmsg = psprintf("Command %s failed at node %d: %s", sql, node_id, PQerrorMessage(conn[i]));
			PQclear(res);
			goto cleanup;
		}
		if (status == PGRES_TUPLES_OK)
		{
			if (PQntuples(res) != 1 || PQgetisnull(res, 0, 0))
			{
				if (ignore_errors)
				{
					appendStringInfoString(&resp, "?;");
					elog(WARNING, "SHARDMAN: Query '%s' doesn't return single tuple at node %d", sql, node_id);
				}
				else
				{
					errmsg = psprintf("Query '%s' doesn't return single tuple at node %d", sql, node_id);
					PQclear(res);
					goto cleanup;
				}
			}
			else
			{
				appendStringInfo(&resp, "%s;", PQgetvalue(res, 0, 0));
			}
		}
		else
		{
			appendStringInfo(&resp, "%d;", PQntuples(res));
		}
		PQclear(res);
	}
  cleanup:
	for (i = 0; i < n_cmds; i++)
	{
		if (two_phase)
		{
			if (errmsg)
			{
				res = PQexec(conn[i], "ROLLBACK PREPARED 'shardlord'");
				if (PQresultStatus(res) != PGRES_COMMAND_OK)
				{
					elog(WARNING, "SHARDMAN: Rollback of 2PC failed at node %d: %s",
						 node_id, PQerrorMessage(conn[i]));
				}
				PQclear(res);
			}
			else
			{
				res = PQexec(conn[i], "COMMIT PREPARED 'shardlord'");
				if (PQresultStatus(res) != PGRES_COMMAND_OK)
				{
					elog(WARNING, "SHARDMAN: Commit of 2PC failed at node %d: %s",
						 node_id, PQerrorMessage(conn[i]));
				}
				PQclear(res);
			}
		}
		PQfinish(conn[i]);
	}

	if (errmsg)
	{
		if (ignore_errors)
		{
			appendStringInfo(&resp, "Error:%s", errmsg);
			elog(WARNING, "SHARDMAN: %s", errmsg);
		}
		else
		{
			elog(ERROR, "SHARDMAN: %s", errmsg);
		}
	}

	pfree(conn);
	SPI_finish();

	PG_RETURN_TEXT_P(cstring_to_text(resp.data));
}

/*
 * Generate CREATE TABLE sql for relation via pg_dump. We use it for root
 * (parent) tables because pg_dump dumps all the info -- indexes, constrains,
 * defaults, everything. Parameter is not REGCLASS because pg_dump can't
 * handle oids anyway. Connstring must be proper libpq connstring, it is feed
 * to pg_dump.
 * TODO: actually we should have much more control on what is dumped, so we
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

	cmd = psprintf("%s -t '%s' --schema-only --dbname='%s' 2>&1",
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
		Form_pg_attribute attr = local_descr->attrs[i];

		if (i != 0)
			appendStringInfoString(&query, ", ");

		/* NAME TYPE[(typmod)] [NOT NULL] [COLLATE "collation"] */
		appendStringInfo(&query, "%s %s%s%s",
						 quote_identifier(NameStr(attr->attname)),
						 format_type_with_typemod_qualified(attr->atttypid,
															attr->atttypmod),
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
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

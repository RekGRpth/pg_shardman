#include "postgres.h"
#include "commands/event_trigger.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "commands/dbcommands.h"
#include "miscadmin.h"
#include "funcapi.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "storage/lmgr.h"
#include "libpq-fe.h"

/*
 * Must be called iff we are dropping extension. Checks that we are dropping
 * pg_shardman extension and calls pg_shardman_cleanup to perform the actual
 * cleanup.
 */
PG_FUNCTION_INFO_V1(pg_shardman_cleanup_c);
Datum
pg_shardman_cleanup_c(PG_FUNCTION_ARGS)
{
    EventTriggerData *trigdata;
	DropStmt *stmt;
	Value *value;
	char *ext_name;

    if (!CALLED_AS_EVENT_TRIGGER(fcinfo))  /* internal error */
        elog(ERROR, "not fired by event trigger manager");

    trigdata = (EventTriggerData *) fcinfo->context;
	Assert(trigdata->parsetree->type == T_DropStmt);
	stmt = (DropStmt *) trigdata->parsetree;
	Assert(stmt->removeType == OBJECT_EXTENSION);
	/* So it is list of pointers to Nodes */
	Assert(stmt->objects->type == T_List);
	/* To Value nodes, actually */
	Assert(((Node *) (linitial(stmt->objects)))->type == T_String);
	/* Seems like no way to use smth like linitial_node, because Value node
	 * can have different types	*/

	value = (Value *) linitial(stmt->objects);
	ext_name = strVal(value);
	if (strcmp(ext_name, "pg_shardman") == 0)
	{
		/* So we are dropping pg_shardman */
		const char *cmd_sql = "select shardman.pg_shardman_cleanup();";

		SPI_connect();
		if (SPI_execute(cmd_sql, true, 0) < 0)
			elog(FATAL, "Stmt failed: %s", cmd_sql);
		SPI_finish();
	}

    PG_RETURN_NULL();
}

/*
 * Generate CREATE TABLE sql for relation via pg_dump. We use it for root
 * (parent) tables because pg_dump dumps all the info -- indexes, constrains,
 * defaults, everything. Parameter is not REGCLASS because pg_dump can't
 * handle oids anyway. Connstring must be proper libpq connstring, it is feed
 * to pg_dump.
 */
PG_FUNCTION_INFO_V1(gen_create_table_sql);
Datum
gen_create_table_sql(PG_FUNCTION_ARGS)
{
	char pg_dump_path[MAXPGPATH];
	/* let the mmgr free that */
	char *relation = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char *connstring =  text_to_cstring(PG_GETARG_TEXT_PP(1));
	const size_t chunksize = 5; /* read max that bytes at time */
	/* how much already allocated *including header* */
	size_t pallocated = VARHDRSZ + chunksize;
	text *sql = (text *) palloc(pallocated);
	char *ptr = VARDATA(sql); /* ptr to first free byte */
	char *cmd;
	FILE *fp;
	size_t bytes_read;
	SET_VARSIZE(sql, VARHDRSZ);
	elog(INFO, "RUNNING GEN CREATE TABLE");

	/* find pg_dump location */
	if (find_my_exec("pg_dump", pg_dump_path) != 0)
	{
		elog(ERROR, "Failed to find pg_dump location");
	}

	cmd = psprintf("%s -t '%s' --schema-only --dbname='%s' 2>&1",
				   pg_dump_path, relation, connstring);

	if ((fp = popen(cmd, "r")) == NULL)
	{
		elog(ERROR, "Failed to run pg_dump, cmd %s", cmd);
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
		elog(ERROR, "pg_dump exited with error status, output was\n%scmd was \n%s",
			 text_to_cstring(sql), cmd);
	}

	PG_RETURN_TEXT_P(sql);
}

/*
 * Reconstruct attrs part of CREATE TABLE stmt, e.g. (i int NOT NULL, j int).
 * The only constraint reconstructed is NOT NULL.
 */
PG_FUNCTION_INFO_V1(reconstruct_table_attrs);
Datum
reconstruct_table_attrs(PG_FUNCTION_ARGS)
{
	StringInfoData query;
	Oid	relid = PG_GETARG_OID(0);
	Relation local_rel = heap_open(relid, AccessExclusiveLock);
	TupleDesc local_descr = RelationGetDescr(local_rel);
	int i;
	text *text_query;
	int32 text_query_size;

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

	/* Now prepare result as 'text' */
	text_query_size = VARHDRSZ + query.len;
	text_query = (text *) palloc(text_query_size);
	SET_VARSIZE(text_query, text_query_size);
	memcpy(VARDATA(text_query), query.data, query.len);

	/* Let xact unlock this */
	heap_close(local_rel, NoLock);
	PG_RETURN_TEXT_P(text_query);
}

/*
 * Basically, this is an sql wrapper around PQconninfoParse. Given libpq
 * connstring, it returns a pair of keywords and values arrays with valid
 * nonempty options.
 */
PG_FUNCTION_INFO_V1(pq_conninfo_parse);
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
		elog(ERROR, "PQconninfoParse failed: %s", errmsg_palloc);
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

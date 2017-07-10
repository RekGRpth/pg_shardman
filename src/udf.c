#include "postgres.h"
#include "commands/event_trigger.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "commands/dbcommands.h"
#include "miscadmin.h"

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
 * Generate CREATE TABLE sql for relation via pg_dump. Parameter is not
 * REGCLASS because pg_dump can't handle oids anyway.
 */
PG_FUNCTION_INFO_V1(gen_create_table_sql);
Datum
gen_create_table_sql(PG_FUNCTION_ARGS)
{
	char pg_dump_path[MAXPGPATH];
	Name db; /* current db name */
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

	/* find pg_dump location */
	if (find_my_exec("pg_dump", pg_dump_path) != 0)
	{
		elog(ERROR, "Failed to find pg_dump location");
	}
	/* find current db */
	db = (Name) palloc(NAMEDATALEN);
	namestrcpy(db, get_database_name(MyDatabaseId));

	cmd = psprintf("%s -t '%s' --schema-only '%s' 2>&1",
				   pg_dump_path, relation, (NameStr(*db)));

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

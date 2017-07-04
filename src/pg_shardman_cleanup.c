#include "postgres.h"
#include "commands/event_trigger.h"
#include "executor/spi.h"

PG_FUNCTION_INFO_V1(pg_shardman_cleanup_c);

/*
 * Must be called iff we are dropping extension. Checks that we are dropping
 * pg_shardman extension and calls pg_shardman_cleanup to perform the actual
 * cleanup.
 */
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
		int e;
		const char *cmd_sql = "select shardman.pg_shardman_cleanup();";

		SPI_connect();
		e = SPI_execute(cmd_sql, true, 0);
		if (e < 0)
			elog(FATAL, "Stmt failed: %s", cmd_sql);
		SPI_finish();
	}

    PG_RETURN_NULL();
}

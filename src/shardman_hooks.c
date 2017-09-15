/* ------------------------------------------------------------------------
 *
 * hooks.c
 *		shardman hooks
 *
 * Copyright (c) 2017, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "postgres.h"
#include "utils/elog.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/proc.h"

#include "pg_shardman.h"
#include "shardman_hooks.h"

emit_log_hook_type old_log_hook;

/*
 * Add [SHND x] where x is node id to each log message, if '%z' is in
 * log_line_prefix. Seems like there is no way to hook something into
 * prefix iself without touching core code.
 */
void
shardman_log(ErrorData *edata)
{
	MemoryContext oldcontext;

	/* Invoke original hook if needed */
	if (old_log_hook != NULL)
		old_log_hook(edata);

	if (shardman_my_id != SHMN_INVALID_NODE_ID &&
		strstr(Log_line_prefix, "%z") != NULL)
	{
		oldcontext = MemoryContextSwitchTo(edata->assoc_context);

		edata->message = psprintf("[SHND %d] %s", shardman_my_id, edata->message);

		MemoryContextSwitchTo(oldcontext);
	}
}

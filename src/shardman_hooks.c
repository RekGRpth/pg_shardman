/* ------------------------------------------------------------------------
 *
 * hooks.c
 *		shardman hooks
 *
 * ------------------------------------------------------------------------
 */

#include "postgres.h"
#include "utils/elog.h"

#include "pg_shardman.h"
#include "shardman_hooks.h"

emit_log_hook_type log_hook_next;

/*
 * Add [SHNODE x] where x is node id to each log message, if '%z' is in
 * log_line_prefix. Seems like there is no way to hook something into
 * prefix iself without touching core code.
 * TODO: In some, probably most interesting cases this hook is not called :(
 */
void
shardman_log_hook(ErrorData *edata)
{
	MemoryContext oldcontext;

	/* Invoke original hook if needed */
	if (log_hook_next != NULL)
		log_hook_next(edata);

	if (strstr(Log_line_prefix, "%z") != NULL &&
		shardman_my_node_id != SHMN_INVALID_NODE_ID)
	{
		oldcontext = MemoryContextSwitchTo(edata->assoc_context);

		edata->message = psprintf("[SHNODE %d] %s",
								  shardman_my_node_id, edata->message);
		printf("Log hook called, line pref is %s, message is %s", Log_line_prefix,
		edata->message);

		MemoryContextSwitchTo(oldcontext);
	}
}

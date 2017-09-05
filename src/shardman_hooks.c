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
shmem_startup_hook_type old_shmem_startup_hook;

/*
 * Add [SHND x] where x is node id to each log message, if '%z' is in
 * log_line_prefix. Seems like there is no way to hook something into
 * prefix iself without touching core code.
 */
void
shardman_log(ErrorData *edata)
{
	MemoryContext oldcontext;
	int32 id;

	/* Invoke original hook if needed */
	if (old_log_hook != NULL)
		old_log_hook(edata);

	id = my_id();
	if (id != SHMN_INVALID_NODE_ID && strstr(Log_line_prefix, "%z") != NULL)
	{
		oldcontext = MemoryContextSwitchTo(edata->assoc_context);

		edata->message = psprintf("[SHND %d] %s", id, edata->message);

		MemoryContextSwitchTo(oldcontext);
	}
}

/*
 * shmem_startup hook: allocate or attach to shared memory,
 */
void
shardman_shmem_startup(void)
{
	bool found;

	if (old_shmem_startup_hook)
		old_shmem_startup_hook();

	/*
	 * Create or attach to the shared memory state, including hash table
	 */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	snss = ShmemInitStruct("pg_shardman", sizeof(ShmnSharedState), &found);
	if (!found)
	{
		/* First time through ... */
		MemSet(snss, 0, sizeof(snss)); /* snss is not ready yet */
		snss->my_id = SHMN_INVALID_NODE_ID;
		snss->lock = &(GetNamedLWLockTranche("pg_shardman"))->lock;
	}
	LWLockRelease(AddinShmemInitLock);
}

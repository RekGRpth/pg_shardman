/* -------------------------------------------------------------------------
 * Copyright (c) 2017, Postgres Professional
 * -------------------------------------------------------------------------
 */
#ifndef SHARDMAN_HOOKS_H
#define SHARDMAN_HOOKS_H

#include "storage/ipc.h"

extern emit_log_hook_type old_log_hook;
extern shmem_startup_hook_type old_shmem_startup_hook;

extern void shardman_log(ErrorData *edata);
extern void shardman_shmem_startup(void);

#endif							/* SHARDMAN_HOOKS_H */

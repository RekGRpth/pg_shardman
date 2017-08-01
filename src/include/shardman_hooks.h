#ifndef SHARDMAN_HOOKS_H
#define SHARDMAN_HOOKS_H

extern emit_log_hook_type log_hook_next;

extern void shardman_log_hook(ErrorData *edata);

#endif							/* SHARDMAN_HOOKS_H */

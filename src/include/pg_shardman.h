#ifndef PG_SHARDMAN_H
#define PG_SHARDMAN_H

#define shmn_elog(level,fmt,...) elog(level, "[SHARDMAN] " fmt, ## __VA_ARGS__)

extern void _PG_init(void);
extern void shardmaster_main(Datum main_arg);

#endif							/* PG_SHARDMAN_H */

#ifndef PG_SHARDMAN_H
#define PG_SHARDMAN_H

#include <signal.h>

#define shmn_elog(level,fmt,...) elog(level, "[SHARDMAN] " fmt, ## __VA_ARGS__)

#define SPI_PROLOG do { \
		StartTransactionCommand(); \
		SPI_connect(); \
		PushActiveSnapshot(GetTransactionSnapshot()); \
	} while (0);

#define SPI_EPILOG do { \
		PopActiveSnapshot(); \
		SPI_finish(); \
		CommitTransactionCommand(); \
	} while (0);

/* flags set by signal handlers */
extern volatile sig_atomic_t got_sigterm;
extern volatile sig_atomic_t got_sigusr1;

/* GUC variables */
extern bool shardman_master;
extern char *shardman_master_dbname;
extern char *shardman_master_connstring;
extern int shardman_cmd_retry_naptime;
extern int shardman_poll_interval;

extern int32 shardman_my_node_id;
#define SHMN_INVALID_NODE_ID -1

typedef struct Cmd
{
	int64 id;
	char *cmd_type;
	char *status;
	char **opts; /* array of n options, opts[n] is NULL */
} Cmd;

extern void _PG_init(void);
extern void shardmaster_main(Datum main_arg);
extern void check_for_sigterm(void);
extern uint64 void_spi(char *sql);
extern void update_cmd_status(int64 id, const char *new_status);
extern void cmd_canceled(Cmd *cmd);
extern char *get_worker_node_connstr(int node_id);
extern int32 get_primary_owner(const char *part_name);
extern int32 get_reptail_owner(const char *part_name, int32 *owner,
							   int32 *partnum);
extern char *get_partition_relation(const char *part_name);

#endif							/* PG_SHARDMAN_H */

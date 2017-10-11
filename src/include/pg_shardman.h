/* -------------------------------------------------------------------------
 *
 * Primary pg_shardman include file.
 *
 * Copyright (c) 2017, Postgres Professional
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_SHARDMAN_H
#define PG_SHARDMAN_H

#include <signal.h>

#include "miscadmin.h"
#include "storage/lwlock.h"
#include "libpq-fe.h"

/*
 * By convention, we prefix shardman-related messages with [SHMN].
 * Unfortunately, currently it is impossible to have variadic polymorhic
 * functions accepting args with different types in plpgsql, so in sql funcs
 * we just type it manually.
 */
#define shmn_elog(level,fmt,...) elog(level, "[SHMN] " fmt, ## __VA_ARGS__)

/* Commonly used SPI stuff */
#define SPI_XACT_STATUS \
	bool spi_already_in_xact = IsTransactionState()

#define SPI_PROLOG \
	do { \
		if (!spi_already_in_xact) \
			StartTransactionCommand(); \
		SPI_connect(); \
		PushActiveSnapshot(GetTransactionSnapshot()); \
	} while (0)

#define SPI_EPILOG \
	do { \
		PopActiveSnapshot(); \
		SPI_finish(); \
		if (!spi_already_in_xact) \
			CommitTransactionCommand(); \
	} while (0)

/* flags set by signal handlers */
extern volatile sig_atomic_t got_sigterm;
extern volatile sig_atomic_t got_sigusr1;
/*
 * Most probably CHECK_FOR_INTERRTUPS here is useless since we handle
 * SIGTERM ourselves (to get adequate log message) and don't need anything
 * else, but just to be sure...
 */
#define SHMN_CHECK_FOR_INTERRUPTS() \
do { \
	check_for_sigterm(); \
	CHECK_FOR_INTERRUPTS(); \
} while (0)
/*
 * Additionally check for SIGUSR1; if it has arrived, mark cmd as canceled and
 * return from current function. Used to save typing in void funcs where we
 * don't need to do anything before 'return'.
 */
#define SHMN_CHECK_FOR_INTERRUPTS_CMD(cmd) \
do { \
	SHMN_CHECK_FOR_INTERRUPTS(); \
	if (got_sigusr1) \
	{ \
		cmd_canceled((cmd)); \
		return; \
	} \
} while(0)

#define SHMN_INVALID_NODE_ID -1

/* GUC variables */
extern bool shardman_shardlord;
extern char *shardman_shardlord_dbname;
extern char *shardman_shardlord_connstring;
extern int shardman_cmd_retry_naptime;
extern int shardman_poll_interval;
extern int shardman_my_id;
extern bool shardman_sync_replicas;

typedef struct Cmd
{
	int64 id;
	char *cmd_type;
	char *status;
	char **opts; /* array of n options, opts[n] is NULL */
} Cmd;

typedef struct Partition
{
	char *part_name;
	int32 owner;
} Partition;

typedef struct RepCount
{
	char *part_name;
	int64 count;
} RepCount;

typedef enum
{
	SNT_LORD,
	SNT_WORKER
} ShmnNodeType;


extern void _PG_init(void);
extern void _PG_fini(void);
extern void shardlord_main(Datum main_arg);
extern bool signal_pending(void);
extern void check_for_sigterm(void);
extern void cmd_canceled(Cmd *cmd);
#define GET_SUBSTATE_SQL(subname) \
	"select srsubstate, srrelid from pg_subscription_rel srel join" \
	" pg_subscription s on srel.srsubid = s.oid where subname = '" subname "';"
extern char *get_substate_sql(const char *subname);
extern void reset_pqconn(PGconn **conn);
extern void reset_pqconn_and_res(PGconn **conn, PGresult *res);
extern uint64 void_spi(char *sql);
extern void update_cmd_status(int64 id, const char *new_status);
extern char *get_node_connstr(int32 node_id, ShmnNodeType node_type);
extern int32 *get_workers(uint64 *num_workers);
extern int32 get_primary_owner(const char *part_name);
extern int32 get_reptail_owner(const char *part_name);
extern int32 get_next_node(const char *part_name, int32 node_id);
extern int32 get_prev_node(const char *part_name, int32 node_id, bool *part_exists);
extern char *get_partition_relation(const char *part_name);
extern Partition *get_parts(const char *relation, uint64 *num_parts);
extern RepCount *get_repcount(const char *relation, uint64 *num_parts);
extern bool node_has_partition(int32 node, const char *part_name);
#endif							/* PG_SHARDMAN_H */

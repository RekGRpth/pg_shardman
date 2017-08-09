#ifndef COPYPART_H
#define COPYPART_H

#include "libpq-fe.h"
#include "access/xlogdefs.h"

#include "pg_shardman.h"

/* Type of task involving partition copy */
typedef enum
{
	COPYPARTTASK_MOVE_PRIMARY,
	COPYPARTTASK_MOVE_REPLICA,
	COPYPARTTASK_CREATE_REPLICA
} CopyPartTaskType;

/* final result of 1 one task */
typedef enum
{
	TASK_IN_PROGRESS,
	TASK_FAILED,
	TASK_SUCCESS
} TaskRes;

/* result of one iteration of task execution */
typedef enum
{
	TASK_EPOLL, /* add me to epoll on epolled_fd on EPOLLIN */
	TASK_WAKEMEUP, /* wake me up again on waketm */
	TASK_DONE /* the work is done, never invoke me again */
} ExecTaskRes;

/*
 *  Current step of 1 master partition move. See comments to corresponding
 *  funcs, e.g. start_tablesync.
 */
typedef enum
{
	COPYPART_START_TABLESYNC,
	COPYPART_START_FINALSYNC,
	COPYPART_FINALIZE,
	COPYPART_DONE
} CopyPartStep;

/* State of copy part task */
typedef struct
{
	CopyPartTaskType type;
	/* wake me up at waketm to do the job. Try to keep it zero when invalid	*/
	struct timespec waketm;
	/* We need to epoll only on socket with dst to wait for copy */
	 /* exec_copypart sets fd here when it wants to be wakened by epoll */
	int fd_to_epoll;
	int fd_in_epoll_set; /* socket *currently* in epoll set. -1 of none */

	const char *part_name; /* partition name */
	int32 src_node; /* node we are copying partition from */
	int32 dst_node; /* node we are copying partition to */
	const char *src_connstr;
	const char *dst_connstr;
	PGconn *src_conn; /* connection to src */
	PGconn *dst_conn; /* connection to dst */

	/*
	 * The following strs are constant during execution; we allocate them
	 * once in init func and they disappear with cmd mem ctxt
	 */
	char *logname; /* name of publication, repslot and subscription */
	char *dst_drop_sub_sql; /* sql to drop sub on dst node */
	char *src_create_pub_and_rs_sql; /* create publ and repslot on src */
	char *relation; /* name of sharded relation */
	char *dst_create_tab_and_sub_sql; /* create table and sub on dst */
	char *substate_sql; /* get current state of subscription */
	char *readonly_sql; /* make src table read-only */
	char *received_lsn_sql; /* get last received lsn on dst */
	char *update_metadata_sql;

	XLogRecPtr sync_point; /* when dst reached this point, it is synced */
	CopyPartStep curstep; /* current step */
	ExecTaskRes exec_res; /* result of the last iteration */
	TaskRes res; /* result of the whole move */
} CopyPartState;

/* State of create replica task */
typedef struct
{
	CopyPartState cp;
	char *drop_cp_sub_sql;
	char *create_data_pub_sql;
	char *create_data_sub_sql;
} CreateReplicaState;

/* State of move partition task */
typedef struct
{
	CopyPartState cp;
	int32 next_node; /* next replica, if exists */
	const char *next_connstr;
	PGconn *next_conn; /* connection to next replica */
	int32 prev_node; /* previous replica, if exists */
	const char *prev_connstr;
	PGconn *prev_conn; /* connection to previous replica */
	/* SQL executed to reconfigure LR channels */
	char *prev_sql;
	char *dst_sql;
	char *next_sql;
} MovePartState;

extern void init_mp_state(MovePartState *mps, const char *part_name,
						  int32 src_node, int32 dst_node);
extern void init_cr_state(CreateReplicaState *cps, const char *part_name,
						   int32 dst_node);
extern void exec_tasks(CopyPartState **tasks, int ntasks);


#endif							/* COPYPART_H */

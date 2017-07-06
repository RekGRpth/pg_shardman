#ifndef PG_SHARDMAN_H
#define PG_SHARDMAN_H

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

extern void _PG_init(void);
extern void shardmaster_main(Datum main_arg);

#endif							/* PG_SHARDMAN_H */

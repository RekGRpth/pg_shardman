#ifndef SHARD_H
#define SHARD_H

#include "pg_shardman.h"

extern void create_hash_partitions(Cmd *cmd);
extern void move_part(Cmd *cmd);
extern void create_replica(Cmd *cmd);
extern void rebalance(Cmd *cmd);

#endif							/* SHARD_H */

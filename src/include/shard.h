#ifndef SHARD_H
#define SHARD_H

#include "pg_shardman.h"

extern void create_hash_partitions(Cmd *cmd);
extern void move_mpart(Cmd *cmd);

#endif							/* SHARD_H */

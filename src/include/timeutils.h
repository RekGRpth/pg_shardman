/* -------------------------------------------------------------------------
 * Copyright (c) 2017, Postgres Professional
 * -------------------------------------------------------------------------
 */
#ifndef TIMEUTILS_H
#define TIMEUTILS_H

#include <time.h>

extern int timespeccmp(struct timespec t1, struct timespec t2);
extern struct timespec timespec_add_millis(struct timespec t, long millis);
extern int timespec_diff_millis(struct timespec t1, struct timespec t2);

#endif							/* TIMEUTILS_H */

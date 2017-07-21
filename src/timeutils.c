#include "timeutils.h"

#define MILLION 1000000L
#define BILLION 1000000000L

/*
 * Like strcmp, but for timespec. Returns -1 if t1 < t2.
 */
int
timespeccmp(struct timespec t1, struct timespec t2)
{
	if (t1.tv_sec == t2.tv_sec)
	{
		if (t1.tv_nsec < t2.tv_nsec)
			return -1;
		else if (t1.tv_nsec > t2.tv_nsec)
			return 1;
		return 0;
	}
	else if (t1.tv_sec < t2.tv_sec)
		return -1;
	return 1;
}

/*
 * Add milliseconds to timespec
 */
struct timespec
timespec_add_millis(struct timespec t, long millis)
{
	time_t tv_sec;
	long tv_nsec;

	tv_sec = t.tv_sec + millis / 1000;
	tv_nsec = t.tv_nsec + (millis % 1000) * MILLION;
	if (tv_nsec >= BILLION) {
		tv_nsec -= BILLION;
		tv_sec++;
	}

	return (struct timespec) { .tv_sec = tv_sec, .tv_nsec = tv_nsec };
}

/*
 * Get t1 - t2 difference in milliseconds. Not reliable if time_t is unsigned
 */
int
timespec_diff_millis(struct timespec t1, struct timespec t2)
{
	int sec_diff = t1.tv_sec - t2.tv_sec;
	long nsec_diff = t1.tv_nsec - t2.tv_nsec;
	return sec_diff * 1000 + nsec_diff / 1000;
}

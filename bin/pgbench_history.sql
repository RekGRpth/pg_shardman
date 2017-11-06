drop table if exists pgbench_history;
create table pgbench_history (tid int, bid int, aid int, delta int,
							  mtime timestamp, filler char(22));

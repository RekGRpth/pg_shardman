drop table if exists shmn_benchmarks cascade;
create table shmn_benchmarks (
	test_id text, instance_type text, workers int, nparts int, sharded_tables text,
	replicas int, repmode text, sync_replicas bool, sync_commit text, CFLAGS text,
	scale int, seconds int, test text, fdw_2pc bool, active_workers text, clients int,
	tps_sum int, avg_latency numeric, end_latency numeric, wal_lag bigint,
	comment text);
copy shmn_benchmarks from '/home/ars/shmn_benchmarks.csv' with (format csv, header);

select workers, nparts, repmode, sync_replicas, clients, tps_sum, pg_size_pretty(wal_lag) from shmn_benchmarks;

-- only important fields
drop view if exists shmn_bench;
create view shmn_bench as select workers, nparts, sharded_tables, repmode, sync_replicas, test, fdw_2pc, active_workers, clients, tps_sum,
		   pg_size_pretty(wal_lag) wal_lag
	  from shmn_benchmarks;

-- take only runs with number of clients maximizing tps
-- for each set of rows which differ only by number of clients we take from
-- window a row with max tps
drop view if exists shmn_benchmarks_optimal_clients;
create view shmn_benchmarks_optimal_clients as
select distinct on (workers, nparts, sharded_tables, repmode, sync_replicas, test, fdw_2pc, active_workers)
					workers, nparts, sharded_tables, repmode, sync_replicas, test, fdw_2pc, active_workers,
				last_value(clients) over wnd clients,
				last_value(tps_sum) over wnd tps_sum,
				last_value(avg_latency) over wnd avg_latency,
				last_value(end_latency) over wnd end_latency,
				pg_size_pretty(last_value(wal_lag) over wnd) wal_lag
  from shmn_benchmarks
		   window wnd as
		   (partition by workers, nparts, sharded_tables, repmode, sync_replicas, fdw_2pc, test, active_workers order by tps_sum
		    rows between unbounded preceding and unbounded following);

-- Create first() aggregate, taken from
-- https://wiki.postgresql.org/wiki/First/last_(aggregate)
-- Create a function that always returns the first non-NULL item
CREATE OR REPLACE FUNCTION public.first_agg ( anyelement, anyelement )
RETURNS anyelement LANGUAGE SQL IMMUTABLE STRICT AS $$
        SELECT $1;
$$;

-- And then wrap an aggregate around it
	drop aggregate public.first();
	CREATE AGGREGATE public.FIRST (
        sfunc    = public.first_agg,
        basetype = anyelement,
        stype    = anyelement
		);

-- flatten to compare no rep, trigger, sync and async, 2pc only
select workers, nparts, (nparts / workers) nparts_per_node,
	   first(tps_sum) filter (where repmode is null) no_rep_tps,
	   first(tps_sum) filter (where repmode = 'trigger') trig_rep_tps,
	   first(tps_sum) filter (where repmode = 'logical' and sync_replicas) sync_rep_tps,
	   first(tps_sum) filter (where repmode = 'logical' and not sync_replicas) async_rep_tps,
	   first(wal_lag) filter (where repmode = 'logical' and not sync_replicas) async_rep_wal_lag
  from shmn_benchmarks_optimal_clients
 where active_workers = workers::text and fdw_2pc
 group by workers, nparts;

-- showing clients
select workers, nparts, (nparts / workers) nparts_per_node,
	   first(tps_sum) filter (where repmode is null) no_rep_tps,
	   first(clients) filter (where repmode is null) no_rep_tps,
	   first(tps_sum) filter (where repmode = 'trigger') trig_rep_tps,
	   first(tps_sum) filter (where repmode = 'logical' and sync_replicas) sync_rep_tps,
	   first(tps_sum) filter (where repmode = 'logical' and not sync_replicas) async_rep_tps,
	   first(wal_lag) filter (where repmode = 'logical' and not sync_replicas) async_rep_wal_lag
  from shmn_benchmarks_optimal_clients
 where active_workers = workers::text and fdw_2pc
 group by workers, nparts;

-- either with 2pc and not, showing it
select workers, nparts, (nparts / workers) nparts_per_node,
	   first(tps_sum) filter (where repmode is null) no_rep_tps,
	   first(fdw_2pc) filter (where repmode is null) no_rep_2pc,
	   first(tps_sum) filter (where repmode = 'trigger') trig_rep_tps,
	   first(fdw_2pc) filter (where repmode = 'trigger') trig_rep_2pc,
	   first(tps_sum) filter (where repmode = 'logical' and sync_replicas) sync_rep_tps,
	   first(fdw_2pc) filter (where repmode = 'logical' and sync_replicas) sync_rep_2pc,
	   first(tps_sum) filter (where repmode = 'logical' and not sync_replicas) async_rep_tps,
	   first(fdw_2pc) filter (where repmode = 'logical' and not sync_replicas) async_rep_2pc,
	   first(wal_lag) filter (where repmode = 'logical' and not sync_replicas) async_rep_wal_lag
  from shmn_benchmarks_optimal_clients
 where active_workers = workers::text
 group by workers, nparts;


select workers, nparts, sharded_tables, repmode, sync_replicas, clients, tps_sum,
	   wal_lag
  from shmn_benchmarks_optimal_clients where active_workers = workers::text and fdw_2pc;

select workers, nparts, sharded_tables, repmode, sync_replicas, fdw_2pc, test, clients, tps_sum,
	   wal_lag
  from shmn_benchmarks_optimal_clients where active_workers = workers::text and sharded_tables = 'pgbench_accounts';


-- see, here lag increases only where there are too many clients already
select * from shmn_bench where repmode = 'logical' and not sync_replicas and fdw_2pc and (
	(workers = 3 and (nparts = 9 or nparts = 30)) or
	(workers = 6 and (nparts = 6 or nparts = 12 or nparts = 18 or nparts = 60)) or
	(workers = 9 and (nparts = 27 or nparts = 90)) or
	(workers = 12)
	)
 order by workers, nparts, clients;

-- same, only for 6:6 and 6:12
select * from shmn_bench where repmode = 'logical' and not sync_replicas and fdw_2pc and (
	(workers = 6 and (nparts = 6 or nparts = 12))
	)
 order by workers, nparts, clients;


-- 2pc vs non-2pc
select *, (s.no_two_pc_tps::numeric(10, 0) / s.two_pc_tps)::numeric(3, 2) no_two_pc_faster_times from
(select workers, nparts, sharded_tables, repmode, sync_replicas, test, active_workers, clients,
	   first(tps_sum) filter (where fdw_2pc) two_pc_tps,
		first(tps_sum) filter (where not fdw_2pc) no_two_pc_tps
  from shmn_bench
 group by workers, nparts, sharded_tables, repmode, sync_replicas, test, active_workers, clients) s
 where (s.two_pc_tps is not null and s.no_two_pc_tps is not null)
	   order by workers, nparts, repmode, sync_replicas, active_workers, clients;

drop table if exists pgbench_accounts;
create table pgbench_accounts (
	aid int not null,
	bid int,
	abalance int,
	filler char(84)
);

drop table if exists pgbench_tellers;
create table pgbench_tellers (
	tid int not null,
	bid int,
	tbalance int,
	filler char(84)
);


drop table if exists pgbench_branches;
create table pgbench_branches (
	bid int not null,
	bbalance int,
	filler char(88)
);

alter table pgbench_branches add primary key (bid);
alter table pgbench_tellers add primary key (tid);
alter table pgbench_accounts add primary key (aid);

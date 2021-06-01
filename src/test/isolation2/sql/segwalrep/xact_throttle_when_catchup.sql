--
-- Test: transaction commits should be throttled when mirror is in
-- catchup state due to a big lag.
-- Throttle test is difficult to automate, this is trying to represent
-- whether transactions being throttled or not by showing a single
-- transaction blocking/unblocking result. The theory is that:
-- generating a bulk inserts to enlarge the WAL lag between primary
-- and mirror;
-- having mirror stay CATCHUP state with syncrep off by suspending
-- 'wal_sender_after_caughtup_within_range' injection point;
-- creating insert transaction with different record size to verify
-- the position of blocking among transaction statements.
--
-- setup
!\retcode gpconfig -c wait_for_replication_threshold -v 1 --skipvalidation;
!\retcode gpstop -u;

CREATE TABLE xact_throttle_tbl (a int, b int, c text) DISTRIBUTED BY (a);
CHECKPOINT;

1: SELECT gp_inject_fault_infinite('wal_sender_after_caughtup_within_range', 'suspend', dbid)
    FROM gp_segment_configuration WHERE content = 0 AND role = 'p';

-- stop the mirror
1: SELECT pg_ctl((SELECT datadir FROM gp_segment_configuration c WHERE c.role='m' AND c.content=0), 'stop');
1: SELECT gp_request_fts_probe_scan();

-- insert records to the primary, '2' will be distributed to content 0,
1: BEGIN;
1: INSERT INTO xact_throttle_tbl VALUES(2, 0, 'a');
-- stuck at executing COMMIT since syncrep hasn't been turned off yet
1: COMMIT;

-- generate a large WAL lag which will create several WAL segment files
1: INSERT INTO xact_throttle_tbl SELECT 2, generate_series(1, 3000000), 'a';

-- make sure mirror is in catchup state
0U&: SELECT wait_until_standby_in_state('catchup');

-- bring the mirror back up, make sure it is in catchup state
1: SELECT pg_ctl_start(datadir, port) FROM gp_segment_configuration WHERE role = 'm' AND content = 0;

-- show catchup state
0U<:

1: SELECT gp_wait_until_triggered_fault('wal_sender_after_caughtup_within_range', 1, dbid)
    FROM gp_segment_configuration WHERE content=0 AND role='p';

-- make sure syncrep is off
0U: show synchronous_standby_names;

-- now mirror stays at CATCHUP state, and syncrep stays at off

-- a) insert transactions with small WAL bytes written should not be blocked,
-- this also indicates slow and small inserts would not be blocked or throttled
1: BEGIN;
1: INSERT INTO xact_throttle_tbl VALUES(2, 0, 'a');
1: COMMIT;

-- b) insert transactions with large WAL bytes written should be throttled
-- make sure the replication threshold is 1024, then insert a longer record
-- e.g. 1025 like below to generate a "large" WAL record
0U: show wait_for_replication_threshold;

2: BEGIN;
-- stuck before executing COMMIT due to throttle
2&: INSERT INTO xact_throttle_tbl VALUES(2, 2, repeat('a', 1025));

-- c) bulk inserts may enlarge WAL bytes written size, resulting throttle
-- Actually, this case is mutually exclusive with the above one since the system
-- is already pending on writing above WAL record, new queries could not be proceeded
-- any more. Put it here as a comment for creating large WAL record in this way.
-- 3&: INSERT INTO xact_throttle_tbl SELECT 2, generate_series(4000000, 5000000), 'aa';

-- wait for a moment to ensure the insert transaction is still running
1: SELECT pg_sleep(5);
-- check the current query in pg_stat_activity after waiting, the state should be still 'active'
1: SELECT application_name,state,query,backend_type FROM pg_stat_activity WHERE query LIKE 'INSERT INTO xact_throttle_tbl%';

-- reset fault on primary as well as mirror
1: SELECT gp_inject_fault('wal_sender_after_caughtup_within_range', 'reset', dbid)
    FROM gp_segment_configuration WHERE content=0;

-- after this, system continue to proceed

2<:
2: COMMIT;

SELECT wait_until_all_segments_synchronized();

-- make sure syncrep is on
0U: show synchronous_standby_names;

-- cleanup
DROP TABLE xact_throttle_tbl;
!\retcode gpconfig -r wait_for_replication_threshold --skipvalidation;
!\retcode gpstop -u;

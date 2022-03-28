-- This case tests OOM when writting elog messages into server log files.
-- - First, we manually set currently used memory 'segmentVmemChunks' to
-- - 'gp_vmem_protect_limit + waivedChunks' in a QE session, then issue
-- - another session which will also request some memory chunks. However,
-- - since we used out all of the memory chunks, it will throw OOM error, when
-- - writting error messages into server logs, it may also request some
-- - extra memory chunks, but under previouse reserving memory logic,
-- - OOM error will be raised again and again..., until abort or panic.

-- start_matchsubs
--
-- m/DETAIL:  Per-query memory limit reached: current limit is \d+ kB, requested \d+ bytes, has \d+ MB available for this query/
-- s/\d+/XXX/g
--
-- m/DETAIL:  Vmem limit reached, failed to allocate \d+ bytes from tracker, which has \d+ MB available/
-- s/\d+/XXX/g
--
-- m/DETAIL:  System memory limit reached, failed to allocate \d+ bytes from system/
-- s/\d+/XXX/g
--
-- m/(seg\d+ \d+.\d+.\d+.\d+:\d+)/
-- s/(.*)/(seg<ID> IP:PORT)/
--
-- end_matchsubs

-- Set the gp_log_format to text format, which will call palloc when writting
-- error log message to server log files.
-- start_ignore
!\retcode gpconfig -c gp_log_format -v text;
!\retcode gpstop -ari;
-- end_ignore

1: create table test_vmem_tbl(c1 int, c2 int, c3 int, c4 int, c5 int, c6 int, c7 int, c8 int, c9 int, c10 int);
2: select gp_inject_fault_infinite('vmem_oom_set_startup_chunks', 'skip', dbid) from gp_segment_configuration where content = 0 and role = 'p';
2: select gp_inject_fault_infinite('vmem_oom_startup', 'suspend', dbid) from gp_segment_configuration where content = 0 and role = 'p';
2&: select gp_wait_until_triggered_fault('vmem_oom_startup', 1, dbid) from gp_segment_configuration where content =0 and role = 'p';
-- The QE session will used out all of the segment's vitual memory, including waivedChunks 
3&: set gp_vmem_idle_resource_timeout to '1h'; 
2<:
-- Issue a query with very long sql plain text, so that it will call palloc to request memory chunk when
-- writting the statement's text to the server log files
2: select gp_inject_fault_infinite('vmem_oom_set_tracked_bytes', 'skip', dbid) from gp_segment_configuration where content = 0 and role = 'p';
1: select temp_t3.c1, temp_t3.c2, temp_t3.c3, temp_t3.c4, temp_t3.c5, temp_t3.c6, temp_t3.c7, temp_t3.c8, temp_t3.c9,
   temp_t3.c10, temp_t3.c1+temp_t3.c2, temp_t3.c2+temp_t3.c3, temp_t3.c3+temp_t3.c4, temp_t3.c4+temp_t3.c5,
   temp_t3.c6+temp_t3.c7, temp_t3.c7+temp_t3.c8, temp_t3.c9+temp_t3.c10
   from
   (select temp_t2.c1, temp_t2.c2, temp_t2.c3, temp_t2.c4, temp_t2.c5, temp_t2.c6, temp_t2.c7, temp_t2.c8, temp_t2.c9,
    temp_t2.c10, temp_t2.c1+temp_t2.c2, temp_t2.c2+temp_t2.c3, temp_t2.c3+temp_t2.c4, temp_t2.c4+temp_t2.c5,
    temp_t2.c6+temp_t2.c7, temp_t2.c7+temp_t2.c8, temp_t2.c9+temp_t2.c10
    from
    (select temp_t1.c1, temp_t1.c2, temp_t1.c3, temp_t1.c4, temp_t1.c5, temp_t1.c6, temp_t1.c7, temp_t1.c8, temp_t1.c9,
     temp_t1.c10, temp_t1.c1+temp_t1.c2, temp_t1.c2+temp_t1.c3, temp_t1.c3+temp_t1.c4, temp_t1.c4+temp_t1.c5,
     temp_t1.c6+temp_t1.c7, temp_t1.c7+temp_t1.c8, temp_t1.c9+temp_t1.c10
     from
     (select c1, c2, c3, c4, c5, c6, c7, c8, c9, c10
      from
      test_vmem_tbl limit 1000000000)
     temp_t1 limit 1000000000)
    temp_t2 limit 100000)
   temp_t3 limit 10000;
2: select gp_inject_fault_infinite('vmem_oom_set_startup_chunks', 'reset', dbid) from gp_segment_configuration where content = 0 and role = 'p';
2: select gp_inject_fault_infinite('vmem_oom_set_tracked_bytes', 'reset', dbid) from gp_segment_configuration where content = 0 and role = 'p';
2: select gp_inject_fault_infinite('vmem_oom_startup', 'reset', dbid) from gp_segment_configuration where content = 0 and role = 'p';
3<:
1q:
2q:
3q:
1: drop table test_vmem_tbl;
-- start_ignore
!\retcode gpconfig -c gp_log_format -v csv;
!\retcode gpstop -ari;
-- end_ignore


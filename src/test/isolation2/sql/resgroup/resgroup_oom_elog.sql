-- This case tests resgroup OOM when writting elog messages into server log files.
-- - First, we manually set currently free chunks of a group 'pResGroupControl->freeChunks'
-- - to '0 - waivedChunks' in a QE session, then set the 'slot->memUsage' to 'slot->memQuota.
-- - So when the QE session goes further, it may request chunks from freeChunks, since we
-- - used out all of the free chunks of the group, it will throw OOM error, when writting
-- - error messages into server logs, it may also request some extra memory chunks, but
-- - under previouse reserving memory logic, OOM error will be raised again and again...,
-- - until abort or panic.

-- Set the gp_log_format to text format, which will call palloc when writting
-- error log message to server log files.
-- start_ignore
!\retcode gpconfig -c gp_log_format -v text;
!\retcode gpstop -ari;
-- end_ignore

CREATE RESOURCE GROUP rg_test_mem WITH (concurrency=2, cpu_rate_limit=10, memory_limit=10);
CREATE ROLE role_test_mem RESOURCE GROUP rg_test_mem;

1: SET ROLE TO role_test_mem;
1: create table test_vmem_tbl(c1 int, c2 int, c3 int, c4 int, c5 int, c6 int, c7 int, c8 int, c9 int, c10 int);
1: select gp_inject_fault_infinite('vmem_oom_set_tracked_bytes', 'skip', dbid) from gp_segment_configuration where content = 0 and role = 'p';
1: select gp_inject_fault_infinite('resgroup_set_mem_chunks', 'skip', dbid) from gp_segment_configuration where content = 0 and role = 'p';
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
1: select gp_inject_fault_infinite('vmem_oom_set_tracked_bytes', 'reset', dbid) from gp_segment_configuration where content = 0 and role = 'p';
1: select gp_inject_fault_infinite('resgroup_set_mem_chunks', 'reset', dbid) from gp_segment_configuration where content = 0 and role = 'p';
1q:

-- start_ignore
!\retcode gpconfig -c gp_log_format -v csv;
!\retcode gpstop -ari;
-- end_ignore

1: drop table test_vmem_tbl;
1: drop role role_test_mem;
1: drop resource group rg_test_mem;


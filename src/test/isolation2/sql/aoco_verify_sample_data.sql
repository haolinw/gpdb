--
-- Verify whether the expected segfile data is read
-- into the buffer or not during scanning sample rows.
--

drop table if exists verify_sample_data_aoco;
create table verify_sample_data_aoco (c text) using ao_column distributed replicated;

1: begin;
2: begin;

1: insert into verify_sample_data_aoco values ('test1-1'), ('test1-2');
2: insert into verify_sample_data_aoco values ('test2-1'), ('test2-2');

1: commit;
2: commit;

select * from verify_sample_data_aoco;
select * from gp_toolkit.__gp_aocsseg('verify_sample_data_aoco');

create or replace function verify_sample_data() returns boolean as $$
declare
     cnt int; /* in func */
begin
     cnt := 0; /* in func */
     while (cnt < 3)
     loop
          select count(*) into cnt from pg_catalog.gp_acquire_sample_rows('verify_sample_data_aoco'::regclass, 3, 'f') as
            (totalrows pg_catalog.float8, totaldeadrows pg_catalog.float8, oversized_cols_length pg_catalog._float8, c text)
            where c = 'test1-1' or c = 'test2-1' or c = 'test2-2' and c != 'test1-2'; /* in func */
     end loop; /* in func */
     return true; /* in func */
end; /* in func */
$$ language plpgsql;

-- inject fault to simulate the expected segfile data isn't read into memory
select gp_inject_fault_infinite('datumstreamread_closed_segfile', 'skip', dbid)
  from gp_segment_configuration where content != -1 AND role = 'p';

analyze verify_sample_data_aoco;

select gp_wait_until_triggered_fault('datumstreamread_closed_segfile', 1, dbid)
  from gp_segment_configuration where content != -1 AND role = 'p';

-- expect verification failed as actual_segno != expect_segno
select verify_sample_data();

select gp_inject_fault('datumstreamread_closed_segfile', 'reset', dbid)
  from gp_segment_configuration where content != -1 AND role = 'p';

-- expect verification successful after fix
select verify_sample_data();

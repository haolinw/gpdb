-- White-box tests asserting composition of AO/CO block directory entries.
-- All tuples are directed to seg0 and each INSERT has an increasing row count
-- to make their identification easy.

-- helper function to assert `tupcount` in pg_ao(cs)seg == sum of number
-- of `row_count` across all aoblkdir entries for each <segno, columngroup_no>
CREATE OR REPLACE FUNCTION test_aoblkdir_rowcount(tabname TEXT)
   RETURNS TABLE (segno INTEGER, columngroup_no INTEGER, rowcount_sum_equalto_seg_tupcount BOOLEAN) AS $$
DECLARE
  row RECORD; /* in func */
  result BOOLEAN; /* in func */
BEGIN
  FOR row IN SELECT aoblkdir.segno, aoblkdir.columngroup_no, SUM(aoblkdir.row_count) as row_count_sum
             FROM gp_toolkit.__gp_aoblkdir(tabname) aoblkdir
             WHERE aoblkdir.segno IN (SELECT ao_or_aocs_seg.segno FROM gp_ao_or_aocs_seg(tabname) ao_or_aocs_seg)
             GROUP BY aoblkdir.segno, aoblkdir.columngroup_no ORDER BY aoblkdir.segno, aoblkdir.columngroup_no ASC
  LOOP
    SELECT (row.row_count_sum = ao_or_aocs_seg.tupcount) INTO result
    FROM gp_ao_or_aocs_seg(tabname) ao_or_aocs_seg WHERE ao_or_aocs_seg.segno = row.segno; /* in func */
    segno := row.segno; /* in func */
    columngroup_no := row.columngroup_no; /* in func */
    rowcount_sum_equalto_seg_tupcount := result; /* in func */
    RETURN NEXT; /* in func */
  END LOOP; /* in func */
END; /* in func */
$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------
-- AO tables
--------------------------------------------------------------------------------

CREATE TABLE ao_blkdir_test(i int, j int) USING ao_row DISTRIBUTED BY (j);
CREATE INDEX ao_blkdir_test_idx ON ao_blkdir_test(i);

1: INSERT INTO ao_blkdir_test SELECT i, 2 FROM generate_series(1, 10) i;
-- There should be 1 block directory row with a single entry covering 10 rows
SELECT (gp_toolkit.__gp_aoblkdir('ao_blkdir_test')).* FROM gp_dist_random('gp_id')
    WHERE gp_segment_id = 0 ORDER BY 1,2,3,4,5;

1: INSERT INTO ao_blkdir_test SELECT i, 2 FROM generate_series(11, 30) i;
-- There should be 2 block directory entries in a new block directory row, and
-- the row from the previous INSERT should not be visible. The entry from the
-- first INSERT should remain unchanged.
SELECT (gp_toolkit.__gp_aoblkdir('ao_blkdir_test')).* FROM gp_dist_random('gp_id')
WHERE gp_segment_id = 0 ORDER BY 1,2,3,4,5;

1: BEGIN;
1: INSERT INTO ao_blkdir_test SELECT i, 2 FROM generate_series(31, 60) i;
2: BEGIN;
2: INSERT INTO ao_blkdir_test SELECT i, 2 FROM generate_series(71, 110) i;
1: COMMIT;
2: COMMIT;
-- The second INSERT of 40 rows above would have landed in segfile 1 (unlike
-- segfile 0, like the first INSERT of 30 rows above). This should be reflected
-- in the block directory entries for these rows.
SELECT (gp_toolkit.__gp_aoblkdir('ao_blkdir_test')).* FROM gp_dist_random('gp_id')
WHERE gp_segment_id = 0 ORDER BY 1,2,3,4,5;

TRUNCATE ao_blkdir_test;
-- Insert enough rows to overflow the first block directory minipage by 2.
INSERT INTO ao_blkdir_test SELECT i, 2 FROM generate_series(1, 292700) i;
-- There should be 2 block directory rows, one with 161 entries covering 292698
-- rows and the other with 1 entry covering the 2 overflow rows.
SELECT (gp_toolkit.__gp_aoblkdir('ao_blkdir_test')).* FROM gp_dist_random('gp_id')
WHERE gp_segment_id = 0 ORDER BY 1,2,3,4,5;

-- Unique index white box tests
DROP TABLE ao_blkdir_test;
CREATE TABLE ao_blkdir_test(i int UNIQUE, j int) USING ao_row DISTRIBUTED BY (i);

SELECT gp_inject_fault('appendonly_insert', 'suspend', '', '', 'ao_blkdir_test', 1, 1, 0, dbid)
FROM gp_segment_configuration WHERE role = 'p' AND content = 0;
1: BEGIN;
1&: INSERT INTO ao_blkdir_test VALUES (2, 2);

-- There should be a placeholder row inserted to cover the rows for each INSERT
-- session, before we insert the 1st row in that session, that is only visible
-- to SNAPSHOT_DIRTY.
SELECT gp_wait_until_triggered_fault('appendonly_insert', 1, dbid)
FROM gp_segment_configuration WHERE role = 'p' AND content = 0;
SELECT (gp_toolkit.__gp_aoblkdir('ao_blkdir_test')).* FROM gp_dist_random('gp_id')
WHERE gp_segment_id = 0 ORDER BY 1,2,3,4,5;
SET gp_select_invisible TO ON;
SELECT (gp_toolkit.__gp_aoblkdir('ao_blkdir_test')).* FROM gp_dist_random('gp_id')
WHERE gp_segment_id = 0 ORDER BY 1,2,3,4,5;
RESET gp_select_invisible;

-- The placeholder row is invisible to other transactions (that don't perform a
-- uniqueness check) while the INSERT is in progress.
2: SELECT (gp_toolkit.__gp_aoblkdir('ao_blkdir_test')).* FROM gp_dist_random('gp_id')
   WHERE gp_segment_id = 0 ORDER BY 1,2,3,4,5;

SELECT gp_inject_fault('appendonly_insert', 'reset', dbid)
FROM gp_segment_configuration WHERE role = 'p' AND content = 0;
1<:

-- The placeholder row is invisible to the INSERTing transaction. Since the
-- INSERT finished, there should be 1 visible blkdir row representing the INSERT.
1: SELECT (gp_toolkit.__gp_aoblkdir('ao_blkdir_test')).* FROM gp_dist_random('gp_id')
WHERE gp_segment_id = 0 ORDER BY 1,2,3,4,5;

-- The placeholder row is invisible to other transactions (that don't perform a
-- uniqueness check) even after the INSERT finishes. The blkdir row representing
-- the INSERT should not be visible as the INSERTing transaction hasn't
-- committed yet.
2: SELECT (gp_toolkit.__gp_aoblkdir('ao_blkdir_test')).* FROM gp_dist_random('gp_id')
WHERE gp_segment_id = 0 ORDER BY 1,2,3,4,5;

1: COMMIT;

-- The placeholder row is invisible to other transactions (that don't perform a
-- uniqueness check) even after the INSERTing transaction commits. Since the
-- INSERTing transaction has committed, the blkdir row representing the INSERT
-- should be visible now.
2: SELECT (gp_toolkit.__gp_aoblkdir('ao_blkdir_test')).* FROM gp_dist_random('gp_id')
WHERE gp_segment_id = 0 ORDER BY 1,2,3,4,5;

DROP TABLE ao_blkdir_test;

-- Test `tupcount` in pg_ao(cs)seg == sum of number of `row_count` across all
-- aoblkdir entries for each <segno, columngroup_no>.
create table ao_blkdir_test_rowcount (id int, a int, b inet, c inet) using ao_row with (compresstype=zlib, compresslevel=3);
create index on ao_blkdir_test_rowcount(a);

insert into ao_blkdir_test_rowcount select 2, i, (select ((i%255)::text || '.' || (i%255)::text || '.' || (i%255)::text || '.' ||
  (i%255)::text))::inet, (select ((i%255)::text || '.' || (i%255)::text || '.' || (i%255)::text || '.' ||
  (i%255)::text))::inet from generate_series(1,1000)i;

insert into ao_blkdir_test_rowcount select * from ao_blkdir_test_rowcount limit 1000;
insert into ao_blkdir_test_rowcount select * from ao_blkdir_test_rowcount limit 1000;

-- concurrent inserts to generate multiple segfiles
1: begin;
1: insert into ao_blkdir_test_rowcount select * from ao_blkdir_test_rowcount limit 1000;

2: begin;
2: insert into ao_blkdir_test_rowcount select * from ao_blkdir_test_rowcount limit 1000;

3: begin;
3: insert into ao_blkdir_test_rowcount select * from ao_blkdir_test_rowcount limit 1000;

4: insert into ao_blkdir_test_rowcount select * from ao_blkdir_test_rowcount limit 1000;

1: commit;
2: commit;
3: abort;

vacuum analyze ao_blkdir_test_rowcount;

select (test_aoblkdir_rowcount('ao_blkdir_test_rowcount')).* from gp_dist_random('gp_id')
where gp_segment_id = 0;

select segno,tupcount from gp_toolkit.__gp_aoseg('ao_blkdir_test_rowcount')
where segment_id = 0;

select (gp_toolkit.__gp_aoblkdir('ao_blkdir_test_rowcount')).segno,
       (gp_toolkit.__gp_aoblkdir('ao_blkdir_test_rowcount')).columngroup_no,
       (gp_toolkit.__gp_aoblkdir('ao_blkdir_test_rowcount')).row_count from gp_dist_random('gp_id')
where gp_segment_id = 0;

update ao_blkdir_test_rowcount set a = a + 1;
vacuum analyze ao_blkdir_test_rowcount;

select (test_aoblkdir_rowcount('ao_blkdir_test_rowcount')).* from gp_dist_random('gp_id')
where gp_segment_id = 0;

select segno,tupcount from gp_toolkit.__gp_aoseg('ao_blkdir_test_rowcount')
where segment_id = 0;

select (gp_toolkit.__gp_aoblkdir('ao_blkdir_test_rowcount')).segno,
       (gp_toolkit.__gp_aoblkdir('ao_blkdir_test_rowcount')).columngroup_no,
       (gp_toolkit.__gp_aoblkdir('ao_blkdir_test_rowcount')).row_count from gp_dist_random('gp_id')
where gp_segment_id = 0;

drop table ao_blkdir_test_rowcount;

--------------------------------------------------------------------------------
-- AOCO tables
--------------------------------------------------------------------------------

CREATE TABLE aoco_blkdir_test(i int, j int) USING ao_column DISTRIBUTED BY (j);
CREATE INDEX aoco_blkdir_test_idx ON aoco_blkdir_test(i);

1: INSERT INTO aoco_blkdir_test SELECT i, 2 FROM generate_series(1, 10) i;
-- There should be 2 block directory rows with a single entry covering 10 rows,
-- (1 for each column).
SELECT (gp_toolkit.__gp_aoblkdir('aoco_blkdir_test')).* FROM gp_dist_random('gp_id')
WHERE gp_segment_id = 0 ORDER BY 1,2,3,4,5;

1: INSERT INTO aoco_blkdir_test SELECT i, 2 FROM generate_series(11, 30) i;
-- There should be 2 block directory rows, carrying 2 entries each. The rows
-- from the previous INSERT should not be visible. The entries from the first
-- INSERT should remain unchanged.
SELECT (gp_toolkit.__gp_aoblkdir('aoco_blkdir_test')).* FROM gp_dist_random('gp_id')
WHERE gp_segment_id = 0 ORDER BY 1,2,3,4,5;

1: BEGIN;
1: INSERT INTO aoco_blkdir_test SELECT i, 2 FROM generate_series(31, 60) i;
2: BEGIN;
2: INSERT INTO aoco_blkdir_test SELECT i, 2 FROM generate_series(71, 110) i;
1: COMMIT;
2: COMMIT;
-- The second INSERT of 40 rows above would have landed in segfile 1 (unlike
-- segfile 0, like the first INSERT of 30 rows above). This should be reflected
-- in the block directory entries for these rows.
SELECT (gp_toolkit.__gp_aoblkdir('aoco_blkdir_test')).* FROM gp_dist_random('gp_id')
WHERE gp_segment_id = 0 ORDER BY 1,2,3,4,5;

TRUNCATE aoco_blkdir_test;
-- Insert enough rows to overflow the first block directory minipage by 2.
INSERT INTO aoco_blkdir_test SELECT i, 2 FROM generate_series(1, 1317143) i;
-- There should be 2 block directory rows, 2 for each column, one with 161
-- entries covering 1317141 rows and the other with 1 entry covering the 2
-- overflow rows.
SELECT (gp_toolkit.__gp_aoblkdir('aoco_blkdir_test')).* FROM gp_dist_random('gp_id')
WHERE gp_segment_id = 0 ORDER BY 1,2,3,4,5;

-- Unique index white box tests
DROP TABLE aoco_blkdir_test;
CREATE TABLE aoco_blkdir_test(h int, i int UNIQUE, j int) USING ao_column DISTRIBUTED BY (i);

SELECT gp_inject_fault('appendonly_insert', 'suspend', '', '', 'aoco_blkdir_test', 1, 1, 0, dbid)
FROM gp_segment_configuration WHERE role = 'p' AND content = 0;
1: BEGIN;
1&: INSERT INTO aoco_blkdir_test VALUES (2, 2, 2);

-- There should be a placeholder row inserted to cover the rows for each INSERT
-- session (for the first non-dropped column), before we insert the 1st row in
-- that session, that is only visible to SNAPSHOT_DIRTY.
SELECT gp_wait_until_triggered_fault('appendonly_insert', 1, dbid)
FROM gp_segment_configuration WHERE role = 'p' AND content = 0;
SELECT (gp_toolkit.__gp_aoblkdir('aoco_blkdir_test')).* FROM gp_dist_random('gp_id')
WHERE gp_segment_id = 0 ORDER BY 1,2,3,4,5;
SET gp_select_invisible TO ON;
SELECT (gp_toolkit.__gp_aoblkdir('aoco_blkdir_test')).* FROM gp_dist_random('gp_id')
WHERE gp_segment_id = 0 ORDER BY 1,2,3,4,5;
RESET gp_select_invisible;

-- The placeholder row is invisible to other transactions (that don't perform a
-- uniqueness check) while the INSERT is in progress.
2: SELECT (gp_toolkit.__gp_aoblkdir('aoco_blkdir_test')).* FROM gp_dist_random('gp_id')
   WHERE gp_segment_id = 0 ORDER BY 1,2,3,4,5;

-- Before the INSERT commits, if we try to drop column 'h', for which the
-- placeholder row was created, the session will block (locking). So it is
-- perfectly safe to use 1 placeholder row (and not have 1 placeholder/column)
3&: ALTER TABLE aoco_blkdir_test DROP COLUMN h;

SELECT gp_inject_fault('appendonly_insert', 'reset', dbid)
FROM gp_segment_configuration WHERE role = 'p' AND content = 0;
1<:

-- The placeholder row is invisible to the INSERTing transaction. Since the
-- INSERT finished, there should be 3 visible blkdir rows representing the
-- INSERT, 1 for each column.
1: SELECT (gp_toolkit.__gp_aoblkdir('aoco_blkdir_test')).* FROM gp_dist_random('gp_id')
   WHERE gp_segment_id = 0 ORDER BY 1,2,3,4,5;

-- The placeholder row is invisible to other transactions (that don't perform a
-- uniqueness check) even after the INSERT finishes. The blkdir rows representing
-- the INSERT should not be visible as the INSERTing transaction hasn't
-- committed yet.
2: SELECT (gp_toolkit.__gp_aoblkdir('aoco_blkdir_test')).* FROM gp_dist_random('gp_id')
   WHERE gp_segment_id = 0 ORDER BY 1,2,3,4,5;

1: COMMIT;

-- The placeholder row is invisible to other transactions (that don't perform a
-- uniqueness check) even after the INSERTing transaction commits. Since the
-- INSERTing transaction has committed, the blkdir rows representing the INSERT
-- should be visible now.
2: SELECT (gp_toolkit.__gp_aoblkdir('aoco_blkdir_test')).* FROM gp_dist_random('gp_id')
   WHERE gp_segment_id = 0 ORDER BY 1,2,3,4,5;

-- Now even though the DROP COLUMN has finished, we would still be able to
-- properly resolve uniqueness checks (by consulting the first non-dropped
-- column's block directory row).
3<:
4: INSERT INTO aoco_blkdir_test VALUES (2, 2);

DROP TABLE aoco_blkdir_test;

-- Test `tupcount` in pg_ao(cs)seg == sum of number of `row_count` across all
-- aoblkdir entries for each <segno, columngroup_no>.
create table ao_blkdir_test_rowcount (id int, a int, b inet, c inet) using ao_column with (compresstype=zlib, compresslevel=3);
create index on ao_blkdir_test_rowcount(a);

insert into ao_blkdir_test_rowcount select 2, i, (select ((i%255)::text || '.' || (i%255)::text || '.' || (i%255)::text || '.' ||
  (i%255)::text))::inet, (select ((i%255)::text || '.' || (i%255)::text || '.' || (i%255)::text || '.' ||
  (i%255)::text))::inet from generate_series(1,1000)i;

insert into ao_blkdir_test_rowcount select * from ao_blkdir_test_rowcount limit 1000;
insert into ao_blkdir_test_rowcount select * from ao_blkdir_test_rowcount limit 1000;

-- concurrent inserts to generate multiple segfiles
1: begin;
1: insert into ao_blkdir_test_rowcount select * from ao_blkdir_test_rowcount limit 1000;

2: begin;
2: insert into ao_blkdir_test_rowcount select * from ao_blkdir_test_rowcount limit 1000;

3: begin;
3: insert into ao_blkdir_test_rowcount select * from ao_blkdir_test_rowcount limit 1000;

4: insert into ao_blkdir_test_rowcount select * from ao_blkdir_test_rowcount limit 1000;

1: commit;
2: commit;
3: abort;

vacuum analyze ao_blkdir_test_rowcount;

select (test_aoblkdir_rowcount('ao_blkdir_test_rowcount')).* from gp_dist_random('gp_id')
where gp_segment_id = 0;

select segno,column_num,physical_segno,tupcount from gp_toolkit.__gp_aocsseg('ao_blkdir_test_rowcount')
where segment_id = 0;

select (gp_toolkit.__gp_aoblkdir('ao_blkdir_test_rowcount')).segno,
       (gp_toolkit.__gp_aoblkdir('ao_blkdir_test_rowcount')).columngroup_no,
       (gp_toolkit.__gp_aoblkdir('ao_blkdir_test_rowcount')).row_count from gp_dist_random('gp_id')
where gp_segment_id = 0;

update ao_blkdir_test_rowcount set a = a + 1;
vacuum analyze ao_blkdir_test_rowcount;

select (test_aoblkdir_rowcount('ao_blkdir_test_rowcount')).* from gp_dist_random('gp_id')
where gp_segment_id = 0;

select segno,column_num,physical_segno,tupcount from gp_toolkit.__gp_aocsseg('ao_blkdir_test_rowcount')
where segment_id = 0;

select (gp_toolkit.__gp_aoblkdir('ao_blkdir_test_rowcount')).segno,
       (gp_toolkit.__gp_aoblkdir('ao_blkdir_test_rowcount')).columngroup_no,
       (gp_toolkit.__gp_aoblkdir('ao_blkdir_test_rowcount')).row_count from gp_dist_random('gp_id')
where gp_segment_id = 0;

drop table ao_blkdir_test_rowcount;

drop function test_aoblkdir_rowcount;

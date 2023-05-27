CREATE EXTENSION pageinspect;

--------------------------------------------------------------------------------
----                            ao_row tables
--------------------------------------------------------------------------------

CREATE TABLE ao_partial_scan1(i int, j int) USING ao_row;

--------------------------------------------------------------------------------
-- Scenario 1: Starting block number of scans map to block directory entries,
-- across multiple minipages, corresponding to multiple segfiles.
--------------------------------------------------------------------------------

-- Create a couple of seg files, spanning a couple of minipages each.
1: BEGIN;
2: BEGIN;
1: INSERT INTO ao_partial_scan1 SELECT 1,j FROM generate_series(1, 300000) j;
2: INSERT INTO ao_partial_scan1 SELECT 20,j FROM generate_series(1, 300000) j;
1: COMMIT;
2: COMMIT;

-- Doing an index build will result in scanning the relation whole.
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'skip', '', '', '', 1, -1, 0, dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
CREATE INDEX ON ao_partial_scan1 USING brin(i) WITH (pages_per_range = 3);
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'status', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'reset', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';

-- Show the composition of the single data page in the BRIN index.
1U: SELECT * FROM brin_page_items(get_raw_page('ao_partial_scan1_i_idx', 2), 'ao_partial_scan1_i_idx');

-- Now desummarize a few ranges.
1U: SELECT brin_desummarize_range('ao_partial_scan1_i_idx', 33554432);
1U: SELECT brin_desummarize_range('ao_partial_scan1_i_idx', 33554438);
1U: SELECT brin_desummarize_range('ao_partial_scan1_i_idx', 33554441);
1U: SELECT brin_desummarize_range('ao_partial_scan1_i_idx', 67108867);

-- Now summarize these desummarized ranges piecemeal and check that we scan only
-- a subset of the blocks each time.
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'skip', '', '', '', 1, -1, 0, dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT brin_summarize_range('ao_partial_scan1_i_idx', 33554432);
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'status', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'reset', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';

SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'skip', '', '', '', 1, -1, 0, dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT brin_summarize_range('ao_partial_scan1_i_idx', 33554438);
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'status', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'reset', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';

SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'skip', '', '', '', 1, -1, 0, dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT brin_summarize_range('ao_partial_scan1_i_idx', 33554441);
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'status', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'reset', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';

SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'skip', '', '', '', 1, -1, 0, dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT brin_summarize_range('ao_partial_scan1_i_idx', 67108867);
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'status', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'reset', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';

-- Sanity: the summary info is reflected in the data page.
1U: SELECT * FROM brin_page_items(get_raw_page('ao_partial_scan1_i_idx', 2), 'ao_partial_scan1_i_idx');

--------------------------------------------------------------------------------
-- Scenario 2: Starting block number of scan maps to hole at the end of the
-- minipage (after the last entry).
--------------------------------------------------------------------------------
CREATE TABLE ao_partial_scan2(i int, j int) USING ao_row;
-- Fill 1 logical heap block with committed rows.
INSERT INTO ao_partial_scan2 SELECT 1, j FROM generate_series(1, 32767) j;
-- Now add some aborted rows at the end of the segfile, resulting in a hole at
-- the end of the minipage.
BEGIN;
INSERT INTO ao_partial_scan2 SELECT 20, j FROM generate_series(1, 32767) j;
ABORT;

-- Doing an index build will result in scanning the committed rows only.
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'skip', '', '', '', 1, -1, 0, dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
CREATE INDEX ON ao_partial_scan2 USING brin(i) WITH (pages_per_range = 1);
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'status', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'reset', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';

-- Show the composition of the single data page in the BRIN index.
1U: SELECT * FROM brin_page_items(get_raw_page('ao_partial_scan2_i_idx', 2), 'ao_partial_scan2_i_idx');

-- Now desummarize the first range of committed rows.
1U: SELECT brin_desummarize_range('ao_partial_scan2_i_idx', 33554432);

-- Summarizing the first range now should only scan the committed rows. (same
-- as the index build)
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'skip', '', '', '', 1, -1, 0, dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT brin_summarize_range('ao_partial_scan2_i_idx', 33554432);
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'status', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'reset', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';

-- Since we fell into a hole (at the end of the segfile), we skip ahead to the
-- offset of the last block directory entry before the hole. We end up scanning
-- just the 1 block as a result.
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'skip', '', '', '', 1, -1, 0, dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT brin_summarize_range('ao_partial_scan2_i_idx', 33554433);
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'status', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'reset', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';

-- Sanity: the summary info is reflected in the data page.
1U: SELECT * FROM brin_page_items(get_raw_page('ao_partial_scan2_i_idx', 2), 'ao_partial_scan2_i_idx');

--------------------------------------------------------------------------------
-- Scenario 3: Starting block number of scan maps to hole at the start of the
-- segfile (and before the first entry of the first minipage).
--------------------------------------------------------------------------------
CREATE TABLE ao_partial_scan3(i int, j int) USING ao_row;
-- Now add some aborted rows at the end of the segfile, resulting in a hole at
-- the end of the minipage.
BEGIN;
INSERT INTO ao_partial_scan3 SELECT 1, j FROM generate_series(1, 32767) j;
ABORT;
-- Fill 3 logical heap blocks with committed rows.
INSERT INTO ao_partial_scan3 SELECT 20, j FROM generate_series(1, 32767 * 3) j;

-- Doing an index build will result in scanning the committed rows only.
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'skip', '', '', '', 1, -1, 0, dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
CREATE INDEX ON ao_partial_scan3 USING brin(i) WITH (pages_per_range = 3);
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'status', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'reset', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';

-- Show the composition of the single data page in the BRIN index.
1U: SELECT * FROM brin_page_items(get_raw_page('ao_partial_scan3_i_idx', 2), 'ao_partial_scan3_i_idx');

-- Now desummarize the range with 1 block of aborted rows and 2 blocks of
-- committed rows.
1U: SELECT brin_desummarize_range('ao_partial_scan3_i_idx', 33554432);

-- Summarizing this range should scan blocks corresponding to the 2 final logical
-- heap blocks in the range only.
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'skip', '', '', '', 1, -1, 0, dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT gp_inject_fault('AppendOnlyBlockDirectory_PartialScan_hole_at_start', 'skip', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT brin_summarize_range('ao_partial_scan3_i_idx', 33554432);
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'status', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT gp_wait_until_triggered_fault('AppendOnlyBlockDirectory_PartialScan_hole_at_start', 1, dbid)
  FROM gp_segment_configuration where role='p' and content = 1;
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'reset', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT gp_inject_fault('AppendOnlyBlockDirectory_PartialScan_hole_at_start', 'reset', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';

-- Sanity: the summary info is reflected in the data page.
1U: SELECT * FROM brin_page_items(get_raw_page('ao_partial_scan3_i_idx', 2), 'ao_partial_scan3_i_idx');

--------------------------------------------------------------------------------
-- Scenario 4: Starting block number of scan maps to hole between two entries
-- in a minipage.
--------------------------------------------------------------------------------

CREATE TABLE ao_partial_scan4(i int, j int) USING ao_row;

-- Using populate_pages creates holes due to the nature of its piecemeal inserts
-- of 1 row. So, each blkdir entry will have 1 row each. Populate 3 blocks this
-- way, 2 blocks full and 1 block with 1 tuple.
SELECT populate_pages('ao_partial_scan4', 1, tid '(33554434, 0)');

-- Doing an index build will result in scanning the whole relation.
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'skip', '', '', '', 1, -1, 0, dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
CREATE INDEX ON ao_partial_scan4 USING brin(i) WITH (pages_per_range = 1);
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'status', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'reset', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';

-- Show the composition of the single data page in the BRIN index.
1U: SELECT * FROM brin_page_items(get_raw_page('ao_partial_scan4_i_idx', 2), 'ao_partial_scan4_i_idx');

-- Now desummarize a range.
1U: SELECT brin_desummarize_range('ao_partial_scan4_i_idx', 33554433);

-- Summarizing it will map to a hole between block directory entries, so we will
-- start our scan from the entry preceding the hole.
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'skip', '', '', '', 1, -1, 0, dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT brin_summarize_range('ao_partial_scan4_i_idx', 33554433);
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'status', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'reset', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';

-- Sanity: the summary info is reflected in the data page.
1U: SELECT * FROM brin_page_items(get_raw_page('ao_partial_scan4_i_idx', 2), 'ao_partial_scan4_i_idx');

--------------------------------------------------------------------------------
----                          ao_column tables
--------------------------------------------------------------------------------

CREATE TABLE aoco_partial_scan1(i int, j int, k int) USING ao_column;

--------------------------------------------------------------------------------
-- Scenario 1: Starting block number of scans map to block directory entries,
-- across multiple minipages, corresponding to multiple segfiles.
--------------------------------------------------------------------------------

-- Create a couple of seg files, spanning a couple of minipages each.
1: BEGIN;
2: BEGIN;
1: INSERT INTO aoco_partial_scan1 SELECT 1,100,k FROM generate_series(1, 300000) k;
2: INSERT INTO aoco_partial_scan1 SELECT 20,200,k FROM generate_series(1, 300000) k;
1: COMMIT;
2: COMMIT;

-- Doing an index build will result in scanning the relation whole.
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'skip', '', '', '', 1, -1, 0, dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
CREATE INDEX ON aoco_partial_scan1 USING brin(i, j) WITH (pages_per_range = 3);
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'status', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'reset', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';

-- Show the composition of the single data page in the BRIN index.
1U: SELECT * FROM brin_page_items(get_raw_page('aoco_partial_scan1_i_j_idx', 2), 'aoco_partial_scan1_i_j_idx');

-- Now desummarize a few ranges.
1U: SELECT brin_desummarize_range('aoco_partial_scan1_i_j_idx', 33554432);
1U: SELECT brin_desummarize_range('aoco_partial_scan1_i_j_idx', 33554438);
1U: SELECT brin_desummarize_range('aoco_partial_scan1_i_j_idx', 33554441);
1U: SELECT brin_desummarize_range('aoco_partial_scan1_i_j_idx', 67108867);

-- Now summarize these desummarized ranges piecemeal and check that we scan only
-- a subset of the blocks each time.
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'skip', '', '', '', 1, -1, 0, dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT brin_summarize_range('aoco_partial_scan1_i_j_idx', 33554432);
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'status', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'reset', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';

SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'skip', '', '', '', 1, -1, 0, dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT brin_summarize_range('aoco_partial_scan1_i_j_idx', 33554438);
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'status', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'reset', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';

SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'skip', '', '', '', 1, -1, 0, dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT brin_summarize_range('aoco_partial_scan1_i_j_idx', 33554441);
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'status', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'reset', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';

SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'skip', '', '', '', 1, -1, 0, dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT brin_summarize_range('aoco_partial_scan1_i_j_idx', 67108867);
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'status', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'reset', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';

-- Sanity: the summary info is reflected in the data page.
1U: SELECT * FROM brin_page_items(get_raw_page('aoco_partial_scan1_i_j_idx', 2), 'aoco_partial_scan1_i_j_idx');

--------------------------------------------------------------------------------
-- Scenario 2: Starting block number of scan maps to hole at the end of the
-- minipage (after the last entry).
--------------------------------------------------------------------------------
CREATE TABLE aoco_partial_scan2(i int, j int, k int) USING ao_column;
-- Fill 1 logical heap block with committed rows.
INSERT INTO aoco_partial_scan2 SELECT 1, 100, k FROM generate_series(1, 32767) k;
-- Now add some aborted rows at the end of the segfile, resulting in a hole at
-- the end of the minipage.
BEGIN;
INSERT INTO aoco_partial_scan2 SELECT 20, 200, k FROM generate_series(1, 32767) k;
ABORT;

-- Doing an index build will result in scanning the committed rows only.
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'skip', '', '', '', 1, -1, 0, dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
CREATE INDEX ON aoco_partial_scan2 USING brin(i, j) WITH (pages_per_range = 1);
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'status', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'reset', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';

-- Show the composition of the single data page in the BRIN index.
1U: SELECT * FROM brin_page_items(get_raw_page('aoco_partial_scan2_i_j_idx', 2), 'aoco_partial_scan2_i_j_idx');

-- Now desummarize the first range of committed rows.
1U: SELECT brin_desummarize_range('aoco_partial_scan2_i_j_idx', 33554432);

-- Summarizing the first range now should only scan the committed rows. (same
-- as the index build)
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'skip', '', '', '', 1, -1, 0, dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT brin_summarize_range('aoco_partial_scan2_i_j_idx', 33554432);
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'status', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'reset', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';

-- Since we fell into a hole (at the end of the segfile), we skip ahead to the
-- offset of the last block directory entries before the hole.
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'skip', '', '', '', 1, -1, 0, dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT brin_summarize_range('aoco_partial_scan2_i_j_idx', 33554433);
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'status', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'reset', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';

-- Sanity: the summary info is reflected in the data page.
1U: SELECT * FROM brin_page_items(get_raw_page('aoco_partial_scan2_i_j_idx', 2), 'aoco_partial_scan2_i_j_idx');

--------------------------------------------------------------------------------
-- Scenario 3: Starting block number of scan maps to hole at the start of the
-- segfile (and before the first entry of the first minipage).
--------------------------------------------------------------------------------
CREATE TABLE aoco_partial_scan3(i int, j int, k int) USING ao_column;
-- Now add some aborted rows at the end of the segfile, resulting in a hole at
-- the end of the minipage.
BEGIN;
INSERT INTO aoco_partial_scan3 SELECT 1, 100, k FROM generate_series(1, 32767) k;
ABORT;
-- Fill 3 logical heap blocks with committed rows.
INSERT INTO aoco_partial_scan3 SELECT 20, 100, k FROM generate_series(1, 32767 * 3) k;

-- Doing an index build will result in scanning the committed rows only.
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'skip', '', '', '', 1, -1, 0, dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
CREATE INDEX ON aoco_partial_scan3 USING brin(i, j) WITH (pages_per_range = 3);
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'status', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'reset', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';

-- Show the composition of the single data page in the BRIN index.
1U: SELECT * FROM brin_page_items(get_raw_page('aoco_partial_scan3_i_j_idx', 2), 'aoco_partial_scan3_i_j_idx');

-- Now desummarize the range with 1 block of aborted rows and 2 blocks of
-- committed rows.
1U: SELECT brin_desummarize_range('aoco_partial_scan3_i_j_idx', 33554432);

-- Summarizing this range should scan blocks corresponding to the 2 final logical
-- heap blocks in the range only.
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'skip', '', '', '', 1, -1, 0, dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT gp_inject_fault('AppendOnlyBlockDirectory_PartialScan_hole_at_start', 'skip', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT brin_summarize_range('aoco_partial_scan3_i_j_idx', 33554432);
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'status', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT gp_wait_until_triggered_fault('AppendOnlyBlockDirectory_PartialScan_hole_at_start', 1, dbid)
  FROM gp_segment_configuration where role='p' and content = 1;
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'reset', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT gp_inject_fault('AppendOnlyBlockDirectory_PartialScan_hole_at_start', 'reset', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';

-- Sanity: the summary info is reflected in the data page.
1U: SELECT * FROM brin_page_items(get_raw_page('aoco_partial_scan3_i_j_idx', 2), 'aoco_partial_scan3_i_j_idx');

--------------------------------------------------------------------------------
-- Scenario 4: Starting block number of scan maps to hole between two entries
-- in a minipage.
--------------------------------------------------------------------------------

CREATE TABLE aoco_partial_scan4(i int) USING ao_column;

-- Using populate_pages creates holes due to the nature of its piecemeal inserts
-- of 1 row. So, each blkdir entry will have 1 row each. Populate 3 blocks this
-- way, 2 blocks full and 1 block with 1 tuple.
SELECT populate_pages('aoco_partial_scan4', 1, tid '(33554434, 0)');

-- Doing an index build will result in scanning the whole relation.
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'skip', '', '', '', 1, -1, 0, dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
CREATE INDEX ON aoco_partial_scan4 USING brin(i) WITH (pages_per_range = 1);
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'status', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'reset', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';

-- Show the composition of the single data page in the BRIN index.
1U: SELECT * FROM brin_page_items(get_raw_page('aoco_partial_scan4_i_idx', 2), 'aoco_partial_scan4_i_idx');

-- Now desummarize a range.
1U: SELECT brin_desummarize_range('aoco_partial_scan4_i_idx', 33554433);

-- Summarizing it will map to a hole between block directory entries, so we will
-- start our scan from the entries preceding the hole.
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'skip', '', '', '', 1, -1, 0, dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT brin_summarize_range('aoco_partial_scan4_i_idx', 33554433);
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'status', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
SELECT gp_inject_fault('AppendOnlyStorageRead_ReadNextBlock_success', 'reset', dbid)
  FROM gp_segment_configuration WHERE content = 1 AND role = 'p';

-- Sanity: the summary info is reflected in the data page.
1U: SELECT * FROM brin_page_items(get_raw_page('aoco_partial_scan4_i_idx', 2), 'aoco_partial_scan4_i_idx');

DROP EXTENSION pageinspect;

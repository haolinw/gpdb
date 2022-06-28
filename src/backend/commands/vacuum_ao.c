/*-------------------------------------------------------------------------
 *
 * vacuum_ao.c
 *	  VACUUM support for append-only tables.
 *
 *
 * Portions Copyright (c) 2016, VMware, Inc. or its affiliates
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * Overview
 * --------
 *
 * Vacuum of AO and AOCO tables happens in three phases:
 *
 * 1. Pre-cleanup phase
 *
 *   Truncate any old AWAITING_DROP segments to zero bytes. We would do this in
 *   the post-cleanup phase, anyway, but reclaiming space as early as possible
 *   is good. We might need the space to complete the compaction phase.
 *
 * 2. Compaction phase
 *
 *   Copy tuples from segments to new segmnents, leaving out dead tuples.
 *
 * 3. Post-cleanup phase.
 *
 *   Truncate any old AWAITING_DROP segments, making them insertable again. If there
 *   are no other transactions running (TODO: or we're in "aggressive mode" and want
 *   to risk "snapshot too old" errors), this can truncate the old segments left
 *   behind in the compaction phase.
 *
 *   Vacuum indexes.
 *
 *   Vacuum auxiliary heap tables.
 *
 * The orchestration of the phases mostly happens in vacuum_rel() (vacuum.c).
 * This file contains functions implementing the phases.
 *
 * The pre-cleanup and post-cleanup phases could run in a local transaction,
 * but the compaction phase needs a distributed transaction.
 * Currently, though, we run each phase in a distributed transaction; there's
 * no harm in that.
 *
 * Both lazy and FULL vacuum work the same on AO tables.
 *
 *
 * Why does compaction have to run in a distributed transaction?
 * ---------------------------------------------------------------------
 *
 * To determine the visibility of AO segments, we rely on the auxiliary
 * pg_aoseg_* heap table. The visiblity of the rows in the pg_aoseg table
 * determines which segments are visible to a snapshot.
 *
 * That works great currently, but if we switch to updating pg_aoseg in a
 * local transaction, some anomalies become possible. A distributed
 * transaction might see an inconsistent view of the segments, because one
 * row version in pg_aoseg is visible according to the distributed snapshot,
 * while another version of the same row is visible to its local snapshot.
 *
 * In fact, you can observe this anomaly even without appendonly tables, if
 * you modify a heap table in utility mode. Here's an example (as an
 * isolationtest2 test schedule):
 *
 * --------------------
 * DROP TABLE IF EXISTS utiltest;
 * CREATE TABLE utiltest (a int, t text);
 * INSERT INTO utiltest SELECT i as a, 'initial' FROM generate_series(1, 10) AS i;
 *
 * create or replace function myfunc() returns setof utiltest as $$ begin perform pg_sleep(10); return query select * from utiltest; end; $$ stable language plpgsql;
 *
 * -- Launch one backend to query the table with a delay. It will acquire
 * -- snapshot first, but scan the table only later. (A serializable snapshot
 * -- would achieve the same thing.)
 * 1&: select * from myfunc();
 *
 * -- Update normally via QD. The delayed query does not see this update, as
 * -- it acquire the snapshot before this.
 * 2: UPDATE utiltest SET t = 'updated' WHERE a <= 3;
 *
 * -- Update again in utility mode. Does the delayed query see this or not?
 * 0U: UPDATE utiltest  SET t = 'updated in utility mode' WHERE a <= 5;
 *
 * -- Get the delayed query's results. It returns 12 rows. It sees the
 * -- initial version of each row, but it *also* sees the new tuple
 * -- versions of the utility-mode update. The utility-updated rows have
 * -- an xmin that's visible to the local snapshot that the delayed query.
 * 1<:
 * --------------------
 *
 * In summary, this test case creates a table with 10 rows, and updates
 * some rows twice. One of the updates is made in utility mode. No rows are
 * deleted or inserted after the initial load, so any query on the table
 * should see 10 rows. But the query that's performed in the function sees
 * 12 rows. It sees two versions of the rows that were updated twice.
 *
 * This is clearly not ideal, but we can perhaps shrug it off as "don't do
 * that". If you update rows in utility mode, weird things can happen. But
 * it poses a problem for the idea of using local transactions for AO
 * vacuum. We can't let that anomaly to happen as a result of a normal VACUUM!
 *
 *
 * XXX: How does index vacuum work? We never reuse TIDs, right? So we can
 * vacuum indexes independently of dropping segments.
 *
 * XXX: We could relax the requirement for AccessExclusiveLock in Vacuum drop
 * phase with a little more effort. Scan could grab a share lock on the segfile
 * it's about to scan.
 *
 * IDENTIFICATION
 *	  src/backend/commands/vacuum_ao.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/aocs_compaction.h"
#include "access/appendonlywriter.h"
#include "access/appendonly_compaction.h"
#include "access/genam.h"
#include "access/multixact.h"
#include "access/visibilitymap.h"
#include "access/xact.h"
#include "catalog/pg_appendonly.h"
#include "cdb/cdbappendonlyam.h"
#include "cdb/cdbtm.h"
#include "cdb/cdbvars.h"
#include "commands/vacuum.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "storage/freespace.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "utils/faultinjector.h"
#include "utils/guc.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/lsyscache.h"
#include "utils/pg_rusage.h"
#include "cdb/cdbappendonlyblockdirectory.h"


static void vacuum_appendonly_index(Relation indexRelation,
									double rel_tuple_count,
									Bitmapset *dead_segs,
									int elevel,
									BufferAccessStrategy bstrategy);

static bool appendonly_tid_reaped(ItemPointer itemptr, void *state);

static void vacuum_appendonly_fill_stats(Relation aorel, Snapshot snapshot, int elevel,
										 BlockNumber *rel_pages, double *rel_tuples,
										 bool *relhasindex);
static int vacuum_appendonly_indexes(Relation aoRelation, int options, Bitmapset *dead_segs,
									 BufferAccessStrategy bstrategy);

static void
ao_vacuum_rel_pre_cleanup(Relation onerel, VacuumParams *params, BufferAccessStrategy bstrategy)
{
	char	   *relname;
	int			elevel;
	Bitmapset	*dead_segs;
	int			options = params->options;

	if (options & VACOPT_VERBOSE)
		elevel = INFO;
	else
		elevel = DEBUG2;

	if (Gp_role == GP_ROLE_DISPATCH)
		elevel = DEBUG2; /* vacuum and analyze messages aren't interesting from the QD */

	relname = RelationGetRelationName(onerel);
	ereport(elevel,
			(errmsg("vacuuming \"%s.%s\"",
					get_namespace_name(RelationGetNamespace(onerel)),
					relname)));

	/*
	 * Collect AWAITING_DROP segments that are no longer visible to anyone.
	 * We cannot actually remove them yet, because there might still be index
	 * entries pointing to them. We cannot recycle the segments until the
	 * indexes have been vacuumed.
	 *
	 * This is optional. We'll drop old AWAITING_DROP segments in the
	 * post-cleanup phase, too, but doing this first helps to reclaim some
	 * space earlier. The compaction phase might need the space.
	 *
	 * This could run in a local transaction.
	 */
	AppendOptimizedRecycleDeadSegments(onerel, &dead_segs);

	/*
	 * Vacuum indexes only when we do find AWAITING_DROP segments,
	 * no need to update stats in pre_cleanup phase.
	 */
	if (dead_segs != NULL)
	{
		/*
		 * Do index vacuuming before dropping dead segments for data
		 * consistency and crash safety. If dropping dead segments before
		 * cleaning up index tuples, the following issues may occur:
		 * 
		 * a) The dead segment file becomes available as soon as dropping
		 * complete. Concurrent inserts may fill it with new tuples hence
		 * might be deleted soon in the following index vacuuming;
		 * 
		 * b) Crash happens in-between AppendOptimizedRecycleDeadSegments()
		 * and vacuum_appendonly_indexes() result in losing the opportunity
		 * to clean index entries fully as a state for which index tuples
		 * to delete will be lost in this case.
		 * 
		 * So make sure to vacuum indexs to be based on persistent information
		 * (AWAITING_DROP state in pg_aoseg) to cleanup dead index tuples
		 * effectively.
		 */
		vacuum_appendonly_indexes(onerel, options, dead_segs, bstrategy);
		/*
		 * Truncate AWAITING_DROP the above collected segments to 0 byte.
		 * Assuming no transaction is able to access the dead segments as it
		 * has been already marked as AWAITING_DROP, no lock will be held in
		 * clearing segment info.
		 */
		AppendOptimizedDropDeadSegments(onerel, dead_segs);
		bms_free(dead_segs);
	}

	/*
	 * Also truncate all live segments to the EOF values stored in pg_aoseg.
	 * This releases space left behind by aborted inserts.
	 */
	AppendOptimizedTruncateToEOF(onerel);
}


static void
ao_vacuum_rel_post_cleanup(Relation onerel, VacuumParams *params, BufferAccessStrategy bstrategy)
{
	BlockNumber	relpages;
	double		reltuples;
	bool		relhasindex;
	int			elevel;
	int			options = params->options;
	TransactionId OldestXmin;
	TransactionId FreezeLimit;
	MultiXactId MultiXactCutoff;
	TransactionId xidFullScanLimit;
	MultiXactId mxactFullScanLimit;
	Bitmapset	*dead_segs;

	if (options & VACOPT_VERBOSE)
		elevel = INFO;
	else
		elevel = DEBUG2;

	if (Gp_role == GP_ROLE_DISPATCH)
		elevel = DEBUG2; /* vacuum and analyze messages aren't interesting from the QD */

	/*
	 * This could run in a *local* transaction:
	 *
	 * 1. Recycled any dead AWAITING_DROP segments, like in the
	 *    pre-cleanup phase.
	 *
	 * 2. Vacuum indexes.
	 * 
	 * 3. Drop/Truncate dead segments.
	 */
	Assert(RelationIsAoRows(onerel) || RelationIsAoCols(onerel));

	AppendOptimizedRecycleDeadSegments(onerel, &dead_segs);
	/* Vacuum indexes and update stats in post_cleanup phases. */
	vacuum_appendonly_indexes(onerel, options, dead_segs, bstrategy);
	AppendOptimizedDropDeadSegments(onerel, dead_segs);
	bms_free(dead_segs);

	/* Update statistics in pg_class */
	vacuum_appendonly_fill_stats(onerel, GetActiveSnapshot(),
								 elevel,
								 &relpages,
								 &reltuples,
								 &relhasindex);

	vacuum_set_xid_limits(onerel,
						  params->freeze_min_age,
						  params->freeze_table_age,
						  params->multixact_freeze_min_age,
						  params->multixact_freeze_table_age,
						  &OldestXmin, &FreezeLimit, &xidFullScanLimit,
						  &MultiXactCutoff, &mxactFullScanLimit);

	vac_update_relstats(onerel,
						relpages,
						reltuples,
						0, /* AO does not currently have an equivalent to
							  Heap's 'all visible pages' */
						relhasindex,
						FreezeLimit,
						MultiXactCutoff,
						false,
						true /* isvacuum */);
}

static void
ao_vacuum_rel_compact(Relation onerel, VacuumParams *params, BufferAccessStrategy bstrategy)
{
	int			compaction_segno;
	int			insert_segno;
	List	   *compacted_segments = NIL;
	List	   *compacted_and_inserted_segments = NIL;
	Snapshot	appendOnlyMetaDataSnapshot = RegisterSnapshot(GetCatalogSnapshot(InvalidOid));
	char	   *relname;
	int			elevel;
	int			options = params->options;

	/*
	 * This should run in a distributed transaction. But also allow utility
	 * mode. This also runs in the QD, but it should have no work to do because
	 * all data resides on QEs nodes.
	 */
	Assert(Gp_role == GP_ROLE_DISPATCH ||
		   Gp_role == GP_ROLE_UTILITY ||
		   DistributedTransactionContext == DTX_CONTEXT_QE_TWO_PHASE_IMPLICIT_WRITER ||
		   DistributedTransactionContext == DTX_CONTEXT_QE_TWO_PHASE_EXPLICIT_WRITER);
	Assert(RelationIsAoRows(onerel) || RelationIsAoCols(onerel));

	if (options & VACOPT_VERBOSE)
		elevel = INFO;
	else
		elevel = DEBUG2;

	if (Gp_role == GP_ROLE_DISPATCH)
		elevel = DEBUG2; /* vacuum and analyze messages aren't interesting from the QD */

	relname = RelationGetRelationName(onerel);
	ereport(elevel,
			(errmsg("compacting \"%s.%s\"",
					get_namespace_name(RelationGetNamespace(onerel)),
					relname)));

	/*
	 * Compact all the segfiles. Repeat as many times as required.
	 *
	 * XXX: Because we compact all segfiles in one transaction, this can
	 * require up 2x the disk space. Alternatively, we could split this into
	 * multiple transactions. The problem with that is that the updates to
	 * pg_aoseg needs to happen in a distributed transaction (Problem 3), so
	 * we would need to coordinate the transactions from the QD.
	 */
	insert_segno = -1;
	while ((compaction_segno = ChooseSegnoForCompaction(onerel, compacted_and_inserted_segments)) != -1)
	{
		/*
		 * Compact this segment. (If the segment doesn't need compaction,
		 * AppendOnlyCompact() will fall through quickly).
		 */
		compacted_segments = lappend_int(compacted_segments, compaction_segno);
		compacted_and_inserted_segments = lappend_int(compacted_and_inserted_segments,
													  compaction_segno);

		/* XXX: maybe print this deeper, only if there's work to be done? */
		if (Debug_appendonly_print_compaction)
			elog(LOG, "compacting segno %d of %s", compaction_segno, relname);

		if (RelationIsAoRows(onerel))
			AppendOnlyCompact(onerel,
							  compaction_segno,
							  &insert_segno,
							  (options & VACOPT_FULL) != 0,
							  compacted_segments);
		else
		{
			Assert(RelationIsAoCols(onerel));
			AOCSCompact(onerel,
						compaction_segno,
						&insert_segno,
						(options & VACOPT_FULL) != 0,
						compacted_segments);
		}

		if (insert_segno != -1)
			compacted_and_inserted_segments = list_append_unique_int(compacted_and_inserted_segments,
																	 insert_segno);

		/*
		 * AppendOnlyCompact() updates pg_aoseg. Increment the command counter, so
		 * that we can update the insertion target pg_aoseg row again.
		 */
		CommandCounterIncrement();
	}

	UnregisterSnapshot(appendOnlyMetaDataSnapshot);
}

/*
 * ao_vacuum_rel()
 *
 * Common interface for vacuuming Append-Optimized table.
 */
void
ao_vacuum_rel(Relation rel, VacuumParams *params, BufferAccessStrategy bstrategy)
{
	Assert(RelationIsAppendOptimized(rel));
	Assert(params != NULL);

	int ao_vacuum_phase = (params->options & VACUUM_AO_PHASE_MASK);

	/*
	 * Do the actual work --- either FULL or "lazy" vacuum
	 */
	if (ao_vacuum_phase == VACOPT_AO_PRE_CLEANUP_PHASE)
		ao_vacuum_rel_pre_cleanup(rel, params, bstrategy);
	else if (ao_vacuum_phase == VACOPT_AO_COMPACT_PHASE)
		ao_vacuum_rel_compact(rel, params, bstrategy);
	else if (ao_vacuum_phase == VACOPT_AO_POST_CLEANUP_PHASE)
		ao_vacuum_rel_post_cleanup(rel, params, bstrategy);
	else
		/* Do nothing here, we will launch the stages later */
		Assert(ao_vacuum_phase == 0);
}

/*
 * vacuum_appendonly_indexes()
 *
 * Perform a vacuum on all indexes of an append-only relation.
 *
 * It returns the number of indexes on the relation.
 */
static int
vacuum_appendonly_indexes(Relation aoRelation, int options, Bitmapset *dead_segs,
						  BufferAccessStrategy bstrategy)
{
	int			i;
	Relation   *Irel;
	int			nindexes;

	Assert(RelationIsAppendOptimized(aoRelation));

	if (Debug_appendonly_print_compaction)
		elog(LOG, "Vacuum indexes for append-only relation %s",
			 RelationGetRelationName(aoRelation));

	/* Now open all indexes of the relation */
	if ((options & VACOPT_FULL))
		vac_open_indexes(aoRelation, AccessExclusiveLock, &nindexes, &Irel);
	else
		vac_open_indexes(aoRelation, RowExclusiveLock, &nindexes, &Irel);

	/* Clean/scan index relation(s) */
	if (Irel != NULL)
	{
		int elevel;

		if (options & VACOPT_VERBOSE)
			elevel = INFO;
		else
			elevel = DEBUG2;

		/* just scan indexes to update statistic */
		if (Gp_role == GP_ROLE_DISPATCH || bms_is_empty(dead_segs))
		{
			for (i = 0; i < nindexes; i++)
			{
				scan_index(Irel[i], Irel[i]->rd_rel->reltuples, elevel, bstrategy);
			}
		}
		else
		{
			for (i = 0; i < nindexes; i++)
			{
				vacuum_appendonly_index(Irel[i],
										Irel[i]->rd_rel->reltuples,
										dead_segs,
										elevel,
										bstrategy);
			}
		}
	}

	vac_close_indexes(nindexes, Irel, NoLock);
	return nindexes;
}

/*
 * Vacuums an index on an append-only table.
 *
 * This is called after an append-only segment file compaction to move
 * all tuples from the compacted segment files.
 * The segmentFileList is an
 */
static void
vacuum_appendonly_index(Relation indexRelation,
						double rel_tuple_count,
						Bitmapset *dead_segs,
						int elevel,
						BufferAccessStrategy bstrategy)
{
	IndexBulkDeleteResult *stats;
	IndexVacuumInfo ivinfo;
	PGRUsage	ru0;

	Assert(RelationIsValid(indexRelation));

	pg_rusage_init(&ru0);

	ivinfo.index = indexRelation;
	ivinfo.message_level = elevel;
	ivinfo.num_heap_tuples = rel_tuple_count;
	ivinfo.strategy = bstrategy;

	/* Do bulk deletion */
	stats = index_bulk_delete(&ivinfo, NULL, appendonly_tid_reaped,
							  (void *) dead_segs);

	/* Do post-VACUUM cleanup */
	stats = index_vacuum_cleanup(&ivinfo, stats);

	if (!stats)
		return;

	/*
	 * Now update statistics in pg_class, but only if the index says the count
	 * is accurate.
	 */
	if (!stats->estimated_count)
		vac_update_relstats(indexRelation,
							stats->num_pages, stats->num_index_tuples,
							0, /* relallvisible */
							false,
							InvalidTransactionId,
							InvalidMultiXactId,
							false,
							true /* isvacuum */);

	ereport(elevel,
			(errmsg("index \"%s\" now contains %.0f row versions in %u pages",
					RelationGetRelationName(indexRelation),
					stats->num_index_tuples,
					stats->num_pages),
			 errdetail("%.0f index row versions were removed.\n"
			 "%u index pages have been deleted, %u are currently reusable.\n"
					   "%s.",
					   stats->tuples_removed,
					   stats->pages_deleted, stats->pages_free,
					   pg_rusage_show(&ru0))));

	pfree(stats);
}

/*
 * appendonly_tid_reaped()
 *
 * Is a particular tid for an appendonly reaped? the inputed state
 * is a bitmap of dropped segno. The index entry is reaped only
 * because of the segment no is a member of dead_segs. In this
 * way, no need to scan visibility map so the performance would be
 * good.
 *
 * This has the right signature to be an IndexBulkDeleteCallback.
 */
static bool
appendonly_tid_reaped(ItemPointer itemptr, void *state)
{
	Bitmapset *dead_segs = (Bitmapset *) state;
	int segno = AOTupleIdGet_segmentFileNum((AOTupleId *)itemptr);

	return bms_is_member(segno, dead_segs);
}

/*
 * Fills in the relation statistics for an append-only relation.
 *
 *	This information is used to update the reltuples and relpages information
 *	in pg_class. reltuples is the same as "pg_aoseg_<oid>:tupcount"
 *	column and we simulate relpages by subdividing the eof value
 *	("pg_aoseg_<oid>:eof") over the defined page size.
 */
static void
vacuum_appendonly_fill_stats(Relation aorel, Snapshot snapshot, int elevel,
							 BlockNumber *rel_pages, double *rel_tuples,
							 bool *relhasindex)
{
	FileSegTotals *fstotal;
	BlockNumber nblocks;
	char	   *relname;
	double		num_tuples;
	int64       hidden_tupcount;
	AppendOnlyVisimap visimap;
	Oid			visimaprelid;
	Oid			visimapidxid;

	Assert(RelationIsAoRows(aorel) || RelationIsAoCols(aorel));

	relname = RelationGetRelationName(aorel);

	/* get updated statistics from the pg_aoseg table */
	if (RelationIsAoRows(aorel))
	{
		fstotal = GetSegFilesTotals(aorel, snapshot);
	}
	else
	{
		Assert(RelationIsAoCols(aorel));
		fstotal = GetAOCSSSegFilesTotals(aorel, snapshot);
	}

	/* calculate the values we care about */
	num_tuples = (double)fstotal->totaltuples;
	nblocks = (uint32)RelationGetNumberOfBlocks(aorel);

	GetAppendOnlyEntryAuxOids(aorel->rd_id,
							  snapshot, 
							  NULL, NULL, NULL,
							  &visimaprelid, &visimapidxid);

	AppendOnlyVisimap_Init(&visimap,
						   visimaprelid,
						   visimapidxid,
						   AccessShareLock,
						   snapshot);
	hidden_tupcount = AppendOnlyVisimap_GetRelationHiddenTupleCount(&visimap);
	num_tuples -= hidden_tupcount;
	Assert(num_tuples > -1.0);
	AppendOnlyVisimap_Finish(&visimap, AccessShareLock);

	if (Debug_appendonly_print_compaction)
		elog(LOG,
			 "Gather statistics after vacuum for append-only relation %s: "
			 "page count %d, tuple count %f",
			 relname,
			 nblocks, num_tuples);

	*rel_pages = nblocks;
	*rel_tuples = num_tuples;
	*relhasindex = aorel->rd_rel->relhasindex;

	ereport(elevel,
			(errmsg("\"%s\": found %.0f rows in %u pages.",
					relname, num_tuples, nblocks)));
	pfree(fstotal);
}

/*
 *	scan_index() -- scan one index relation to update pg_class statistics.
 *
 * We use this when we have no deletions to do.
 */
void
scan_index(Relation indrel, double num_tuples,
		   int elevel, BufferAccessStrategy vac_strategy)
{
	IndexBulkDeleteResult *stats;
	IndexVacuumInfo ivinfo;
	PGRUsage	ru0;

	pg_rusage_init(&ru0);

	ivinfo.index = indrel;
	ivinfo.analyze_only = false;
	ivinfo.estimated_count = false;
	ivinfo.message_level = elevel;
	ivinfo.num_heap_tuples = num_tuples;
	ivinfo.strategy = vac_strategy;

	stats = index_vacuum_cleanup(&ivinfo, NULL);

	if (!stats)
		return;

	/*
	 * Now update statistics in pg_class, but only if the index says the count
	 * is accurate.
	 */
	if (!stats->estimated_count)
		vac_update_relstats(indrel,
							stats->num_pages, stats->num_index_tuples,
							0, /* relallvisible, don't bother for indexes */
							false,
							InvalidTransactionId,
							InvalidMultiXactId,
							false,
							true /* isvacuum */);

	ereport(elevel,
			(errmsg("index \"%s\" now contains %.0f row versions in %u pages",
					RelationGetRelationName(indrel),
					stats->num_index_tuples,
					stats->num_pages),
	errdetail("%u index pages have been deleted, %u are currently reusable.\n"
			  "%s.",
			  stats->pages_deleted, stats->pages_free,
			  pg_rusage_show(&ru0))));

	pfree(stats);
}

/*-------------------------------------------------------------------------
 *
 * twophase_rmgr.c
 *	  Two-phase-commit resource managers tables
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/transam/twophase_rmgr.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/multixact.h"
#include "access/shard_stamp.h"
#include "access/twophase_rmgr.h"
#include "pgstat.h"
#include "storage/lock.h"
#include "storage/predicate.h"


/* 0009：分片 2PC 段钩子（默认 NULL = 行为不变；扩展装载） */
shard_twophase_cb_type shard_twophase_recover_hook = NULL;
shard_twophase_cb_type shard_twophase_postcommit_hook = NULL;
shard_twophase_cb_type shard_twophase_postabort_hook = NULL;
shard_at_prepare_hook_type shard_at_prepare_hook = NULL;

static void
shard_twophase_recover(TransactionId xid, uint16 info,
					   void *recdata, uint32 len)
{
	if (shard_twophase_recover_hook)
		shard_twophase_recover_hook(xid, info, recdata, len);
}

static void
shard_twophase_postcommit(TransactionId xid, uint16 info,
						  void *recdata, uint32 len)
{
	if (shard_twophase_postcommit_hook)
		shard_twophase_postcommit_hook(xid, info, recdata, len);
}

static void
shard_twophase_postabort(TransactionId xid, uint16 info,
						 void *recdata, uint32 len)
{
	if (shard_twophase_postabort_hook)
		shard_twophase_postabort_hook(xid, info, recdata, len);
}

const TwoPhaseCallback twophase_recover_callbacks[TWOPHASE_RM_MAX_ID + 1] =
{
	NULL,						/* END ID */
	lock_twophase_recover,		/* Lock */
	NULL,						/* pgstat */
	multixact_twophase_recover, /* MultiXact */
	predicatelock_twophase_recover,	/* PredicateLock */
	shard_twophase_recover		/* 0009 Shard */
};

const TwoPhaseCallback twophase_postcommit_callbacks[TWOPHASE_RM_MAX_ID + 1] =
{
	NULL,						/* END ID */
	lock_twophase_postcommit,	/* Lock */
	pgstat_twophase_postcommit, /* pgstat */
	multixact_twophase_postcommit,	/* MultiXact */
	NULL,						/* PredicateLock */
	shard_twophase_postcommit	/* 0009 Shard */
};

const TwoPhaseCallback twophase_postabort_callbacks[TWOPHASE_RM_MAX_ID + 1] =
{
	NULL,						/* END ID */
	lock_twophase_postabort,	/* Lock */
	pgstat_twophase_postabort,	/* pgstat */
	multixact_twophase_postabort,	/* MultiXact */
	NULL,						/* PredicateLock */
	shard_twophase_postabort	/* 0009 Shard */
};

const TwoPhaseCallback twophase_standby_recover_callbacks[TWOPHASE_RM_MAX_ID + 1] =
{
	NULL,						/* END ID */
	lock_twophase_standby_recover,	/* Lock */
	NULL,						/* pgstat */
	NULL,						/* MultiXact */
	NULL,						/* PredicateLock */
	NULL						/* 0009 Shard（本项目无热备） */
};

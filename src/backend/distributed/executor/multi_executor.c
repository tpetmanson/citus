/*-------------------------------------------------------------------------
 *
 * multi_executor.c
 *
 * Entrypoint into distributed query execution.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_master_planner.h"
#include "distributed/multi_planner.h"
#include "distributed/multi_router_executor.h"
#include "distributed/multi_resowner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/multi_utility.h"
#include "distributed/worker_protocol.h"
#include "executor/execdebug.h"
#include "executor/executor.h"
#include "commands/copy.h"
#include "nodes/makefuncs.h"
#include "storage/lmgr.h"
#include "tcop/utility.h"
#include "utils/snapmgr.h"
#include "utils/memutils.h"


/*
 * FIXME: Comment
 *
 * I think it's better however to only have one type of CitusScanState, to
 * allow to easily share code between routines.
 */
static CustomExecMethods RealTimeCustomExecMethods = {
	.CustomName = "RealTimeScan",
	.BeginCustomScan = CitusSelectBeginScan,
	.ExecCustomScan = RealTimeExecScan,
	.EndCustomScan = CitusEndScan,
	.ReScanCustomScan = CitusReScan,
	.ExplainCustomScan = CitusExplainScan
};

static CustomExecMethods TaskTrackerCustomExecMethods = {
	.CustomName = "TaskTrackerScan",
	.BeginCustomScan = CitusSelectBeginScan,
	.ExecCustomScan = TaskTrackerExecScan,
	.EndCustomScan = CitusEndScan,
	.ReScanCustomScan = CitusReScan,
	.ExplainCustomScan = CitusExplainScan
};

static CustomExecMethods RouterModificationCustomExecMethods = {
	.CustomName = "RouterModificationScan",
	.BeginCustomScan = CitusModifyBeginScan,
	.ExecCustomScan = RouterModificationExecScan,
	.EndCustomScan = CitusEndScan,
	.ReScanCustomScan = CitusReScan,
	.ExplainCustomScan = CitusExplainScan
};

static CustomExecMethods RouterSelectCustomExecMethods = {
	.CustomName = "RouterModificationScan",
	.BeginCustomScan = CitusSelectBeginScan,
	.ExecCustomScan = RouterSelectExecScan,
	.EndCustomScan = CitusEndScan,
	.ReScanCustomScan = CitusReScan,
	.ExplainCustomScan = CitusExplainScan
};

static CustomExecMethods RouterMultipleTasksCustomExecMethods = {
	.CustomName = "RouterModificationScan",
	.BeginCustomScan = CitusModifyBeginScan,
	.ExecCustomScan = RouterMultipleTasksExecScan,
	.EndCustomScan = CitusEndScan,
	.ReScanCustomScan = CitusReScan,
	.ExplainCustomScan = CitusExplainScan
};


static void PrepareMasterJobDirectory(Job *workerJob);
static void LoadTuplesIntoTupleStore(CitusScanState *scanState, Job *workerJob);
static Relation FauxRelation(TupleDesc tupleDescriptor);


Node *
RouterCreateScan(CustomScan *scan)
{
	CitusScanState *scanState = palloc0(sizeof(CitusScanState));
	MultiPlan *multiPlan = NULL;
	Job *workerJob = NULL;
	List *taskList = NIL;

	scanState->executorType = MULTI_EXECUTOR_ROUTER;
	scanState->customScanState.ss.ps.type = T_CustomScanState;
	scanState->multiPlan = GetMultiPlan(scan);

	multiPlan = scanState->multiPlan;
	workerJob = multiPlan->workerJob;
	taskList = workerJob->taskList;

	if (list_length(taskList) == 1)
	{
		bool isModificationQuery = IsModificationPlan(multiPlan);
		if (isModificationQuery)
		{
			scanState->customScanState.methods = &RouterModificationCustomExecMethods;
		}
		else
		{
			scanState->customScanState.methods = &RouterSelectCustomExecMethods;
		}
	}
	else
	{
		scanState->customScanState.methods = &RouterMultipleTasksCustomExecMethods;
	}

	return (Node *) scanState;
}


Node *
RealTimeCreateScan(CustomScan *scan)
{
	CitusScanState *scanState = palloc0(sizeof(CitusScanState));

	scanState->executorType = MULTI_EXECUTOR_REAL_TIME;
	scanState->customScanState.ss.ps.type = T_CustomScanState;
	scanState->multiPlan = GetMultiPlan(scan);

	scanState->customScanState.methods = &RealTimeCustomExecMethods;

	return (Node *) scanState;
}


Node *
TaskTrackerCreateScan(CustomScan *scan)
{
	CitusScanState *scanState = palloc0(sizeof(CitusScanState));

	scanState->executorType = MULTI_EXECUTOR_TASK_TRACKER;
	scanState->customScanState.ss.ps.type = T_CustomScanState;
	scanState->multiPlan = GetMultiPlan(scan);

	scanState->customScanState.methods = &TaskTrackerCustomExecMethods;

	return (Node *) scanState;
}


Node *
InvalidCreateScan(CustomScan *scan)
{
	CitusScanState *scanState = palloc0(sizeof(CitusScanState));

	scanState->executorType = MULTI_EXECUTOR_INVALID_FIRST;
	scanState->customScanState.ss.ps.type = T_CustomScanState;
	scanState->multiPlan = GetMultiPlan(scan);

	Assert(IsA(scanState, CustomScanState));

	/* ensure plan is executable */
	VerifyMultiPlanValidity(scanState->multiPlan);

	return (Node *) scanState;
}


void
CitusSelectBeginScan(CustomScanState *node, EState *estate, int eflags)
{
	ValidateCitusScanState(node);
}


void
ValidateCitusScanState(CustomScanState *node)
{
	CitusScanState *scanState = (CitusScanState *) node;
	MultiPlan *multiPlan = scanState->multiPlan;

	Assert(IsA(scanState, CustomScanState));

	/* ensure plan is executable */
	VerifyMultiPlanValidity(multiPlan);
}


TupleTableSlot *
RealTimeExecScan(CustomScanState *node)
{
	CitusScanState *scanState = (CitusScanState *) node;
	TupleTableSlot *resultSlot = NULL;

	if (!scanState->finishedRemoteScan)
	{
		MultiPlan *multiPlan = scanState->multiPlan;
		Job *workerJob = multiPlan->workerJob;

		PrepareMasterJobDirectory(workerJob);
		MultiRealTimeExecute(workerJob);

		LoadTuplesIntoTupleStore(scanState, workerJob);

		scanState->finishedRemoteScan = true;
	}

	resultSlot = ReadNextTuple(scanState);

	return resultSlot;
}


TupleTableSlot *
TaskTrackerExecScan(CustomScanState *node)
{
	CitusScanState *scanState = (CitusScanState *) node;
	TupleTableSlot *resultSlot = NULL;

	if (!scanState->finishedRemoteScan)
	{
		MultiPlan *multiPlan = scanState->multiPlan;
		Job *workerJob = multiPlan->workerJob;

		PrepareMasterJobDirectory(workerJob);
		MultiTaskTrackerExecute(workerJob);

		LoadTuplesIntoTupleStore(scanState, workerJob);

		scanState->finishedRemoteScan = true;
	}

	resultSlot = ReadNextTuple(scanState);

	return resultSlot;
}


/*
 * PrepareMasterJobDirectory creates a directory on the master node to keep task
 * execution results. We also register this directory for automatic cleanup on
 * portal delete.
 */
static void
PrepareMasterJobDirectory(Job *workerJob)
{
	StringInfo jobDirectoryName = MasterJobDirectoryName(workerJob->jobId);
	CreateDirectory(jobDirectoryName);

	ResourceOwnerEnlargeJobDirectories(CurrentResourceOwner);
	ResourceOwnerRememberJobDirectory(CurrentResourceOwner, workerJob->jobId);
}


/*
 * Load data collected by real-time or task-tracker executors into the tuplestore
 * of CitusScanState. For that first create a tuplestore, and then copy the
 * files one-by-one.
 *
 * Note that in the long term it'd be a lot better if Multi*Execute() directly
 * filled the tuplestores, but that's a fair bit of work.
 */
static void
LoadTuplesIntoTupleStore(CitusScanState *scanState, Job *workerJob)
{
	CustomScanState customScanState = scanState->customScanState;
	List *workerTaskList = workerJob->taskList;
	EState *executorState = NULL;
	MemoryContext executorTupleContext = NULL;
	ExprContext *executorExpressionContext = NULL;
	TupleDesc tupleDescriptor = NULL;
	Relation fauxRelation = NULL;
	ListCell *workerTaskCell = NULL;
	uint32 columnCount = 0;
	Datum *columnValues = NULL;
	bool *columnNulls = NULL;

	executorState = scanState->customScanState.ss.ps.state;
	executorTupleContext = GetPerTupleMemoryContext(executorState);
	executorExpressionContext = GetPerTupleExprContext(executorState);

	tupleDescriptor = customScanState.ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor;
	fauxRelation = FauxRelation(tupleDescriptor);

	columnCount = tupleDescriptor->natts;
	columnValues = palloc0(columnCount * sizeof(Datum));
	columnNulls = palloc0(columnCount * sizeof(bool));

	Assert(scanState->tuplestorestate == NULL);
	scanState->tuplestorestate = tuplestore_begin_heap(true, false, work_mem);

	foreach(workerTaskCell, workerTaskList)
	{
		Task *workerTask = (Task *) lfirst(workerTaskCell);
		StringInfo jobDirectoryName = MasterJobDirectoryName(workerTask->jobId);
		StringInfo taskFilename = TaskFilename(jobDirectoryName, workerTask->taskId);
		List *copyOptions = NIL;
		CopyState copyState = NULL;

		if (BinaryMasterCopyFormat)
		{
			DefElem *copyOption = makeDefElem("format", (Node *) makeString("binary"));
			copyOptions = lappend(copyOptions, copyOption);
		}

		copyState = BeginCopyFrom(fauxRelation, taskFilename->data, false, NULL,
								  copyOptions);

		while (true)
		{
			MemoryContext oldContext = NULL;
			bool nextRowFound = false;

			ResetPerTupleExprContext(executorState);
			oldContext = MemoryContextSwitchTo(executorTupleContext);

			nextRowFound = NextCopyFrom(copyState, executorExpressionContext,
										columnValues, columnNulls, NULL);
			if (!nextRowFound)
			{
				MemoryContextSwitchTo(oldContext);
				break;
			}

			tuplestore_putvalues(scanState->tuplestorestate, tupleDescriptor,
								 columnValues, columnNulls);
			MemoryContextSwitchTo(oldContext);
		}
	}
}


/*
 * FauxRelation creates a faux Relation from the given tuple descriptor.
 * To be able to use copy.c, we need a Relation descriptor. As there's no
 * relation corresponding to the data loaded from workers, we need to fake one.
 * We just need the bare minimal set of fields accessed by BeginCopyFrom().
 */
static Relation
FauxRelation(TupleDesc tupleDescriptor)
{
	Relation fauxRelation = palloc0(sizeof(RelationData));
	fauxRelation->rd_att = tupleDescriptor;
	fauxRelation->rd_rel = palloc0(sizeof(FormData_pg_class));
	fauxRelation->rd_rel->relkind = RELKIND_RELATION;

	return fauxRelation;
}


TupleTableSlot *
ReadNextTuple(CitusScanState *scanState)
{
	Tuplestorestate *tupleStore = scanState->tuplestorestate;
	TupleTableSlot *resultSlot = NULL;
	ScanDirection scanDirection = NoMovementScanDirection;
	bool forwardScanDirection = true;

	if (tupleStore == NULL)
	{
		return NULL;
	}

	scanDirection = scanState->customScanState.ss.ps.state->es_direction;
	Assert(ScanDirectionIsValid(scanDirection));

	if (ScanDirectionIsBackward(scanDirection))
	{
		forwardScanDirection = false;
	}

	resultSlot = scanState->customScanState.ss.ps.ps_ResultTupleSlot;
	tuplestore_gettupleslot(tupleStore, forwardScanDirection, false, resultSlot);

	return resultSlot;
}


void
CitusEndScan(CustomScanState *node)
{
	CitusScanState *scanState = (CitusScanState *) node;

	if (scanState->tuplestorestate)
	{
		tuplestore_end(scanState->tuplestorestate);
		scanState->tuplestorestate = NULL;
	}
}


void
CitusReScan(CustomScanState *node)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("rescan is unsupported"),
					errdetail("We don't expect this code path to be executed.")));
}

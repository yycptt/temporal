// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination activity_state_replicator_mock.go

package ndc

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

type (
	HSMStateReplicator interface {
		SyncHSMState(
			ctx context.Context,
			request *hsm.SyncRequest,
		) error
	}

	HSMStateReplicatorImpl struct {
		shardContext  shard.Context
		workflowCache wcache.Cache
		logger        log.Logger
	}
)

func NewHSMStateReplicator(
	shardContext shard.Context,
	workflowCache wcache.Cache,
	logger log.Logger,
) *HSMStateReplicatorImpl {

	return &HSMStateReplicatorImpl{
		shardContext:  shardContext,
		workflowCache: workflowCache,
		logger:        log.With(logger, tag.ComponentHSMStateReplicator),
	}
}

func (r *HSMStateReplicatorImpl) SyncHSMState(
	ctx context.Context,
	request *hsm.SyncRequest,
) (retError error) {
	namespaceID := namespace.ID(request.WorkflowKey.GetNamespaceID())
	execution := &commonpb.WorkflowExecution{
		WorkflowId: request.WorkflowKey.GetWorkflowID(),
		RunId:      request.WorkflowKey.GetRunID(),
	}

	firstItem, err := versionhistory.GetFirstVersionHistoryItem(request.EventVersionHistory)
	if err != nil {
		return err
	}
	lastItem, err := versionhistory.GetLastVersionHistoryItem(request.EventVersionHistory)
	if err != nil {
		return err
	}

	workflowContext, release, err := r.workflowCache.GetOrCreateWorkflowExecution(
		ctx,
		r.shardContext,
		namespaceID,
		execution,
		workflow.LockPriorityHigh,
	)
	if err != nil {
		// for get workflow execution context, with valid run id
		// err will not be of type EntityNotExistsError
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := workflowContext.LoadMutableState(ctx, r.shardContext)
	if err != nil {
		if _, isNotFound := err.(*serviceerror.NotFound); isNotFound {
			// TODO: verify that Resender will handle workflow not found case and drop the task
			return serviceerrors.NewRetryReplication(
				"sync HSM state encountered workflow not found",
				namespaceID.String(),
				execution.GetWorkflowId(),
				execution.GetRunId(),
				firstItem.EventId,
				firstItem.Version,
				lastItem.EventId,
				lastItem.Version,
			)
		}
		return err
	}

	synced, err := r.syncHSMNode(mutableState, request)
	if !synced {
		return nil
	}

	state, _ := mutableState.GetWorkflowStateStatus()
	if state == enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
		return workflowContext.SubmitClosedWorkflowSnapshot(
			ctx,
			r.shardContext,
			workflow.TransactionPolicyPassive,
		)
	}

	updateMode := persistence.UpdateWorkflowModeUpdateCurrent
	if state != enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE {
		updateMode = persistence.UpdateWorkflowModeBypassCurrent
	}

	return workflowContext.UpdateWorkflowExecutionWithNew(
		ctx,
		r.shardContext,
		updateMode,
		nil, // no new workflow
		nil, // no new workflow
		workflow.TransactionPolicyPassive,
		nil,
	)
}

func (r *HSMStateReplicatorImpl) syncHSMNode(
	mutableState workflow.MutableState,
	request *hsm.SyncRequest,
) (bool, error) {

	shouldSync, err := r.testVersionHistory(mutableState, request.EventVersionHistory)
	if err != nil || !shouldSync {
		return shouldSync, err
	}

	currentHSM := mutableState.HSM()

	// we don't care about the root here which is the entire mutable state
	incomingHSM, err := hsm.NewRoot(
		r.shardContext.StateMachineRegistry(),
		workflow.StateMachineType.ID,
		mutableState,
		request.StateMachineNode.Children,
		mutableState,
	)

	synced := false
	incomingHSM.Walk(func(incomingNode *hsm.Node) error {
		if incomingNode.Parent == nil {
			// skip root which is the entire mutable state
			return nil
		}

		incomingNodePath := incomingNode.Path()
		currentNode, err := currentHSM.Child(incomingNodePath)
		if err != nil {
			// we already done history resend before, node should always be found
			return err
		}

		// TODO: consider moving the failover version comparison logic into node.Sync() as well

		if incomingNode.InitialNamespaceFailoverVersion() != currentNode.InitialNamespaceFailoverVersion() {
			// this should only happen when we choose to continne to sync state when history event branch diverges
			// in testVersionHistory
			return nil
		}

		if incomingNode.CurrentNamespaceFailoverVersion() < currentNode.CurrentNamespaceFailoverVersion() {
			// current version is newer
			return nil
		}

		shouldSync := false
		if incomingNode.CurrentNamespaceFailoverVersion() > currentNode.CurrentNamespaceFailoverVersion() {
			shouldSync = true
		} else {
			// we have to rely on component to tell us which state is newer
			compareResult, err := currentNode.CompareData(incomingNode)
			if err != nil {
				return err
			}

			shouldSync = compareResult < 0
		}

		if !shouldSync {
			return nil
		}

		synced = true
		return currentNode.Sync(incomingNode)
	})

	return synced, nil
}

func (r *HSMStateReplicatorImpl) testVersionHistory(
	mutableState workflow.MutableState,
	incomingVersionHistory *historyspb.VersionHistory,
) (bool, error) {
	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(
		mutableState.GetExecutionInfo().GetVersionHistories(),
	)
	if err != nil {
		return false, err
	}

	lastLocalItem, err := versionhistory.GetLastVersionHistoryItem(currentVersionHistory)
	if err != nil {
		return false, err
	}

	lastIncomingItem, err := versionhistory.GetLastVersionHistoryItem(incomingVersionHistory)
	if err != nil {
		return false, err
	}

	lcaItem, err := versionhistory.FindLCAVersionHistoryItem(currentVersionHistory, incomingVersionHistory)
	if err != nil {
		return false, err
	}

	if versionhistory.IsLCAVersionHistoryItemAppendable(currentVersionHistory, lcaItem) ||
		versionhistory.IsLCAVersionHistoryItemAppendable(incomingVersionHistory, lcaItem) {
		// not diverged, resend any missing events
		if versionhistory.CompareVersionHistoryItem(lastLocalItem, lastIncomingItem) >= 0 {
			return true, nil
		}

		workflowKey := mutableState.GetWorkflowKey()
		return false, serviceerrors.NewRetryReplication(
			"sync HSM state encountered missing events",
			workflowKey.NamespaceID,
			workflowKey.WorkflowID,
			workflowKey.RunID,
			lastLocalItem.EventId,
			lastLocalItem.Version,
			lastIncomingItem.EventId, // TODO: shall we just resend all history events?
			lastIncomingItem.Version,
		)
	}

	// event version history has diverged
	if lastIncomingItem.GetVersion() < lastLocalItem.GetVersion() {
		// TODO: it's actually interesting if we want to continue to sync state in this case
		// just return true here if needed,
		// the actual sync logic can tell which state is newer and do the right thing
		return false, nil
	}

	if lastIncomingItem.GetVersion() > lastLocalItem.GetVersion() {
		workflowKey := mutableState.GetWorkflowKey()
		return false, serviceerrors.NewRetryReplication(
			resendHigherVersionMessage,
			workflowKey.NamespaceID,
			workflowKey.WorkflowID,
			workflowKey.RunID,
			lcaItem.GetEventId(),
			lcaItem.GetVersion(),
			lastIncomingItem.EventId, // TODO: shall we just resend all history events?
			lastIncomingItem.Version,
		)
	}

	return true, nil
}

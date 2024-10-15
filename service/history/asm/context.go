package asm

import (
	"context"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/history/workflow/update"
)

var _ workflow.Context = (*ContextImpl)(nil)

type (
	ContextImpl struct {
		workflow.Context
		registry Registry

		MutableState MutableState
		logger       log.Logger
	}
)

func NewContext(
	config *configs.Config,
	asmKey Key,
	logger log.Logger,
	throttledLogger log.ThrottledLogger,
	metricsHandler metrics.Handler,
	registry Registry,
) *ContextImpl {
	return &ContextImpl{
		Context: workflow.NewContext(
			config,
			asmKey,
			logger,
			throttledLogger,
			metricsHandler,
		),
		registry:     registry,
		MutableState: nil,
		logger:       logger,
	}
}

func (c *ContextImpl) GetASMKey() Key {
	return c.Context.GetWorkflowKey()
}

func (c *ContextImpl) GetMutableState(
	ctx context.Context,
	shardContext shard.Context,
) (MutableState, error) {

	if c.MutableState == nil {
		if err := c.initMutableState(ctx, shardContext); err != nil {
			return nil, err
		}
	}

	namespaceEntry, err := shardContext.GetNamespaceRegistry().GetNamespaceByID(
		namespace.ID(c.GetASMKey().NamespaceID),
	)
	if err != nil {
		return nil, err
	}

	shouldFlush, err := c.MutableState.StartTransaction(namespaceEntry)
	if err != nil {
		return nil, err
	}
	if shouldFlush {
		panic("asm mutable state should never flush when starting transaction")
	}

	return c.MutableState, nil
}

func (c *ContextImpl) initMutableState(
	ctx context.Context,
	shardContext shard.Context,
) error {
	asmKey := c.GetASMKey()
	namespaceEntry, err := shardContext.GetNamespaceRegistry().GetNamespaceByID(
		namespace.ID(asmKey.NamespaceID),
	)
	if err != nil {
		return err
	}

	response, err := workflow.GetWorkflowExecutionFromPersistence(
		ctx,
		shardContext,
		&persistence.GetWorkflowExecutionRequest{
			ShardID:     shardContext.GetShardID(),
			NamespaceID: asmKey.NamespaceID,
			WorkflowID:  asmKey.WorkflowID,
			RunID:       asmKey.RunID,
		},
	)
	if err != nil {
		return err
	}

	c.MutableState, err = NewMutableStateFromDB(
		shardContext,
		c.registry,
		shardContext.GetEventsCache(),
		c.logger,
		namespaceEntry,
		response.State,
		response.DBRecordVersion,
	)
	if err != nil {
		c.MutableState = nil
		return err
	}
	return nil
}

func (c *ContextImpl) Clear() {
	// TODO chasm: clear more stuff
	c.Context.Clear()
}

// Lock/Unlock no override

func (c *ContextImpl) IsDirty() bool {
	// TODO chasm: check new component fields
	return c.Context.IsDirty()
}

//////////////////////////////////////////////////////////////////
// Following methods should not be used by chasm engine for now //
//////////////////////////////////////////////////////////////////

func (c *ContextImpl) LoadMutableState(
	ctx context.Context,
	shardContext shard.Context,
) (workflow.MutableState, error) {
	panic("should not be used by chasm engine")
}

func (c *ContextImpl) RefreshTasks(
	ctx context.Context,
	shardContext shard.Context,
) error {
	panic("should not be used by chasm engine")
}

func (c *ContextImpl) ReapplyEvents(
	ctx context.Context,
	shardContext shard.Context,
	eventBatches []*persistence.WorkflowEvents,
) error {
	panic("should not be used by chasm engine")
}

func (c *ContextImpl) PersistWorkflowEvents(
	ctx context.Context,
	shardContext shard.Context,
	workflowEventsSlice ...*persistence.WorkflowEvents,
) (int64, error) {
	panic("should not be used by chasm engine")
}

// func (c *ContextImpl) CreateWorkflowExecution(
// 	ctx context.Context,
// 	shardContext shard.Context,
// 	createMode persistence.CreateWorkflowMode,
// 	prevRunID string,
// 	prevLastWriteVersion int64,
// 	newMutableState workflow.MutableState,
// 	newWorkflow *persistence.WorkflowSnapshot,
// 	newWorkflowEvents []*persistence.WorkflowEvents,
// ) error {
// 	panic("should not be used by chasm engine")
// }

func (c *ContextImpl) ConflictResolveWorkflowExecution(
	ctx context.Context,
	shardContext shard.Context,
	conflictResolveMode persistence.ConflictResolveWorkflowMode,
	resetMutableState workflow.MutableState,
	newContext workflow.Context,
	newMutableState workflow.MutableState,
	currentContext workflow.Context,
	currentMutableState workflow.MutableState,
	resetWorkflowTransactionPolicy workflow.TransactionPolicy,
	newWorkflowTransactionPolicy *workflow.TransactionPolicy,
	currentTransactionPolicy *workflow.TransactionPolicy,
) error {
	panic("should not be used by chasm engine")
}

// func (c *ContextImpl) UpdateWorkflowExecutionAsActive(
//
//	ctx context.Context,
//	shardContext shard.Context,
//
//	) error {
//		panic("should not be used by chasm engine")
//	}
func (c *ContextImpl) UpdateWorkflowExecutionWithNewAsActive(
	ctx context.Context,
	shardContext shard.Context,
	newContext workflow.Context,
	newMutableState workflow.MutableState,
) error {
	panic("should not be used by chasm engine")
}
func (c *ContextImpl) UpdateWorkflowExecutionAsPassive(
	ctx context.Context,
	shardContext shard.Context,
) error {
	panic("should not be used by chasm engine")
}
func (c *ContextImpl) UpdateWorkflowExecutionWithNewAsPassive(
	ctx context.Context,
	shardContext shard.Context,
	newContext workflow.Context,
	newMutableState workflow.MutableState,
) error {
	panic("should not be used by chasm engine")
}
func (c *ContextImpl) UpdateWorkflowExecutionWithNew(
	ctx context.Context,
	shardContext shard.Context,
	updateMode persistence.UpdateWorkflowMode,
	newContext workflow.Context,
	newMutableState workflow.MutableState,
	updateWorkflowTransactionPolicy workflow.TransactionPolicy,
	newWorkflowTransactionPolicy *workflow.TransactionPolicy,
) error {
	panic("should not be used by chasm engine")
}
func (c *ContextImpl) SetWorkflowExecution(
	ctx context.Context,
	shardContext shard.Context,
) error {
	panic("should not be used by chasm engine")
}
func (c *ContextImpl) SubmitClosedWorkflowSnapshot(
	ctx context.Context,
	shardContext shard.Context,
	transactionPolicy workflow.TransactionPolicy,
) error {
	panic("should not be used by chasm engine")
}
func (c *ContextImpl) UpdateRegistry(ctx context.Context, ms workflow.MutableState) update.Registry {
	panic("should not be used by chasm engine")
}

package engine

import (
	"context"
	"errors"

	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/chasm"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/history/workflow/cache"
)

// TODO: add panic wrapper

type engineImpl struct {
	entityCache     cache.Cache
	shardController shard.Controller
	registry        *chasm.Registry
}

func NewEngineImpl(
	entityCache cache.Cache,
) *engineImpl {
	return &engineImpl{
		entityCache: entityCache,
	}
}

func (e *engineImpl) updateComponent(
	ctx context.Context,
	ref chasm.ComponentRef,
	updateFn func(chasm.MutableContext, chasm.Component) error,
	opts ...chasm.TransitionOption,
) (updatedRef chasm.ComponentRef, retError error) {
	/*
		1. find the entity from the cache.
		2. load mutable state and init the component tree
		3. staleness check based on the ref
		4. find the component based on the path
		5. populate the component struct
		6. invoke update func
		7. scan the entire tree and calculate mutation
		8. persist the mutation

	*/

	// step 1 2 and 3
	shardContext, entityLease, err := e.getEntityLease(ctx, ref)
	if err != nil {
		return chasm.ComponentRef{}, err
	}
	defer func() {
		entityLease.GetReleaseFn()(retError)
	}()

	mutableState := entityLease.GetMutableState()
	componentTree := mutableState.ChasmTree()

	mutableContext := chasm.NewMutableContextImpl(ctx, componentTree)

	// step 4 and 5
	component, err := componentTree.GetComponentByRef(mutableContext, ref)
	if err != nil {
		return chasm.ComponentRef{}, err
	}

	// TODO: evaluate access rules

	// step 6
	if err := updateFn(mutableContext, component); err != nil {
		return chasm.ComponentRef{}, err
	}

	// step 7 and 8
	if err := entityLease.GetContext().UpdateWorkflowExecutionAsActive(
		ctx,
		shardContext,
	); err != nil {
		return chasm.ComponentRef{}, err
	}

	newRef, ok := mutableContext.Ref(component)
	if !ok {
		panic("component ref not found in the new compoenent tree")
	}

	return newRef, nil
}

func (e *engineImpl) getEntityLease(
	ctx context.Context,
	ref chasm.ComponentRef,
) (shard.Context, api.WorkflowLease, error) {
	shardContext, err := e.getShardContext(ref)
	if err != nil {
		return nil, nil, err
	}

	consistencyChecker := api.NewWorkflowConsistencyChecker(
		shardContext,
		e.entityCache,
	)

	var staleReferenceErr error
	// TODO: this loads the entire mutable state
	// need to refactor the code to support partial loading later
	entityLease, err := consistencyChecker.GetWorkflowLeaseWithConsistencyCheck(
		ctx,
		nil,
		func(mutableState workflow.MutableState) bool {
			// TODO: validate root component name matches
			executionInfo := mutableState.GetExecutionInfo()
			if err := workflow.TransitionHistoryStalenessCheck(
				executionInfo.GetTransitionHistory(),
				ref.EntityLastUpdateVT,
			); err != nil && errors.Is(err, consts.ErrStaleState) {
				return false

			}

			// reference might be stale
			// no need to reload, but request should be failed
			staleReferenceErr = err

			return true
		},
		definition.NewWorkflowKey(
			ref.EntityKey.NamespaceID,
			ref.EntityKey.BusinessID,
			ref.EntityKey.EntityID,
		),
		// TODO: we need to figure out the priority from the context.
		// api -> high,
		// task processing -> low
		locks.PriorityHigh,
	)
	if err == nil {
		entityLease.GetReleaseFn()(nil)
		err = staleReferenceErr
	}

	return shardContext, entityLease, err
}

func (e *engineImpl) getShardContext(
	ref chasm.ComponentRef,
) (shard.Context, error) {
	// TODO: use shardingFn of the component
	// e.registry.getComponentByName(ref.rootComponentName).entityOptions.shardingFn(ref.EntityKey)
	return e.shardController.GetShardByNamespaceWorkflow(
		namespace.ID(ref.EntityKey.NamespaceID),
		ref.EntityKey.BusinessID,
	)
}

package asm

import (
	"context"
	"errors"
	"strings"

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	workflowcache "go.temporal.io/server/service/history/workflow/cache"
)

var (
	ErrASMTypeNotRegistered      = serviceerror.NewInternal("ASM type not registered")
	ErrOutboundTimerNotSupported = serviceerror.NewUnimplemented("Outbound timer queue is not supported")
	ErrASMInstanceNotFound       = serviceerror.NewNotFound("ASM instance not found")
)

type (
	EngineImpl struct {
		shardController  shard.Controller
		asmRegistry      Registry
		namespaceRegisty namespace.Registry

		asmCache    workflowcache.Cache
		eventsCache events.Cache
	}
)

func NewEngine(
	shardController shard.Controller,
	asmRegistry Registry,
	asmCache workflowcache.Cache,
	namespaceRegisty namespace.Registry,
	eventsCache events.Cache,
) *EngineImpl {
	return &EngineImpl{
		shardController:  shardController,
		asmRegistry:      asmRegistry,
		namespaceRegisty: namespaceRegisty,
		eventsCache:      eventsCache,
	}
}

func (e *EngineImpl) New(
	ctx context.Context,
	asmType string,
	key Key,
	transitionFn TransitionFn,
) (Key, error) {

	if len(key.RunID) != 0 {
		return Key{}, serviceerror.NewInternal("RunID must be empty")
	}

	namespaceEntry, err := e.namespaceRegisty.GetNamespaceByID(namespace.ID(key.NamespaceID))
	if err != nil {
		return Key{}, err
	}

	// TODO: this routing part needs to be customized.
	shardContext, err := e.shardController.GetShardByNamespaceWorkflow(
		namespace.ID(key.NamespaceID),
		key.WorkflowID,
	)
	if err != nil {
		return Key{}, err
	}

	asm, ok := e.asmRegistry.Get(asmType)
	if !ok {
		return Key{}, ErrASMTypeNotRegistered
	}

	key.RunID = uuid.New()

	mutableState := NewMutableState(
		shardContext,
		e.asmRegistry,
		e.eventsCache,
		shardContext.GetLogger(),
		namespaceEntry,
		key.WorkflowID,
		key.RunID,
		shardContext.GetTimeSource().Now(),
	)
	asmContext := NewContext(
		shardContext.GetConfig(),
		key,
		shardContext.GetLogger(),
		shardContext.GetThrottledLogger(),
		shardContext.GetMetricsHandler(),
		e.asmRegistry,
	)

	asmInstance := asm.New(
		newInstanceBackend(e.asmRegistry, mutableState, ""),
	)

	if err := transitionFn(asmInstance); err != nil {
		return Key{}, err
	}

	snapshot, events, err := mutableState.CloseTransactionAsSnapshot(workflow.TransactionPolicyActive)
	if err != nil {
		return Key{}, err
	}

	if err := asmContext.CreateWorkflowExecution(
		ctx,
		shardContext,
		persistence.CreateWorkflowModeBrandNew,
		"",
		0,
		mutableState,
		snapshot,
		events,
	); err != nil {
		return Key{}, err
	}

	// TODO: needs to handle ID reuse

	return key, nil
}

func (e *EngineImpl) Update(
	ctx context.Context,
	ref InstanceRef,
	transitionFn TransitionFn,
) (_ ConsistenceToken, retError error) {

	shardContext, err := e.shardController.GetShardByNamespaceWorkflow(
		namespace.ID(ref.Key.NamespaceID),
		ref.Key.WorkflowID,
	)
	if err != nil {
		return nil, err
	}

	wfContext, release, err := e.asmCache.GetOrCreateWorkflowExecution(
		ctx,
		shardContext,
		namespace.ID(ref.Key.NamespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: ref.Key.WorkflowID,
			RunId:      ref.Key.RunID,
		},
		locks.PriorityHigh, // TODO: determine this based on caller info in ctx
	)
	if err != nil {
		return nil, err
	}
	defer func() { release(retError) }()

	asmContext := wfContext.(*ContextImpl)

	mutableState, err := asmContext.GetMutableState(ctx, shardContext)
	if err != nil {
		return nil, err
	}

	// validate consistence token
	err = mutableState.ValidateConsistenceToken(ref.ConsistenceToken)
	if err != nil && errors.Is(err, consts.ErrStaleState) {
		asmContext.Clear()
		mutableState, err = asmContext.GetMutableState(ctx, shardContext)
		if err != nil {
			return nil, err
		}

		err = mutableState.ValidateConsistenceToken(ref.ConsistenceToken)
	}
	if err != nil {
		return nil, err
	}

	// validate ref path & first versioned transition
	asmInstance, err := mutableState.GetASMInstance(
		ref.instancePath,
		ref.initialVersionedTransition,
	)
	if err != nil {
		return nil, err
	}

	asm, ok := e.asmRegistry.Get(asmInstance.Type())
	if !ok {
		return nil, ErrASMTypeNotRegistered
	}

	// pre hook
	keyList := strings.Split(ref.instancePath, "/")
	ancestors := make([]ancenstorInfo, 0, len(keyList)+1)
	for currentPath, i := "", 0; currentPath != ref.instancePath; currentPath, i = currentPath+keyList[i], i+1 {
		ancenstorInstance, err := mutableState.GetASMInstance(currentPath, nil)
		if err != nil {
			return nil, err
		}
		ancestorASM, ok := e.asmRegistry.Get(ancenstorInstance.Type())
		if !ok {
			return nil, ErrASMTypeNotRegistered
		}
		ancestors = append(ancestors, ancenstorInfo{
			asm:      ancestorASM,
			instance: ancenstorInstance,
		})
	}
	ancestors = append(ancestors, ancenstorInfo{
		asm:      asm,
		instance: asmInstance,
	})

	for i := 0; i < len(ancestors)-1; i++ {
		asm := ancestors[i].asm
		childASMType := ancestors[i+1].instance.Type()
		if hooks, ok := asm.Hooks()[childASMType]; ok {
			if hooks.PreHook != nil {
				if err := hooks.PreHook(ctx, ancestors[i].instance); err != nil {
					return nil, err
				}
			}
		}
	}

	if err := transitionFn(asmInstance); err != nil {
		return nil, err
	}

	// post hook
	// TODO: only invoke when asmInstance is dirty

	for i := len(ancestors) - 2; i >= 0; i-- {
		asm := ancestors[i].asm
		childASMType := ancestors[i+1].instance.Type()
		if hooks, ok := asm.Hooks()[childASMType]; ok {
			if hooks.PostHook != nil {
				if err := hooks.PostHook(ctx, ancestors[i].instance); err != nil {
					return nil, err
				}
			}
		}
	}

	if err := asmContext.UpdateWorkflowExecutionAsActive(
		ctx,
		shardContext,
	); err != nil {
		return nil, err
	}

	return mutableState.ConsistencyToken()
}

type (
	ancenstorInfo struct {
		asm      ASM
		instance Instance
	}
)

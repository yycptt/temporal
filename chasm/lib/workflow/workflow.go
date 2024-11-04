package workflow

import (
	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/eventstore"
)

type Library struct {
}

func (Library) Components() (comps []chasm.ComponentType) {
	comps = append(comps,
		chasm.NewComponentType[Workflow](chasm.ComponentTypeOptions{}),
		chasm.NewComponentType[Memo](chasm.ComponentTypeOptions{}),
		chasm.NewComponentType[EventStore](chasm.ComponentTypeOptions{}),
	)
	return
}

// one want to run & one dont after parent close

func (Library) Tasks() (defs []chasm.TaskType) {
	panic("unimplemented")
}

func (l Library) Services() (defs []*nexus.Service) {
	service := nexus.NewService("workflow")
	_ = service.Register(
		&executeOperation{},
		chasm.NewWriteOperation("CompleteTask", Workflow.CompleteTask),
	)
	defs = append(defs, service)
	return
}

var _ chasm.Library = Library{}

// TODO: Some proto struct.
type State struct {
}

type Memo struct {
	State *commonpb.Payload
}

type Workflow struct {
	State *State // proto.Message

	EventStore *chasm.Ptr[eventstore.EventStore]
	Memo       *chasm.Ptr[Memo]
	Activities *chasm.Map[activity.Activity]
}

func (w Workflow) CompleteTask(ctx chasm.WriteContext, request *CompleteTaskRequest) (*CompleteTaskResponse, error) {
	events := w.EventStore.MustGet()

	act, err := activity.NewActivity(ctx, &activity.ActivityOptions{
		Event:      &activity.ScheduledEvent{},
		EventStore: events,
	})
	if err != nil {
		return nil, err
	}
	w.Activities.Set("some-id", act)
	return &CompleteTaskResponse{}, nil
}

type CallerInfo struct {
	CallerType int // api (operation) or task
	Request    any

	// TargetComponent any
}

func (w Workflow) Intercept(
	ctx chasm.WriteContext,
	info CallerInfo,
	next func() error, // continue the transition
) error {
	// interceptor should be invoked within a lock
	// otherwise the result is useless. This is the same as
	// task Validate method.
	// but what it really want to intercept is not the method access
	// on the child component, but the operation or task (the context
	// of the method access), which spans
	// multiple lock unlocks.
	// this is easy for task processing, framework can embed info into the context
	// we should do the same for api calls.

	// pre

	next()

	// post

	// how to tell say activity is started/completed?
	// well technically you can, because you can see it's targeting an activity
	return nil
}

type workflowOptions struct {
}

// not required.
func (*workflowOptions) TypeName() string {
	panic("unimplemented")
}

func (*workflowOptions) Storage() chasm.StorageOptions {
	return chasm.StorageOptionsPersistent{}
}

type EventStore struct {
	State *struct{ Exclude []string }

	Events *chasm.Map[eventstore.Event]
}

func (s EventStore) Add(ctx chasm.WriteContext, event eventstore.Event) {
	// Here there'll be a type switch to record workflow events.
	// TODO: not implemented.
}

func (s EventStore) Get(ctx chasm.ReadContext, id int64) eventstore.Event {
	panic("todo")
}

type embeddedEventStoreOptions struct {
}

func (*embeddedEventStoreOptions) Storage() chasm.StorageOptions {
	return chasm.StorageOptionsHistory{}
}

func NewWorkflow(ctx chasm.WriteContext, request *ExecuteRequest) (Workflow, error) {
	w := chasm.NewComponent[Workflow](ctx)
	// TODO: Attach callback state machines from options.
	w.State = &State{}
	memo := chasm.NewComponent[Memo](ctx)
	memo.State = nil // TODO
	w.EventStore.Set(chasm.NewComponent[EventStore](ctx))
	w.Memo.Set(memo)
	// TODO: Add workflow task...
	return w, nil
}

// This will have codegen.
type ExecuteRequest struct {
	NamespaceID, ID string
}

type ExecuteResponse struct {
}

type executeOperation struct {
	nexus.UnimplementedOperation[*ExecuteRequest, *ExecuteResponse]
}

func (*executeOperation) Cancel(chasm.EngineContext, string, nexus.CancelOperationOptions) error {
	panic("unimplemented")
}

func (*executeOperation) GetInfo(chasm.EngineContext, string, nexus.GetOperationInfoOptions) (*nexus.OperationInfo, error) {
	panic("unimplemented")
}

func (*executeOperation) GetResult(chasm.EngineContext, string, nexus.GetOperationResultOptions) (*ExecuteResponse, error) {
	panic("unimplemented")
}

// Name implements nexus.Operation.
func (*executeOperation) Name() string {
	return "Execute"
}

// Start implements nexus.Operation.
func (o *executeOperation) Start(ctx chasm.EngineContext, request *ExecuteRequest, opts nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[*ExecuteResponse], error) {
	key := chasm.InstanceKey{NamespaceID: request.NamespaceID, BusinessID: request.ID}
	err := chasm.CreateInstance(ctx, key, NewWorkflow, request, chasm.ComponentOptions{
		Storage: chasm.StorageOptionsPersistent{},
	})
	if err != nil {
		return nil, err
	}

	// TODO: How can this return run ID and first workflow task?
	return &nexus.HandlerStartOperationResultAsync{
		OperationID: "TODO",
	}, nil
}

// This will have codegen.
type CompleteTaskRequest struct {
	Ref chasm.Ref
}

type CompleteTaskResponse struct {
}

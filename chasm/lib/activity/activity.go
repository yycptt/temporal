package activity

import (
	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/eventstore"
)

type Library struct {
}

func (Library) Components() (comps []chasm.ComponentType) {
	comps = append(comps, chasm.NewComponentType[Activity](chasm.ComponentTypeOptions{}))
	return
}

// Components implements chasm.Library.
func (Library) Tasks() (defs []chasm.TaskType) {
	defs = append(defs, chasm.NewTaskType[ScheduleTask](&scheduleTaskOptions{}))
	return
}

func (Library) Services() (defs []*nexus.Service) {
	service := nexus.NewService("activity")
	_ = service.Register(recordTaskStartedOperation)
	defs = append(defs, service)
	return
}

var _ chasm.Library = Library{}

// TODO: Some proto enum.
type Status int

const (
	StatusScheduled = Status(iota)
	StatusStarted
)

// TODO: Some proto struct.
type State struct {
	Status Status
}

type Activity struct {
	State *State

	EventStore *chasm.Ptr[eventstore.EventStore]
}

type ScheduledEvent struct {
}

func (ScheduledEvent) ID() int64 {
	return 0
}

type ActivityOptions struct {
	EventStore eventstore.EventStore
	Event      *ScheduledEvent
}

func NewActivity(ctx chasm.WriteContext, options *ActivityOptions) (Activity, error) {
	activity := chasm.NewComponent[Activity](ctx)
	activity.State = &State{
		Status: StatusScheduled,
	}
	var s eventstore.EventStore
	if options.EventStore == nil {
		s = options.EventStore
	} else {
		es := chasm.NewComponent[eventstore.EmbeddedEventStore](ctx)
		es.State = &struct{ Exclude []string }{Exclude: []string{"ActivityStartedEvent"}}
		s = es
	}
	activity.EventStore.Set(s)
	s.Add(ctx, options.Event)
	return activity, nil
}

func (a Activity) RecordTaskStarted(ctx chasm.WriteContext, request *RecordTaskStartedRequest) (chasm.NoValue, error) {
	// Transition only from Scheduled and other validations.
	a.State.Status = StatusStarted
	a.EventStore.MustGet().Get(ctx, 0) // TODO: get by token.
	return nil, nil
}

func (a Activity) loadRequest(ctx chasm.ReadContext, task ScheduleTask) (request *matchingservice.AddActivityTaskRequest, err error) {
	// TODO: Populate with data from state machine.
	return &matchingservice.AddActivityTaskRequest{}, nil
}

type ScheduleTask struct{}

func (ScheduleTask) Attributes() chasm.TaskAttributes {
	return chasm.TaskAttributes{
		Deadline: chasm.Immediate,
	}
}

func (ScheduleTask) Destination() string {
	return ""
}

var _ chasm.Task = ScheduleTask{}

type scheduleTaskOptions struct {
	matchingClient matchingservice.MatchingServiceClient
}

// Type implements chasm.Task.
func (*scheduleTaskOptions) Validate(ctx chasm.ReadContext, comp chasm.Component, task ScheduleTask) error {
	if ctx.Instance().State != chasm.InstanceStateRunning {
		return chasm.ErrStaleReference
	}
	if comp.(Activity).State.Status != StatusScheduled {
		return chasm.ErrStaleReference
	}
	return nil
}

func (d *scheduleTaskOptions) Execute(ctx chasm.EngineContext, ref chasm.Ref, task ScheduleTask) error {
	request, err := chasm.Execute(ctx, ref, Activity.loadRequest, task)
	if err != nil {
		return err
	}
	_, err = d.matchingClient.AddActivityTask(ctx, request)
	return err
}

// This will have codegen.
type RecordTaskStartedRequest struct {
	Ref chasm.Ref
}

type RecordTaskStartedResponse struct {
}

var recordTaskStartedOperation = chasm.NewSyncOperation(
	"RecordTaskStarted",
	func(
		ctx chasm.EngineContext,
		request *RecordTaskStartedRequest,
		options nexus.StartOperationOptions,
	) (*RecordTaskStartedResponse, error) {

		_, err := chasm.Execute(ctx, request.Ref, Activity.RecordTaskStarted, request)
		if err != nil {
			return nil, err
		}

		return &RecordTaskStartedResponse{}, nil
	},
)

// This will have codegen.
type StartRequest struct {
	NamespaceID, ID string
}

type StartResponse struct {
}

var startOperation = chasm.NewSyncOperation("Start", func(ctx chasm.EngineContext, request *StartRequest, options nexus.StartOperationOptions) (*StartResponse, error) {
	key := chasm.InstanceKey{NamespaceID: request.NamespaceID, BusinessID: request.ID}
	initOpts := &ActivityOptions{
		Event: &ScheduledEvent{},
	}
	err := chasm.CreateInstance(ctx, key, NewActivity, initOpts, chasm.ComponentOptions{
		Storage: chasm.StorageOptionsPersistent{},
	})
	if err != nil {
		return nil, err
	}

	return &StartResponse{}, nil
})

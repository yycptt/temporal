package activity

import (
	"time"

	"go.temporal.io/api/common/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencepb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/service/history/chasm"
	"go.temporal.io/server/service/history/chasm/components/callback"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var _ Activity = (*ActivityImpl)(nil)

type (
	ActivityImpl struct {
		// In V1, we will only support only one non-chasm.XXX field in the struct.
		// and that field must be a proto.Message.
		//
		// Framework will try to recognize the type and do serialization/deserialization
		// proto.Message is recommended so the component get compatibility if state definition changes
		// At the end of a transaction, framework can compare old & new states,
		// figure out which fields are dirty, and send a proto.Message with only those dirty fields
		// set to persistence.
		State persistencepb.ActivityInfo // proto.Message

		// One nice thing about this approach is that it defines the structure of the component tree
		// statically. This solve's our partial load problem as now we can look at this structure
		// definition and figure what paths to load.
		// - If the field is a chasm.ComponentMap, then caller needs to specify a path
		// - If the componentField is an interface, then check **all** registered components
		// that implements that interface.
		//
		// Use field tags to control the default loading behavior
		// Can also support field name tag, so the fields can be renamed
		Input  *chasm.DataField[*common.Payload] // lazy by default
		Output *chasm.DataField[*common.Payload] // lazy by default

		Callback *chasm.DataField[*callback.Callback]
	}
)

func NewScheduledActivity(
	chasmContext chasm.MutableContext,
	params *NewActivityRequest,
) (*ActivityImpl, *NewActivityResponse, error) {
	// after return framework will use reflection to analyze
	// and understand the structure of the component tree
	activity := &ActivityImpl{
		// State: persistencepb.ActivityInfo{},
	}
	_, err := activity.Schedule(chasmContext, &ScheduleRequest{
		Input: params.Input,
	})
	if err != nil {
		return nil, &NewActivityResponse{}, err
	}

	return activity, &NewActivityResponse{}, nil
}

func (i *ActivityImpl) RunningState() chasm.ComponentState {
	panic("not implemented")
}

func (i *ActivityImpl) Schedule(
	chasmContext chasm.MutableContext,
	req *ScheduleRequest,
) (*ScheduleResponse, error) {
	// also validate current state etc.

	i.UpdateStateToScheduled(chasmContext, req)

	for _, t := range i.GenerateScheduledTasks(chasmContext) {
		if err := chasmContext.AddTask(chasm.Task{
			Attributes: t.Attributes,
			Data:       t.Data,
		}); err != nil {
			return nil, err
		}
	}

	return &ScheduleResponse{}, nil
}

func (i *ActivityImpl) UpdateStateToScheduled(
	chasmContext chasm.MutableContext,
	req *ScheduleRequest,
) {
	i.State.ScheduledTime = timestamppb.New(chasmContext.Now())
	i.Input = chasm.NewDataField(chasmContext, &common.Payload{
		Data: req.Input,
	})
}

func (i *ActivityImpl) GenerateScheduledTasks(
	chasmContext chasm.Context,
) []chasm.Task {
	return []chasm.Task{
		{
			Attributes: chasm.TaskAttributes{}, // immediate task
			Data:       DispatchTask{},
		},
		{
			Attributes: chasm.TaskAttributes{
				ScheduledTime: chasmContext.Now().Add(10 * time.Second),
			},
			Data: TimeoutTask{
				TimeoutType: TimeoutTypeScheduleToStart,
			},
		},
	}
}

func (i *ActivityImpl) LoadDispatchInfo(
	chasmContext chasm.Context,
	_ struct{},
) (*matchingservice.AddActivityTaskRequest, error) {
	// load dispatch info
	return &matchingservice.AddActivityTaskRequest{}, nil
}

func (i *ActivityImpl) RecordStarted(
	chasmContext chasm.MutableContext,
	req *RecordStartedRequest,
) (*RecordStartedResponse, error) {

	i.UpdateStateToStarted(chasmContext, req)

	payload, err := i.Input.Get(chasmContext)
	if err != nil {
		return nil, err
	}

	for _, t := range i.GenerateStartedTasks(chasmContext) {
		if err := chasmContext.AddTask(chasm.Task{
			Attributes: t.Attributes,
			Data:       t.Data,
		}); err != nil {
			return nil, err
		}
	}

	return &RecordStartedResponse{
		Input: payload.Data,
	}, nil
}

func (i *ActivityImpl) UpdateStateToStarted(
	chasmContext chasm.MutableContext,
	req *RecordStartedRequest,
) {
	// only this field will be updated
	i.State.StartedTime = timestamppb.New(chasmContext.Now())
	// update other states
}

func (i *ActivityImpl) GenerateStartedTasks(
	chasmContext chasm.Context,
) []chasm.Task {
	return []chasm.Task{
		{
			Attributes: chasm.TaskAttributes{
				ScheduledTime: chasmContext.Now().Add(10 * time.Second),
			},
			Data: TimeoutTask{
				TimeoutType: TimeoutTypeStartToClose,
			},
		},
	}
}

func (i *ActivityImpl) RecordCompleted(
	chasmContext chasm.MutableContext,
	req *RecordCompletedRequest,
) (*RecordCompletedResponse, error) {
	// say we have a completedTime field
	// i.State.CompletedTime = timestamppb.New(chasmContext.Now())
	i.Output = chasm.NewDataField(chasmContext, &common.Payload{
		Data: req.Output,
	})

	return &RecordCompletedResponse{}, nil
}

func (i *ActivityImpl) Describe(
	_ chasm.Context,
	_ *DescribeActivityRequest,
) (*DescribeActivityResponse, error) {
	panic("not implemented")
}

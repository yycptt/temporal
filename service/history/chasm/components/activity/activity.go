package activity

import (
	"time"

	"go.temporal.io/api/common/v1"
	persistencepb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/service/history/chasm"
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

		// Or the component needes to implement SerDe interface
		State persistencepb.ActivityInfo // proto.Message
	}
)

func NewScheduledActivity(
	chasmContext chasm.MutableContext,
	params *NewActivityRequest,
) (*ActivityImpl, *NewActivityResponse, error) {
	// after return framework will use reflection to analyze
	// and understand the structure of the component tree
	activity := &ActivityImpl{}
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
	ctx chasm.MutableContext,
	req *ScheduleRequest,
) (*ScheduleResponse, error) {
	// also validate current state etc.

	i.State.ScheduledTime = timestamppb.New(chasm.Now(ctx, i))

	// we need to provide a name (string) here, since there could be
	// multiple fields with the same type. In this case, "input" & "output"
	// in reflection case this will be the field name, which is bind to a type
	if err := chasm.NewChildData(ctx, i, "input", &common.Payload{
		Data: req.Input,
	}); err != nil {
		return nil, err
	}

	if err := ctx.AddTask(
		i,
		chasm.TaskAttributes{}, // immediate task
		DispatchTask{},
	); err != nil {
		return nil, err
	}
	if err := chasm.AddTask(
		ctx,
		i,
		chasm.TaskAttributes{
			ScheduledTime: chasm.Now(ctx, i).Add(10 * time.Second),
		},
		TimeoutTask{
			TimeoutType: TimeoutTypeScheduleToStart,
		},
	); err != nil {
		return nil, nil
	}

	return &ScheduleResponse{}, nil
}

func (i *ActivityImpl) RecordStarted(
	ctx chasm.MutableContext,
	req *RecordStartedRequest,
) (*RecordStartedResponse, error) {

	// only this field will be updated
	i.State.StartedTime = timestamppb.New(chasm.Now(ctx, i))
	// update other states

	// when retrieving child data, we need to provide again the field name as a string
	// and also the type information
	// alternatively we can change this to
	// var payload *common.Payload
	// err := chasm.GetChild(ctx, i, &payload) and populate the variable passed in
	payload, err := chasm.GetChild[*common.Payload](ctx, i, "input")
	if err != nil {
		return nil, err
	}

	if err := ctx.AddTask(
		i,
		chasm.TaskAttributes{
			ScheduledTime: chasm.Now(ctx, i).Add(10 * time.Second),
		},
		TimeoutTask{
			TimeoutType: TimeoutTypeStartToClose,
		},
	); err != nil {
		return nil, nil
	}

	return &RecordStartedResponse{
		Input: payload.Data,
	}, nil
}

func (i *ActivityImpl) RecordCompleted(
	ctx chasm.MutableContext,
	req *RecordCompletedRequest,
) (*RecordCompletedResponse, error) {
	// say we have a completedTime field
	// i.State.CompletedTime = timestamppb.New(chasmContext.Now())

	// Same here, we need to provide the field name as a string.
	if err := chasm.NewChildData(ctx, i, "output", &common.Payload{
		Data: req.Output,
	}); err != nil {
		return nil, err
	}

	return &RecordCompletedResponse{}, nil
}

func (i *ActivityImpl) Describe(
	_ chasm.Context,
	_ *DescribeActivityRequest,
) (*DescribeActivityResponse, error) {
	panic("not implemented")
}

func (i *ActivityImpl) Serialize() ([]byte, error) {
	return i.State.Marshal()
}

func (i *ActivityImpl) Deserialize(data []byte) error {
	return i.State.Unmarshal(data)
}

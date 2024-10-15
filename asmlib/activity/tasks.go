package activity

import (
	"context"
	"errors"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/service/history/asm"
	"go.temporal.io/server/service/history/consts"
)

type (
	DispatchTask struct {
	}

	dispatchTaskHandler struct{}

	TimeoutTask struct {
		timeoutTime time.Time
		timeoutType timeoutType
	}

	timeoutTaskHandler struct{}
)

type timeoutType int

const (
	timeoutTypeScheduleToClose timeoutType = iota
	timeoutTypeScheduleToStart
	timeoutTypeStartToClose
	timeoutTypeHeartbeat
)

func (t *DispatchTask) TaskType() string {
	return "dispatch"
}

func (t *DispatchTask) Properties() asm.TaskProperties {
	return asm.TaskProperties{
		ScheduledTime: time.Time{},
		Destination:   "",
	}
}

func (t *dispatchTaskHandler) SerializeTask(task asm.Task) (*commonpb.DataBlob, error) {
	// don't need to serialize anything, maybe attempts
	return nil, nil
}

func (t *dispatchTaskHandler) DeserializeTask(_ asm.TaskProperties, _ *commonpb.DataBlob) (asm.Task, error) {
	// don't need to serialize anything
	return &DispatchTask{}, nil
}

func (t *dispatchTaskHandler) Validate(_ asm.Task, instance asm.Instance) error {
	activityInstance := instance.(*instanceImpl)
	if activityInstance.startedTime.IsZero() {
		return nil
	}
	return consts.ErrStaleReference
}

func (t *dispatchTaskHandler) Execute(
	ctx context.Context,
	task asm.Task,
	instanceRef asm.InstanceRef,
	engine asm.Engine,
) error {
	var scheduledEventToken asm.HistoryEventToken
	_, err := engine.Update(ctx, asm.UpdateRequest{
		Ref: instanceRef,
		TransitionFn: func(instance asm.Instance) error {
			activityInstance := instance.(*instanceImpl)
			if !activityInstance.startedTime.IsZero() {
				return consts.ErrStaleReference
			}
			scheduledEventToken = activityInstance.scheduledEventToken
			return nil
		},
	})
	if err != nil {
		return err
	}

	// call to matching to dispatch the activity
	return nil
}

func (t *TimeoutTask) TaskType() string {
	return "timeout"
}

func (t *TimeoutTask) Properties() asm.TaskProperties {
	return asm.TaskProperties{
		ScheduledTime: t.timeoutTime,
		Destination:   "",
	}
}

func (t *timeoutTaskHandler) SerializeTask(task asm.Task) (*commonpb.DataBlob, error) {
	// only need to serialize the timeout type part
	panic("not implemented")
}

func (t *timeoutTaskHandler) DeserializeTask(properties asm.TaskProperties, _ *commonpb.DataBlob) (asm.Task, error) {

	return &TimeoutTask{
		timeoutTime: properties.ScheduledTime,
		timeoutType: 0, // only need to decode timeout type
	}, nil
}

func (t *timeoutTaskHandler) Validate(task asm.Task, instance asm.Instance) error {
	activityInstance := instance.(*instanceImpl)
	switch task.(*TimeoutTask).timeoutType {
	case timeoutTypeScheduleToClose:
		if !activityInstance.completedTime.IsZero() {
			return nil
		}
		return consts.ErrStaleReference
	case timeoutTypeScheduleToStart:
		if activityInstance.startedTime.IsZero() {
			return nil
		}
		return consts.ErrStaleReference
	case timeoutTypeStartToClose:
		if activityInstance.completedTime.IsZero() {
			return nil
		}
		return consts.ErrStaleReference
	case timeoutTypeHeartbeat:
		if activityInstance.hbTimeoutTime == task.(*TimeoutTask).timeoutTime {
			return nil
		}
		return consts.ErrStaleReference
	}
	return errors.New("unknown timeout type")
}

func (t *timeoutTaskHandler) Execute(
	ctx context.Context,
	task asm.Task,
	instanceRef asm.InstanceRef,
	engine asm.Engine,
) error {
	_, err := engine.Update(ctx, asm.UpdateRequest{
		Ref: instanceRef,
		TransitionFn: func(instance asm.Instance) error {
			activityInstance := instance.(*instanceImpl)
			_, err := activityInstance.RecordTimedOut(RecordTimedOutRequest{})
			if err != nil {
				return err
			}
			return nil
		},
	})
	if err != nil {
		return err
	}
	return nil
}

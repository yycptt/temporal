package activity

import (
	"context"

	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/service/history/chasm"
	"go.temporal.io/server/service/history/consts"
)

// This should be in fx logic

type DispatchTask struct {
	chasm.OperationProgressBase
}

type DispatchTaskHandler struct {
	matchingClient matchingservice.MatchingServiceClient
}

func (h *DispatchTaskHandler) Validate(
	chasmContext chasm.Context,
	activity *ActivityImpl,
	task *DispatchTask,
) error {
	if !activity.State.StartedTime.AsTime().IsZero() {
		return consts.ErrStaleReference
	}

	return nil
}

func (h *DispatchTaskHandler) Execute(
	ctx context.Context,
	activityRef chasm.ComponentRef,
	t *DispatchTask,
) error {
	addTaskRequest, _, err := chasm.UpdateComponent(
		ctx,
		activityRef,
		(*ActivityImpl).GetDispatchInfo,
		t,
	)
	if err != nil {
		return err
	}

	_, err = h.matchingClient.AddActivityTask(ctx, addTaskRequest)
	return err
}

const (
	TimeoutTypeScheduleToStart = iota
	TimeoutTypeStartToClose
)

type TimeoutTask struct {
	// similar to component state,
	// use a proto message if possible
	TimeoutType int

	chasm.OperationProgressBase
}

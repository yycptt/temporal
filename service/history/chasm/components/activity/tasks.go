package activity

import (
	"context"

	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/service/history/chasm"
	"go.temporal.io/server/service/history/consts"
)

// This should be in fx logic

type DispatchTask struct{}

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
	_ *DispatchTask,
) error {
	if _, _, err := chasm.UpdateComponent(
		ctx,
		chasm.UpdateComponentRequest[*ActivityImpl, struct{}, struct{}]{
			Ref: activityRef,
			UpdateFn: func(activity *ActivityImpl, chasmContext chasm.MutableContext, _ struct{}) (struct{}, error) {
				// fetch some states here
				// return nil
				panic("not implemented")
			},
		},
	); err != nil {
		return err
	}

	_, err := h.matchingClient.AddActivityTask(ctx, &matchingservice.AddActivityTaskRequest{})
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
}

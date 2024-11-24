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
	MatchingClient matchingservice.MatchingServiceClient
}

func (h *DispatchTaskHandler) Validate(
	chasmContext chasm.Context,
	activity *ActivityImpl,
	task DispatchTask,
) error {
	if !activity.State.StartedTime.AsTime().IsZero() {
		return consts.ErrStaleReference
	}

	return nil
}

func (h *DispatchTaskHandler) Execute(
	ctx context.Context,
	activityRef chasm.ComponentRef,
	_ DispatchTask,
) error {
	matchingRequest, err := chasm.ReadComponent(
		ctx,
		chasm.ReadComponentRequest[*ActivityImpl, struct{}, *matchingservice.AddActivityTaskRequest]{
			Ref:    activityRef,
			ReadFn: (*ActivityImpl).LoadDispatchInfo,
		},
	)
	if err != nil {
		return err
	}

	// NOTE: we need to have a different set of APIs on matching for this.
	// because there is no Ref concept any more that works for both
	// top level activity and workflow activity
	_, err = h.MatchingClient.AddTopLevelActivityTask(ctx, matchingRequest)
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

package workflow

import (
	"context"

	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/service/history/chasm"
	"go.temporal.io/server/service/history/chasm/components/activity"
	"go.temporal.io/server/service/history/consts"
)

type ActivityDispatchTaskHandler struct {
	activity.DispatchTaskHandler
}

func (h *ActivityDispatchTaskHandler) Validate(
	chasmContext chasm.Context,
	w *WorkflowImpl,
	ref chasm.ComponentRef,
	task *activity.DispatchTask,
) error {
	if w.RunningState() != chasm.ComponentStateRunning {
		return consts.ErrStaleReference
	}

	activityID := ref.Path()[1]
	a, err := w.Activities[activityID].Get(chasmContext)
	if err != nil {
		return err
	}

	// Validate logic can be reused
	return h.DispatchTaskHandler.Validate(chasmContext, a, ref, task)
}

func (h *ActivityDispatchTaskHandler) Execute(
	ctx context.Context,
	ref chasm.ComponentRef,
	task *activity.DispatchTask,
) error {

	// Executor logic can't be reused
	addTaskRequest, err := chasm.ReadComponent(
		ctx,
		chasm.ReadComponentRequest[*WorkflowImpl, *activity.DispatchTask, *matchingservice.AddActivityTaskRequest]{
			Ref: ref,
			ReadFn: func(w *WorkflowImpl, chasmContext chasm.Context, t *activity.DispatchTask) (*matchingservice.AddActivityTaskRequest, error) {
				activityID := ref.Path()[1]
				a, err := w.Activities[activityID].Get(chasmContext)
				if err != nil {
					return nil, err
				}

				return a.GetDispatchInfo(chasmContext, t)
			},
			Input: task,
		},
	)
	if err != nil {
		return err
	}

	_, err = h.DispatchTaskHandler.MatchingClient.AddActivityTask(ctx, addTaskRequest)
	return err
}

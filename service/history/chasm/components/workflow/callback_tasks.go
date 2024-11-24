package workflow

import (
	"context"

	"go.temporal.io/server/service/history/chasm"
	"go.temporal.io/server/service/history/chasm/components/callback"
	"go.temporal.io/server/service/history/consts"
)

// What if the Callback is for a NexusOperation?
// Shall we a NexusOperationCallbackInvocationTask?
type ActivityCallbackInvocationTask struct {
	ActivityID string

	callback.InvocationTask // should be activity.CallbackInvocationTask
}

type ActivityCallbackInvocationTaskHandler struct {
	callback.InvocationTaskHandler // should be activity.CallbackInvocationTaskHandler
}

func (h *ActivityCallbackInvocationTaskHandler) Validate(
	chasmContext chasm.Context,
	workflow *WorkflowImpl,
	task ActivityCallbackInvocationTask,
) error {
	if workflow.RunningState() != chasm.ComponentStateRunning {
		return consts.ErrStaleReference
	}

	a, err := workflow.Activities[task.ActivityID].Get(chasmContext)
	if err != nil {
		return err
	}

	// this should be activity.CallbackInvocationTaskHandler.Validate
	return h.InvocationTaskHandler.Validate(chasmContext, a, task.InvocationTask)
}

func (h *ActivityCallbackInvocationTaskHandler) Execute(
	ctx context.Context,
	workflowRef chasm.ComponentRef,
	task ActivityCallbackInvocationTask,
) error {
	// similar to activity dispatch task, the Execute method on callback
	// want to make a RPC call outside the lock.
	// so we can't use the Execute method directly

	invocationInfo, err := chasm.ReadComponent(
		ctx,
		chasm.ReadComponentRequest[*WorkflowImpl, string, *callback.InvocationInfo]{
			Ref:    workflowRef,
			ReadFn: (*WorkflowImpl).LoadActivityCallbackInvocationInfo,
			Input:  task.ActivityID,
		},
	)
	if err != nil {
		return err
	}

	// make RPC call with invocationInfo
	return nil
}

package workflow

import (
	"context"

	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/service/history/chasm"
	"go.temporal.io/server/service/history/chasm/components/activity"
	"go.temporal.io/server/service/history/consts"
)

type ActivityDispatchTask struct {
	activity.DispatchTask

	ActivityID string
}

type ActivityDispatchTaskHandler struct {
	activity.DispatchTaskHandler
}

func (h *ActivityDispatchTaskHandler) Validate(
	chasmContext chasm.Context,
	workflow *WorkflowImpl,
	task ActivityDispatchTask,
) error {
	if workflow.RunningState() != chasm.ComponentStateRunning {
		return consts.ErrStaleReference
	}

	a, err := workflow.Activities[task.ActivityID].Get(chasmContext)
	if err != nil {
		return err
	}

	// Here we can reuse the Validate method directly since everything is in one transition
	// while lock is held
	return h.DispatchTaskHandler.Validate(
		chasmContext,
		a,
		task.DispatchTask,
	)
}

func (h *ActivityDispatchTaskHandler) Execute(
	ctx context.Context,
	workflowRef chasm.ComponentRef,
	task ActivityDispatchTask,
) error {
	// NOTE where we can not reuse the Execute method directly since:
	// 1. The Execute method needs a ref to the activity
	//        This we can solve by having a different form a Ref that resolves directly to a activity
	//        e.g. NewRefFromComponent(activityImpl) ComponentRef
	// 2. The Execute method will try to lock the instance again
	//        This can also be addressed by changing the impl to say if the ref points directly to a
	//        inmemory object, then skipping the lock
	// 3. Execute() method wants to do something outside the lock. (push to matching)
	//        The logic we would like to inject is for each chasm.Update/Read call, not per task
	//        so can only reuse each method call. In this case Workflow.LoadActivityDispatchInfo
	//        reuses activity.LoadActivityDispatchInfo

	matchingRequest, err := chasm.ReadComponent(
		ctx,
		chasm.ReadComponentRequest[*WorkflowImpl, string, *matchingservice.AddActivityTaskRequest]{
			Ref:    workflowRef,
			ReadFn: (*WorkflowImpl).LoadActivityDispatchInfo,
			Input:  task.ActivityID,
		},
	)
	if err != nil {
		return err
	}

	// NOTE: this is actually AddWorkflowActivityTask as the request contains activityID
	_, err = h.DispatchTaskHandler.MatchingClient.AddActivityTask(ctx, matchingRequest)
	return err
}

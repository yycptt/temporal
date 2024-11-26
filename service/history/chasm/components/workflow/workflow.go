package workflow

import (
	"context"
	"errors"
	"time"

	"go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/service/history/chasm"
	"go.temporal.io/server/service/history/chasm/components/activity"
	"go.temporal.io/server/service/history/consts"
)

type (
	WorkflowImpl struct {
		WorkflowInfo persistence.WorkflowExecutionInfo

		Activities chasm.ComponentMap[*activity.ActivityImpl]
	}
)

func NewWorkflow(
	chasmContext chasm.MutableContext,
	request *NewWorkflowRequest,
) (*NewWorkflowResponse, error) {
	workflow := &WorkflowImpl{
		WorkflowInfo: persistence.WorkflowExecutionInfo{}, // populate some fields
	}

	if err := chasmContext.AddTask(
		workflow,
		chasm.TaskAttributes{
			ScheduledTime: chasmContext.Now(workflow).Add(time.Hour),
		},
		WorkflowTimeoutTask{},
	); err != nil {
		return nil, err
	}
	if err := chasmContext.AddTask(
		workflow,
		chasm.TaskAttributes{},
		DispatchWorkflowTask{},
	); err != nil {
		return nil, err
	}

	return &NewWorkflowResponse{}, nil
}

func (w *WorkflowImpl) RunningState() chasm.ComponentState {
	panic("not implemented")
}

func (w *WorkflowImpl) RespondWorkflowTaskCompleted(
	chasmContext chasm.MutableContext,
	request *RespondWorkflowTaskCompletedRequest,
) (*RespondWorkflowTaskCompletedResponse, error) {
	for _, command := range request.ActivityCommands {
		a, _, err := activity.NewScheduledActivity(
			chasmContext,
			&activity.NewActivityRequest{},
		)
		if err != nil {
			return nil, err
		}

		w.Activities[command.ActivityID] = chasm.NewComponentField(
			chasmContext,
			a,
			chasm.NewComponentOptions{},
		)
	}
	return &RespondWorkflowTaskCompletedResponse{}, nil
}

// point is that this API is defined on workflow, not activity.
// Activity component service can still define a reuseable api to complete activity
// given a Ref. i.e. workflow doesn't need to re-define completeActivity, that's already
// defined on activity component and SDK worker can just invoke that api since it has the ref.
// But any byID activity can only be defined on Instances (top level).
func (w *WorkflowImpl) CompletedActivityByID(
	chasmContext chasm.MutableContext,
	request *CompleteActivityByIDRequest,
) (*CompleteActivityByIDResponse, error) {

	a, err := w.Activities.Get(chasmContext, request.ActivityID)
	if err != nil {
		return nil, errors.New("activity not found")
	}
	activityImpl, err := a.Get(chasmContext)
	if err != nil {
		return nil, err
	}

	_, err = activityImpl.RecordCompleted(chasmContext, &activity.RecordCompletedRequest{})
	if err != nil {
		return nil, err
	}

	// continue to schedule workflow task

	delete(w.Activities, request.ActivityID)

	return nil, err
}

// separate two concerns

// needs to be registered in the registry
func (w *WorkflowImpl) CustomChildOpertionRule(
	chasmContext chasm.Context,
	childComponent *activity.ActivityImpl,
) bool {
	// this is the default rule
	return chasm.ShouldContinueOperation(w, chasmContext, childComponent)
}

// needs to be registered in the registry
func (w *WorkflowImpl) ActivityCompletionStateListener()

// needs to be registered in the registry
// Only need to intercept the sub-component type cared about
// Framework will provide default behavior based on the parent component's
// running state & child component's lifecycle option.
func (w *WorkflowImpl) InterceptActivity(
	chasmContext chasm.MutableContext,
	childActivity *activity.ActivityImpl,
	next func() error,
) (retErr error) {
	// it will be much easier if we can listen on an event...
	// imagine what the code will look like if we want to check for multiple things.
	// activity started/completed/failed/timedout/paused.
	// huge switch case in this method.

	activityClosed := func(childActivity *activity.ActivityImpl) (bool, error) {
		resp, err := childActivity.Describe(
			chasmContext,
			&activity.DescribeActivityRequest{},
		)
		if err != nil {
			return false, err
		}
		return resp.CompletedTime.IsZero(), nil
	}
	closedBeforeProcessing, err := activityClosed(childActivity)
	if err != nil {
		return err
	}

	defer func() {
		closedAfterProcessing, err := activityClosed(childActivity)
		if err != nil {
			retErr = err
		}

		if !closedBeforeProcessing && closedAfterProcessing {
			// delete the activity
			// schedule new workflowTask
		}
	}()

	// if you just want to default behavior for pre-operation interceptor
	//
	// if chasm.ShouldContinueOperation(chasmContext, w, childComponent) {
	// 	return next()
	// }
	// return consts.ErrWorkflowCompleted

	runningState := w.RunningState()
	if runningState == chasm.ComponentStateRunning {
		return next()
	}

	// closed
	if chasmContext.Intent() == chasm.OperationIntentObserve {
		return next()
	}
	if chasmContext.Intent() == chasm.OperationIntentNotification {
		// we should check reset case here as well
		return next()
	}

	// closed but activity is abandoned
	// allow progress operations to proceed
	if chasmContext.LifeCycleOption(childActivity) == chasm.LifecycleOptionAbandon {
		return next()
	}

	return consts.ErrWorkflowCompleted
}

// This won't work because it introduces a new way of updating component,
// which can't be intercetped by the interceptor.
// type WorkflowActivityInterceptor struct {
// 	w *WorkflowImpl

// 	activity.Activity
// }

// func NewWorkflowActivityInterceptor(
// 	ctx chasm.Context,
// 	w *WorkflowImpl,
// 	activity activity.Activity,
// ) (activity.Activity, error) {
// 	return &WorkflowActivityInterceptor{
// 		w:        w,
// 		Activity: activity,
// 	}, nil
// }

// func (i *WorkflowActivityInterceptor) RecordStarted(
// 	chasmContext chasm.MutableContext,
// 	req *activity.RecordStartedRequest,
// ) (*activity.RecordStartedResponse, error) {
// 	return i.Activity.RecordStarted(chasmContext, req)
// }

type WorkflowTimeoutTask struct{}
type DispatchWorkflowTask struct{}

type NewWorkflowRequest struct{}
type NewWorkflowResponse struct{}

type ActivityCommand struct {
	ActivityID string
	Input      []byte
}

type RespondWorkflowTaskCompletedRequest struct {
	ActivityCommands []ActivityCommand
}

type RespondWorkflowTaskCompletedResponse struct{}

type CompleteActivityByIDRequest struct {
	WorkflowKey chasm.InstanceKey
	ActivityID  string
	Output      []byte
}
type CompleteActivityByIDResponse struct{}

type WorkflowHandler struct {
}

func (h *WorkflowHandler) CompleteActivityByID(
	ctx context.Context,
	request *CompleteActivityByIDRequest,
) (*CompleteActivityByIDResponse, error) {
	resp, _, err := chasm.UpdateComponent(
		ctx,
		chasm.NewComponentRef(request.WorkflowKey, "Workflow"),
		(*WorkflowImpl).CompletedActivityByID,
		request,
		chasm.EngineEagerLoadOption([]chasm.ComponentPath{
			{"Activities", request.ActivityID},
		}),
	)
	return resp, err
}

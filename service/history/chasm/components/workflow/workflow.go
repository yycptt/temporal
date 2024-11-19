package workflow

import (
	"context"
	"errors"
	"time"

	"go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/service/history/chasm"
	"go.temporal.io/server/service/history/chasm/components/activity"
)

type (
	WorkflowImpl struct {
		WorkflowInfo persistence.WorkflowExecutionInfo
	}
)

func NewWorkflow(
	ctx chasm.MutableContext,
	request *NewWorkflowRequest,
) (*NewWorkflowResponse, error) {
	workflow := &WorkflowImpl{
		WorkflowInfo: persistence.WorkflowExecutionInfo{}, // populate some fields
	}

	if err := chasm.AddTask(
		ctx,
		workflow,
		chasm.TaskAttributes{
			ScheduledTime: chasm.Now(ctx, workflow).Add(time.Hour),
		},
		WorkflowTimeoutTask{},
	); err != nil {
		return nil, err
	}
	if err := ctx.AddTask(
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
	ctx chasm.MutableContext,
	request *RespondWorkflowTaskCompletedRequest,
) (*RespondWorkflowTaskCompletedResponse, error) {
	for _, command := range request.ActivityCommands {

		a, _, err := activity.NewScheduledActivity(
			ctx,
			&activity.NewActivityRequest{},
		)
		if err != nil {
			return nil, err
		}

		// we need to provide a name (string) here when creating the child
		// also need to make sure the name is unique either within the parent
		// or at least within the same component type.
		// If we have a chasm.ComponentMap, then the name is implicitly scoped to that field
		// name, which is guaranteed to be unique.
		if err := chasm.NewChildComponent(ctx, w, a, command.ActivityID); err != nil {
			return nil, err
		}

	}
	return &RespondWorkflowTaskCompletedResponse{}, nil
}

// point is that this API is defined on workflow, not activity.
// Activity component service can still define a reuseable api to complete activity
// given a Ref. i.e. workflow doesn't need to re-define completeActivity, that's already
// defined on activity component and SDK worker can just invoke that api since it has the ref.
// But any byID activity can only be defined on Instances (top level).
func (w *WorkflowImpl) CompletedActivityByID(
	ctx chasm.MutableContext,
	request *CompleteActivityByIDRequest,
) (*CompleteActivityByIDResponse, error) {

	a, err := chasm.GetChild[*activity.ActivityImpl](ctx, w, request.ActivityID)
	if err != nil {
		return nil, errors.New("activity not found")
	}

	_, err = a.RecordCompleted(ctx, &activity.RecordCompletedRequest{})
	if err != nil {
		return nil, err
	}

	// continue to schedule workflow task

	return &CompleteActivityByIDResponse{}, nil
}

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
		chasm.UpdateComponentRequest[*WorkflowImpl, *CompleteActivityByIDRequest, *CompleteActivityByIDResponse]{
			// TODO: take in a struct def, not a registred name
			// But point here is that tell the framework your struct definition,
			// so it can determine which sub components needs to be eagerly loaded.

			// Also note that if you don't have a ref,
			// you can only interact with to the top level component.
			Ref: chasm.NewComponentRef(request.WorkflowKey, "Workflow"),
			// for partial load, we need to provide the field name as a string
			EagerLoadPaths: []chasm.ComponentPath{
				{"Activities", request.ActivityID},
			},
			UpdateFn: (*WorkflowImpl).CompletedActivityByID,
		},
	)

	// partial load:
	// the only benefit of using reflection is that we can automatically figure out
	// all sub-components that needs to be eagerly loaded.
	// However, if it a sub-component that always needs to be loaded, why separate component?

	// without reflection, we need to define this list at probably component registration time

	// allow you specify a path in string format
	// depending on the scope of component name, without reflection, we

	return resp, err
}

package workflow

import (
	"context"

	"go.temporal.io/server/service/history/chasm"
)

type RespondWorkflowTaskCompletedRequest struct {
	RefToken         []byte
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

func (h *WorkflowHandler) RespondWorkflowTaskCompleted(
	ctx context.Context,
	request *RespondWorkflowTaskCompletedRequest,
) (*RespondWorkflowTaskCompletedResponse, error) {
	ref, err := chasm.DeserializeComponentRef(request.RefToken)
	if err != nil {
		return nil, err
	}

	resp, _, err := chasm.UpdateComponent(
		ctx,
		chasm.UpdateComponentRequest[*WorkflowImpl, *RespondWorkflowTaskCompletedRequest, *RespondWorkflowTaskCompletedResponse]{
			Ref:      ref,
			UpdateFn: (*WorkflowImpl).RespondWorkflowTaskCompleted,
			Input:    request,
		},
	)
	return resp, err
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
			EagerLoadPaths: []chasm.ComponentPath{
				{"Activities", request.ActivityID},
			},
			UpdateFn: (*WorkflowImpl).CompleteActivityByID,
			Input:    request,
		},
	)
	return resp, err
}

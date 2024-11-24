package workflow

import (
	"context"

	"go.temporal.io/server/service/history/chasm"
	"go.temporal.io/server/service/history/chasm/components/activity"
)

type WorkflowHandler struct {
}

func (h *WorkflowHandler) CompleteActivityByID(
	ctx context.Context,
	request *CompleteActivityByIDRequest,
) (*CompleteActivityByIDResponse, error) {
	ref, err := chasm.DeserializeComponentRef(request.RefToken)
	if err != nil {
		return nil, err
	}

	resp, _, err := chasm.UpdateComponent(
		ctx,
		chasm.UpdateComponentRequest[*WorkflowImpl, *CompleteActivityByIDRequest, *CompleteActivityByIDResponse]{
			Ref: ref,
			EagerLoadPaths: []chasm.DataFieldPath{
				{"Activities", request.ActivityID},
			},
			UpdateFn: (*WorkflowImpl).CompleteActivityByID,
		},
	)
	return resp, err
}

type ActivityToken struct {
	RefToken   []byte // ref to the workflow
	ActivityID string // ref to the activity in the workflow
}

func (t *ActivityToken) Deserialize([]byte) error {
	panic("not implemented")
}

func (t *ActivityToken) Serialize() ([]byte, error) {
	panic("not implemented")
}

type CompleteActivityRequest struct {
	SerializedActivityToken []byte

	*activity.RecordCompletedRequest
}
type CompleteActivityResponse struct {
	*activity.RecordCompletedResponse
}

func (h *WorkflowHandler) CompleteActivity(
	ctx context.Context,
	request *CompleteActivityRequest,
) (*CompleteActivityResponse, error) {
	var activityToken ActivityToken
	if err := activityToken.Deserialize(request.SerializedActivityToken); err != nil {
		return nil, err
	}

	ref, err := chasm.DeserializeComponentRef(activityToken.RefToken)
	if err != nil {
		return nil, err
	}

	resp, _, err := chasm.UpdateComponent(
		ctx,
		chasm.UpdateComponentRequest[*WorkflowImpl, *CompleteActivityRequest, *CompleteActivityResponse]{
			Ref: ref,
			EagerLoadPaths: []chasm.DataFieldPath{
				{"Activities", activityToken.ActivityID},
			},
			UpdateFn: (*WorkflowImpl).CompleteActivity,
			Input:    request,
		},
	)
	return resp, err
}

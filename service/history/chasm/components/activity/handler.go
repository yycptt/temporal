package activity

import (
	"context"

	"go.temporal.io/api/common/v1"
	"go.temporal.io/server/service/history/chasm"
)

type ActivityHandler struct {
}

type NewActivityRequest struct {
	Input []byte
}

type NewActivityResponse struct {
	RefToken []byte
}

func (h *ActivityHandler) NewActivity(
	ctx context.Context,
	request *NewActivityRequest,
) (*NewActivityResponse, error) {
	resp, activityRef, err := chasm.NewInstance(
		ctx,
		chasm.NewInstanceRequest[*ActivityImpl, *NewActivityRequest, *NewActivityResponse]{
			Key: chasm.InstanceKey{
				NamespaceID: "default",
				BusinessID:  "memo",
				// in V1 we probably don't support specifying instanceID,
				// need to change persistence implementation for supporting that.
				// InstanceID:  uuid.New().String(),
			},
			IDReusePolicy: chasm.BusinessIDReusePolicyAllowDuplicate,
			NewFn:         NewScheduledActivity,
		},
	)
	if err != nil {
		return nil, err
	}

	resp.RefToken, err = activityRef.Serialize()
	return resp, err
}

func (h *ActivityHandler) RecordStarted(
	ctx context.Context,
	request *RecordStartedRequest,
) (*RecordStartedResponse, error) {
	// resp := &RecordStartedResponse{}

	ref, err := chasm.DeserializeComponentRef(request.RefToken)
	if err != nil {
		return nil, err
	}

	resp, startedActivityRef, err := chasm.UpdateComponent(
		ctx,
		chasm.UpdateComponentRequest[Activity, *RecordStartedRequest, *RecordStartedResponse]{
			Ref:      ref,
			UpdateFn: Activity.RecordStarted, // (*ActivityImpl).RecordStarted,
			Input:    request,
		},
	)

	resp.RefToken, err = startedActivityRef.Serialize()
	return resp, err
}

func (h *ActivityHandler) RecordCompleted(
	ctx context.Context,
	request *RecordCompletedRequest,
) (*RecordCompletedResponse, error) {
	ref, err := chasm.DeserializeComponentRef(request.RefToken)
	if err != nil {
		return nil, err
	}

	resp, _, err := chasm.UpdateComponent(
		ctx,
		chasm.UpdateComponentRequest[*ActivityImpl, *RecordCompletedRequest, *RecordCompletedResponse]{
			Ref:      ref,
			UpdateFn: (*ActivityImpl).RecordCompleted,
			Input:    request,
		},
	)

	return resp, err
}

type GetActivityResultRequest struct {
	RefToken []byte
}

type GetActivityResultResponse struct {
	Output []byte
}

func (h *ActivityHandler) GetActivityResult(
	ctx context.Context,
	request *GetActivityResultRequest,
) (*GetActivityResultResponse, error) {
	ref, err := chasm.DeserializeComponentRef(request.RefToken)
	if err != nil {
		return nil, err
	}

	var resp *GetActivityResultResponse
	if resp, _, err = chasm.PollComponent(
		ctx,
		chasm.PollComponentRequest[*ActivityImpl, *GetActivityResultRequest, *GetActivityResultResponse]{
			Ref: ref,
			PredicateFn: func(ai *ActivityImpl, ctx chasm.Context, garr *GetActivityResultRequest) bool {
				return ai.RunningState() == chasm.ComponentStateCompleted
			},
			OperationFn: func(ai *ActivityImpl, ctx chasm.MutableContext, garr *GetActivityResultRequest) (*GetActivityResultResponse, error) {
				outputPayload, err := chasm.GetChild[*common.Payload](ctx, ai, "output")
				resp.Output = outputPayload.Data
				return resp, err
			},
		},
	); err != nil {
		return nil, err
	}

	return resp, nil
}

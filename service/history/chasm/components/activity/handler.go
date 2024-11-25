package activity

import (
	"context"

	"go.temporal.io/server/service/history/chasm"
)

var _ Service = (*ActivityHandler)(nil)

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
				BusinessID:  "activityID",
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

	ref, err := chasm.DeserializeComponentRef(request.RefToken)
	if err != nil {
		return nil, err
	}

	resp, startedActivityRef, err := chasm.UpdateComponent(
		ctx,
		chasm.UpdateComponentRequest[*ActivityImpl, *RecordStartedRequest, *RecordStartedResponse]{
			Ref:      ref,
			UpdateFn: (*ActivityImpl).RecordStarted,
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
			PredicateFn: func(a *ActivityImpl, _ chasm.Context, _ *GetActivityResultRequest) (bool, error) {
				return a.RunningState() == chasm.ComponentStateCompleted, nil
			},
			OperationFn: func(a *ActivityImpl, ctx chasm.MutableContext, _ *GetActivityResultRequest) (*GetActivityResultResponse, error) {
				outputPayload, err := a.Output.Get(ctx)
				resp.Output = outputPayload.Data
				return resp, err
			},
			Input: request,
		},
	); err != nil {
		return nil, err
	}

	return resp, nil
}

package activity

import (
	"context"

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
		chasm.InstanceKey{
			NamespaceID: "default",
			BusinessID:  "memo",
			// in V1 we probably don't support specifying instanceID,
			// need to change persistence implementation for supporting that.
			// InstanceID:  uuid.New().String(),
		},
		NewScheduledActivity,
		request,
		chasm.EngineIDReusePolicyOption(
			chasm.BusinessIDReusePolicyAllowDuplicate,
			chasm.BusinessIDConflictPolicyFail,
		),
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
		ref,
		(*ActivityImpl).RecordStarted,
		request,
		chasm.EngineEagerLoadOption([]chasm.ComponentPath{
			{"Input"},
		}),
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
		ref,
		(*ActivityImpl).RecordCompleted,
		request,
	)

	return resp, err
}

type GetActivityResultRequest struct {
	RefToken []byte

	chasm.OperationObserveBase
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
		ref,
		func(a *ActivityImpl, ctx chasm.Context, _ *GetActivityResultRequest) bool {
			return a.RunningState() == chasm.ComponentStateCompleted
		},
		func(a *ActivityImpl, ctx chasm.MutableContext, _ *GetActivityResultRequest) (*GetActivityResultResponse, error) {
			outputPayload, err := a.Output.Get(ctx)
			resp.Output = outputPayload.Data
			return resp, err
		},
		request,
	); err != nil {
		return nil, err
	}

	return resp, nil
}

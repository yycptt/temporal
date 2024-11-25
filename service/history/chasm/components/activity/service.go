package activity

import (
	"context"

	"go.temporal.io/server/service/history/chasm"
)

// TODO: this is not the full activity service
// full service should also have endpoint for NewActivity which only applies to top level activity.
// or takes no Ref
// This is only the part of the activity service that can be executed even when activity is a sub component
type Service interface {
	RecordStarted(context.Context, *RecordStartedRequest) (*RecordStartedResponse, error)
	RecordCompleted(context.Context, *RecordCompletedRequest) (*RecordCompletedResponse, error)
	GetActivityResult(context.Context, *GetActivityResultRequest) (*GetActivityResultResponse, error)
}

type ServiceHandler struct {
}

// Use code-gen for this.

func (h *ServiceHandler) RecordStarted(
	ctx context.Context,
	request *RecordStartedRequest,
) (*RecordStartedResponse, error) {
	ref, err := chasm.DeserializeComponentRef(request.RefToken)
	if err != nil {
		return nil, err
	}

	// framework will find the top level component from the ref, and call the registered ServiceHandler method.
	return chasm.HandleOperation(ctx, ref, Service.RecordStarted, request)
}

func (h *ServiceHandler) RecordCompleted(
	ctx context.Context,
	request *RecordCompletedRequest,
) (*RecordCompletedResponse, error) {
	ref, err := chasm.DeserializeComponentRef(request.RefToken)
	if err != nil {
		return nil, err
	}

	return chasm.HandleOperation(ctx, ref, Service.RecordCompleted, request)
}

func (h *ServiceHandler) GetActivityResult(
	ctx context.Context,
	request *GetActivityResultRequest,
) (*GetActivityResultResponse, error) {
	ref, err := chasm.DeserializeComponentRef(request.RefToken)
	if err != nil {
		return nil, err
	}

	return chasm.HandleOperation(ctx, ref, Service.GetActivityResult, request)
}

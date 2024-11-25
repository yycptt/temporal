package workflow

import (
	"context"
	"errors"

	"go.temporal.io/server/service/history/chasm"
	"go.temporal.io/server/service/history/chasm/components/activity"
	"go.temporal.io/server/service/history/consts"
)

var _ activity.Service = (*WorkflowActivityHandler)(nil)

type WorkflowActivityHandler struct{}

func (h *WorkflowActivityHandler) RecordStarted(
	ctx context.Context,
	request *activity.RecordStartedRequest,
) (*activity.RecordStartedResponse, error) {
	ref, err := chasm.DeserializeComponentRef(request.RefToken)
	if err != nil {
		return nil, err
	}

	activityPath := ref.Path() // ["Activities", "activityID"]
	activityID := activityPath[1]

	resp, _, err := chasm.UpdateComponent(
		ctx,
		chasm.UpdateComponentRequest[*WorkflowImpl, *activity.RecordStartedRequest, *activity.RecordStartedResponse]{
			Ref: ref,
			UpdateFn: func(
				w *WorkflowImpl,
				chasmContext chasm.MutableContext,
				request *activity.RecordStartedRequest,
			) (*activity.RecordStartedResponse, error) {
				if w.RunningState() != chasm.ComponentStateRunning {
					return nil, consts.ErrWorkflowCompleted
				}

				a, err := w.Activities[activityID].Get(chasmContext)
				if err != nil {
					return nil, errors.New("activity not found")
				}

				return a.RecordStarted(chasmContext, request)
			},
			Input: request,
		},
	)

	return resp, err
}

func (h *WorkflowActivityHandler) RecordCompleted(
	ctx context.Context,
	request *activity.RecordCompletedRequest,
) (*activity.RecordCompletedResponse, error) {
	ref, err := chasm.DeserializeComponentRef(request.RefToken)
	if err != nil {
		return nil, err
	}

	activityPath := ref.Path() // ["Activities", "activityID"]
	activityID := activityPath[1]

	resp, _, err := chasm.UpdateComponent(
		ctx,
		chasm.UpdateComponentRequest[*WorkflowImpl, *activity.RecordCompletedRequest, *activity.RecordCompletedResponse]{
			Ref: ref,
			UpdateFn: func(
				w *WorkflowImpl,
				chasmContext chasm.MutableContext,
				request *activity.RecordCompletedRequest,
			) (*activity.RecordCompletedResponse, error) {
				if w.RunningState() != chasm.ComponentStateRunning {
					return nil, consts.ErrWorkflowCompleted
				}

				a, err := w.Activities[activityID].Get(chasmContext)
				if err != nil {
					return nil, errors.New("activity not found")
				}

				resp, err := a.RecordCompleted(chasmContext, request)
				if err != nil {
					return nil, err
				}

				delete(w.Activities, activityID)
				// schedule workflow task

				return resp, nil
			},
			Input: request,
		},
	)

	return resp, err
}

func (h *WorkflowActivityHandler) GetActivityResult(
	ctx context.Context,
	request *activity.GetActivityResultRequest,
) (*activity.GetActivityResultResponse, error) {
	ref, err := chasm.DeserializeComponentRef(request.RefToken)
	if err != nil {
		return nil, err
	}

	activityPath := ref.Path() // ["Activities", "activityID"]
	activityID := activityPath[1]

	resp, _, err := chasm.PollComponent(
		ctx,
		chasm.PollComponentRequest[*WorkflowImpl, *activity.GetActivityResultRequest, *activity.GetActivityResultResponse]{
			Ref: ref,
			PredicateFn: func(
				w *WorkflowImpl,
				chasmContext chasm.Context,
				request *activity.GetActivityResultRequest,
			) (bool, error) {
				// How to do this if we allow direct sub-component access?
				if w.RunningState() != chasm.ComponentStateRunning {
					return true, nil
				}
				a, err := w.Activities[activityID].Get(chasmContext)
				if err != nil {
					return false, err
				}
				return a.RunningState() == chasm.ComponentStateCompleted, nil
			},
			OperationFn: func(
				w *WorkflowImpl,
				chasmContext chasm.MutableContext,
				request *activity.GetActivityResultRequest,
			) (*activity.GetActivityResultResponse, error) {
				a, err := w.Activities[activityID].Get(chasmContext)
				if err != nil {
					return nil, err
				}

				if a.Output == nil {
					return nil, consts.ErrWorkflowCompleted
				}

				outputPayload, err := a.Output.Get(chasmContext)
				if err != nil {
					return nil, err
				}

				return &activity.GetActivityResultResponse{
					Output: outputPayload.Data,
				}, nil
			},
			Input: request,
		},
	)
	return resp, err
}

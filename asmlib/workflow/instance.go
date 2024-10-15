package workflow

import (
	"go.temporal.io/api/common/v1"
	"go.temporal.io/server/asmlib/activity"
	"go.temporal.io/server/service/history/asm"
)

type (
	instanceImpl struct {
		backend asm.InstanceBackend

		status string
	}
)

type (
	RecordWorkflowTaskCompletedRequest  struct{}
	RecordWorkflowTaskCompletedResponse struct{}
)

func (i *instanceImpl) RecordWorkflowTaskCompleted(
	request *RecordWorkflowTaskCompletedRequest,
) (*RecordWorkflowTaskCompletedResponse, error) {

	// say we need to schedule an activity

	var activityInstance activity.Activity
	if err := i.backend.NewChildASM("common/activityID", &activityInstance); err != nil {
		return nil, err
	}

	if err := activityInstance.Schedule(common.Payload{}); err != nil {
		return nil, err
	}

	return &RecordWorkflowTaskCompletedResponse{}, nil
}

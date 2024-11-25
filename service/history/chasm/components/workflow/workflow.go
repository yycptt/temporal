package workflow

import (
	"errors"
	"time"

	"go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/service/history/chasm"
	"go.temporal.io/server/service/history/chasm/components/activity"
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
func (w *WorkflowImpl) CompleteActivityByID(
	chasmContext chasm.MutableContext,
	request *CompleteActivityByIDRequest,
) (*CompleteActivityByIDResponse, error) {

	a, err := w.Activities[request.ActivityID].Get(chasmContext)
	if err != nil {
		return nil, errors.New("activity not found")
	}

	_, err = a.RecordCompleted(chasmContext, &activity.RecordCompletedRequest{})
	if err != nil {
		return nil, err
	}

	// continue to schedule workflow task

	return nil, nil
}

type WorkflowTimeoutTask struct{}
type DispatchWorkflowTask struct{}

type NewWorkflowRequest struct{}
type NewWorkflowResponse struct{}

type ActivityCommand struct {
	ActivityID string
	Input      []byte
}

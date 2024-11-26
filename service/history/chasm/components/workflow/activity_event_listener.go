package workflow

import (
	"go.temporal.io/server/service/history/chasm"
	"go.temporal.io/server/service/history/chasm/components/activity"
)

type ActivityEventListener struct {
	w *WorkflowImpl
	a *activity.ActivityImpl
}

func NewActivityEventListener(
	chasmContext chasm.MutableContext,
	w *WorkflowImpl,
	a *activity.ActivityImpl,
	// we likely also need (relative) path to the activity as well
) *ActivityEventListener {
	return &ActivityEventListener{
		w: w,
		a: a,
	}
}

func (l *ActivityEventListener) OnStart(e activity.ActivityStartedEvent) error {
	return nil
}

func (l *ActivityEventListener) OnCompletion(e activity.ActivityCompletedEvent) error {
	// schedule workflow task
	// delete activity
	return nil
}

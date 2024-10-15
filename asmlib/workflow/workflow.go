package workflow

import (
	"go.temporal.io/server/asmlib/activity"
	"go.temporal.io/server/service/history/asm"
	"go.temporal.io/server/service/history/consts"
)

const (
	asmType = "workflow"
)

type (
	asmImpl struct{}
)

func (a *asmImpl) Type() string {
	return asmType
}

func (a *asmImpl) Options() []asm.Option {
	return nil
}

func (a *asmImpl) Interceptors() map[any]asm.Interceptor {
	var activity activity.Activity
	return map[any]asm.Interceptor{
		&activity: func(instance asm.Instance) error {
			workflowInstance := instance.(*instanceImpl)
			if workflowInstance.status == "closed" {
				return consts.ErrWorkflowCompleted
			}
			return nil
		},
	}
}

func (a *asmImpl) EventListeners() map[any]asm.EventListener {
	return map[any]asm.EventListener{
		&activity.ActivityStartedEvent{}: func(instance asm.Instance, event any) error {
			startedEvent := event.(*activity.ActivityStartedEvent)
			// schedule workflow task

			// maybe record event etc.
			return nil
		},
	}
}

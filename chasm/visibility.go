package chasm

import (
	"context"

	"go.temporal.io/api/common/v1"
	"go.temporal.io/server/api/persistence/v1"
)

type builtInLibrary struct {
	UnimplementedLibrary
}

func (b *builtInLibrary) Name() string {
	return "chasm"
}

func (b *builtInLibrary) Components() []*RegistrableComponent {
	return []*RegistrableComponent{
		NewRegistrableComponent[*Visibility]("visibility"),
	}
}

func (b *builtInLibrary) Tasks() []*RegistrableTask {
	return []*RegistrableTask{
		NewRegistrableSideEffectTask[*Visibility](
			"visibilityTask",
			&visibilityTaskValidator{},
			&visibilityTaskExecutor{},
		),
	}
}

type Visibility struct {
	UnimplementedComponent

	State *persistence.VisibilityState

	SA   Collection[string, *common.Payload]
	Memo Collection[string, *common.Payload]
}

func (v *Visibility) LifecycleState(_ Context) LifecycleState {
	return LifecycleStateRunning
}

func (v *Visibility) SearchAttributes(
	chasmContext Context,
) (map[string]*common.Payload, error) {
	sa := make(map[string]*common.Payload, len(v.SA))
	for key, field := range v.SA {
		value, err := field.Get(chasmContext)
		if err != nil {
			return nil, err
		}
		sa[key] = value
	}
	return sa, nil
}

func (v *Visibility) UpdateSearchAttributes(
	mutableContext MutableContext,
	updates map[string]*common.Payload,
) {
	for key, value := range updates {
		v.SA[key] = NewDataField(mutableContext, value)
	}
	v.generateTask(mutableContext)
}

func (v *Visibility) RemoveSearchAttributes(
	mutableContext MutableContext,
	keys ...string,
) {
	for _, key := range keys {
		delete(v.SA, key)
	}
	v.generateTask(mutableContext)
}

func (v *Visibility) generateTask(
	mutableContext MutableContext,
) {
	v.State.TransitionCount++
	mutableContext.AddTask(v, TaskAttributes{}, visibilityTask{TransitionCount: v.State.TransitionCount})
}

type visibilityTask struct {
	TransitionCount int64
}

type visibilityTaskValidator struct{}

func (v *visibilityTaskValidator) Validate(
	_ Context,
	component *Visibility,
	task visibilityTask,
) (bool, error) {
	return task.TransitionCount < component.State.TransitionCount, nil
}

type visibilityTaskExecutor struct{}

func (v *visibilityTaskExecutor) Execute(
	ctx context.Context,
	componentRef ComponentRef,
	task visibilityTask,
) error {
	panic("visibilityTaskExecutor should not be called directly")
}

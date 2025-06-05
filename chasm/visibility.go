package chasm

import (
	"context"

	"go.temporal.io/api/common/v1"
	"go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/payload"
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

func NewVisibility() *Visibility {
	return &Visibility{}
}

func (v *Visibility) LifecycleState(_ Context) LifecycleState {
	return LifecycleStateRunning
}

func (v *Visibility) SearchAttributes(
	chasmContext Context,
) (map[string]any, error) {
	sa := make(map[string]any, len(v.SA))
	for key, field := range v.SA {
		p, err := field.Get(chasmContext)
		if err != nil {
			return nil, err
		}
		var v any
		if err = payload.Decode(p, &v); err != nil {
			return nil, err
		}
		sa[key] = v
	}
	return sa, nil
}

func SearchAttributeByName[T any](
	chasmContext Context,
	visibility *Visibility,
	name string,
) (T, error) {
	var result T
	p, err := visibility.SA[name].Get(chasmContext)
	if err != nil {
		return result, err
	}
	if err = payload.Decode(p, &result); err != nil {
		return result, err
	}
	return result, nil
}

func UpdateSearchAttribute[T ~int | ~int32 | ~int64 | ~string | ~bool | ~float64 | ~[]byte](
	chasmContext MutableContext,
	visibility *Visibility,
	name string,
	value T,
) {
	p, err := payload.Encode(value)
	if err != nil {
		panic(err) // will never happen
	}
	visibility.SA[name] = NewDataField(chasmContext, p)
}

func (v *Visibility) UpdateSearchAttributes(
	mutableContext MutableContext,
	updates map[string]any,
) error {
	for key, value := range updates {
		p, err := payload.Encode(value)
		if err != nil {
			return err
		}
		v.SA[key] = NewDataField(mutableContext, p)
	}
	v.generateTask(mutableContext)
	return nil
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

func UpdateMemo[T ~int | ~int32 | ~int64 | ~string | ~bool | ~float64 | ~[]byte](
	chasmContext MutableContext,
	visibility *Visibility,
	name string,
	value T,
) {
	p, err := payload.Encode(value)
	if err != nil {
		panic(err) // will never happen
	}
	visibility.Memo[name] = NewDataField(chasmContext, p)
}

func (v *Visibility) UpdateMemo(
	mutableContext MutableContext,
	updates map[string]any,
) error {
	for key, value := range updates {
		p, err := payload.Encode(value)
		if err != nil {
			return err
		}
		v.Memo[key] = NewDataField(mutableContext, p)
	}
	v.generateTask(mutableContext)
	return nil
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

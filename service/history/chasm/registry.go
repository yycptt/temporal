package chasm

type Registry struct{}

func (r *Registry) RegisterLibrary(
	lib Library,
) error {
	panic("not implemented")
}

type Library interface {
	Name() string
	Components() []RegistrableComponent
	Tasks() []RegistrableTask
}

type RegistrableComponent struct {
}

func NewRegistrableComponent[P Component](
	name string,
	options ...RegistrableComponentOption,
) RegistrableComponent {
	panic("not implemented")
}

type componentOptions struct{}

type RegistrableComponentOption func(opt *componentOptions)

func NewComponentEagerLoadingOption[T any](
	childName string,
) RegistrableComponentOption {
	panic("not implemented")
}

func NewComponentShardingOption(
	sharding func(InstanceKey) string,
) RegistrableComponentOption {
	panic("not implemented")
}

type RegistrableTask struct{}

func NewRegistrableTask[C any, T any](
	handler TaskHandler[C, T],
	options RegistrableTaskOptions,
) RegistrableTask {
	panic("not implemented")
}

type RegistrableTaskOptions struct {
	Name string
}

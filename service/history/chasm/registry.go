package chasm

type Registry struct{}

func (r *Registry) RegisterLibrary(lib Library) {
	panic("not implemented")
}

type Library interface {
	Name() string
	Components() []RegistrableComponent
	Tasks() []RegistrableTask
}

type RegistrableComponent struct {
}

// type RegistrableChildEventListener[P Component] struct{}

// func NewRegistrableChildEventListener[P, C Component, L any](
// 	listenerCtor func(MutableContext, P, C) L,
// ) RegistrableChildEventListener[P] {
// 	panic("not implemented")
// }

// type RegistrableChildOperationRule[P Component] struct{}

// func NewRegistrableChildOperationRule[P, C Component](
// 	rule func(P, Context, C) bool,
// ) RegistrableChildOperationRule[P] {
// 	panic("not implemented")
// }

func NewRegistrableComponent[P Component](
	name string,
	opts ...RegistrableComponentOption,
	// childEventListerners []RegistrableChildEventListener[P],
	// childOperationRules []RegistrableChildOperationRule[P],
	// childStateListeners []RegistrableChildStateListener[P],
	// childInterceptors []RegistrableChildInterceptor[P],
	// options RegistrableComponentOptions,
) RegistrableComponent {
	panic("not implemented")
}

type registrableComponentOptions struct {
	// eventListener
	// operationRule
	// interceptor
}

type RegistrableComponentOption func(*registrableComponentOptions)

func EventListener[P, C Component, L any](
	listenerCtor func(MutableContext, P, C) L,
) RegistrableComponentOption {
	panic("not implemented")
}

func OperationRule[P, C Component](
	rule func(P, Context, C) bool,
) RegistrableComponentOption {
	panic("not implemented")
}

func Interceptor[P, C Component](
	interceptorFn func(P, MutableContext, C, func() error) error,
) RegistrableComponentOption {
	panic("not implemented")
}

func ShardingFn(
	func(InstanceKey) string,
) RegistrableComponentOption {
	panic("not implemented")
}

// type RegistrableChildStateListener[P Component] struct{}

// func NewRegistrableChildStateListener[P, C Component](
// 	predicateFn func(Context, C) (bool, error),
// 	transitionFn func(P, MutableContext, C) error,
// ) RegistrableChildStateListener[P] {
// 	panic("not implemented")
// }

// type RegistrableChildInterceptor[P Component] struct{}

// func NewRegistrableChildInterceptor[P, C Component](
// 	interceptorFn func(P, MutableContext, C, func() error) error,
// ) RegistrableChildInterceptor[P] {
// 	panic("not implemented")
// }

type RegistrableTask struct{}

func NewRegistrableTask[C any, T any](
	name string,
	handler TaskHandler[C, T],
	// options RegistrableTaskOptions,
) RegistrableTask {
	panic("not implemented")
}

// type RegistrableComponentOptions struct {
// 	// maybe we use use proto name of the state TBD.
// 	Name string

// 	// only applys when component is used as an instance
// 	StaticInstanceOptions StaticInstanceOptions
// }

// type StaticInstanceOptions struct {
// 	ShardingOption InstanceShardingOption
// }

type DynamicInstanceOptions struct {
	StorageOption     InstanceStorageOption
	ReplicationOption InstanceReplicationOption
}

type InstanceStorageOption int

const (
	IntanceStorageOptionInMemory InstanceStorageOption = iota
	InstanceStorageOptionPersistent
)

type InstanceReplicationOption int

const (
	InstanceReplicationOptionSingleCluster InstanceReplicationOption = iota
	InstanceReplicationOptionMultiCluster
)

// type InstanceShardingOption struct {
// 	Sharding func(InstanceKey) string
// }

// type RegistrableTaskOptions struct {
// 	Name string
// }

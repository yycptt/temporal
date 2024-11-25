package chasm

type Registry struct{}

func (r *Registry) RegisterLibrary(lib Library) {
	panic("not implemented")
}

type Library interface {
	Name() string
	Components() []RegistrableComponent
	Tasks() []RegistrableTask
	Services() []RegistrableService
}

type RegistrableService struct{}

func NewRegistrableService[S any]() RegistrableService {
	panic("not implemented")
}

type RegistrableComponent struct {
}

func NewRegistrableComponent[P Component](
	options RegistrableComponentOptions,
) RegistrableComponent {
	panic("not implemented")
}

type RegistrableTask struct{}

func NewRegistrableTask[T any](
	options RegistrableTaskOptions,
) RegistrableTask {
	panic("not implemented")
}

type RegistrableComponentOptions struct {
	// maybe we use use proto name of the state TBD.
	Name string

	// only applys when component is used as an instance
	StaticInstanceOptions StaticInstanceOptions

	ServiceHandlers []RegistrableServiceHandler
	TaskHandlers    []RegistrableTaskHandler
}

type RegistrableServiceHandler struct{}

func NewRegistrableServiceHandler[S any](
	handler S,
) RegistrableServiceHandler {
	panic("not implemented")
}

type RegistrableTaskHandler struct{}

func NewRegistrableTaskHandler[C any, T any](
	handler TaskHandler[C, T],
) RegistrableTaskHandler {
	panic("not implemented")
}

type StaticInstanceOptions struct {
	ShardingOption InstanceShardingOption
}

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

type InstanceShardingOption struct {
	Sharding func(InstanceKey) string
}

type RegistrableTaskOptions struct {
	Name string
}

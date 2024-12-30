package chasm

import (
	"errors"
	"reflect"
	"strings"
)

type Library interface {
	Name() string
	Components() []RegistrableComponent
	Tasks() []RegistrableTask
	// Service()
}

type RegistrableComponent struct {
	name          string
	componentType reflect.Type // this must be a struct type
	entityOptions entityOptions
}

type entityOptions struct {
	ephemeral     bool
	singleCluster bool
	shardingFn    func(EntityKey) string
}

func NewRegistrableComponent[C Component](
	name string,
	opts ...RegistrableComponentOption,
) RegistrableComponent {
	rc := RegistrableComponent{
		name:          name,
		componentType: reflect.TypeFor[C](),
		entityOptions: entityOptions{},
	}
	for _, opt := range opts {
		opt(&rc)
	}
	return rc
}

type RegistrableComponentOption func(*RegistrableComponent)

func EntityEphemeral() RegistrableComponentOption {
	return func(rc *RegistrableComponent) {
		rc.entityOptions.ephemeral = true
	}
}

// Is there any use case where we don't want to replicate
// certain instances of a archetype?
func EntitySingleCluster() RegistrableComponentOption {
	return func(rc *RegistrableComponent) {
		rc.entityOptions.singleCluster = true
	}
}

func EntityShardingFn(
	shardingFn func(EntityKey) string,
) RegistrableComponentOption {
	return func(rc *RegistrableComponent) {
		rc.entityOptions.shardingFn = shardingFn
	}
}

type RegistrableTask struct {
	name          string
	componentType reflect.Type // this can be an interface
	taskType      reflect.Type // task also stores the path of the component, so path of component -> component -> component name -> component struct type
	handler       any
}

func NewRegistrableTask[C any, T any](
	name string,
	handler TaskHandler[C, T],
	// opts ...RegistrableTaskOptions, no options right now
) RegistrableTask {
	return RegistrableTask{
		name:          name,
		componentType: reflect.TypeFor[C](),
		taskType:      reflect.TypeFor[T](),
		handler:       handler,
	}
}

type Registry struct {
	components     map[string]RegistrableComponent
	componentNames map[reflect.Type]string

	tasks     map[string]RegistrableTask
	taskNames map[reflect.Type]string
}

func (r *Registry) RegisterLibrary(lib Library) error {
	prefix := lib.Name()
	for _, c := range lib.Components() {
		if err := r.registerComponent(prefix, c); err != nil {
			return err
		}
	}
	for _, t := range lib.Tasks() {
		if err := r.registerTask(prefix, t); err != nil {
			return err
		}
	}
	return nil
}

func (r *Registry) registerComponent(
	prefix string,
	c RegistrableComponent,
) error {
	if strings.Contains("/", c.name) {
		return errors.New("component name cannot contain '/'")
	}
	name := prefix + "/" + c.name
	if _, ok := r.components[name]; ok {
		return errors.New("component already registered")
	}
	if c.componentType.Kind() != reflect.Struct {
		return errors.New("component type must be a struct")
	}
	r.components[name] = c
	r.componentNames[c.componentType] = name
	return nil
}

func (r *Registry) registerTask(
	prefix string,
	t RegistrableTask,
) error {
	if strings.Contains("/", t.name) {
		return errors.New("task name cannot contain '/'")
	}
	name := prefix + "/" + t.name
	if _, ok := r.tasks[name]; ok {
		return errors.New("task already registered")
	}
	if t.componentType.Kind() != reflect.Struct {
		return errors.New("task type must be a struct")
	}
	r.tasks[name] = t
	r.taskNames[t.taskType] = name
	return nil
}

func (r *Registry) getComponentByName(
	name string,
) (RegistrableComponent, bool) {
	c, ok := r.components[name]
	return c, ok
}

func (r *Registry) getComponentByInstance(
	instance any,
) (RegistrableComponent, bool) {
	name, ok := r.componentNames[reflect.TypeOf(instance)]
	if !ok {
		return RegistrableComponent{}, false
	}
	return r.getComponentByName(name)
}

func (r *Registry) getTaskByName(
	name string,
) (RegistrableTask, bool) {
	t, ok := r.tasks[name]
	return t, ok
}

func (r *Registry) getTaskByInstance(
	instance any,
) (RegistrableTask, bool) {
	name, ok := r.taskNames[reflect.TypeOf(instance)]
	if !ok {
		return RegistrableTask{}, false
	}
	return r.getTaskByName(name)
}

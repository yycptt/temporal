package chasm

import (
	"context"
	"reflect"
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"google.golang.org/protobuf/proto"
)

// 2. History event as a append only, immutable ordered storaged model
// 3. parent notify/intercept child component

// Open questions:
// 2. event notification?
// 3. history storage model?
// 4. CompletedActivityByID??? (this is obviously an API on workflow, not Activity)

type InstanceKey struct {
	NamespaceID string
	BusinessID  string
	InstanceID  string
}

type Context interface {
	// Context is not binded to any component,
	// so all methods needs to take in component as a parameter

	// TODO: don't provide any methods here, make all of them
	// chasm.XXX(Context, Component, ...)

	// NOTE: component created in the current transaction won't have a ref

	ref(Component) (ComponentRef, bool)
	lifeCycleOption(Component) LifecycleOption
	now(Component) time.Time

	listChildNames(Component, reflect.Type) ([]string, error)
	getChild(Component, string) (any, error)

	getContext() context.Context
}

func GetChild[T any](
	ctx Context,
	parent Component,
	childName string,
) (T, error) {
	child, err := ctx.getChild(parent, childName)
	return child.(T), err
}

func ListChildNames[T any](
	ctx Context,
	parent Component,
) ([]string, error) {
	return ctx.listChildNames(parent, reflect.TypeFor[T]())
}

type childOptions struct {
	name            string
	LifecycleOption LifecycleOption
}

type ChildOptions func(*childOptions)

func NewChildLifecycleOption(option LifecycleOption) ChildOptions {
	return func(o *childOptions) {
		o.LifecycleOption = option
	}
}

type ComponentData interface {
	proto.Message
}

type MutableContext interface {
	Context

	newChildComponent(parent Component, name string, child Component, opts ...ChildOptions) error
	newChildData(parent Component, name string, data ComponentData) error
	removeChild(parent Component, name string) error

	AddTask(component Component, attributes TaskAttributes, task any) error
}

func NewChildComponent(
	ctx MutableContext,
	parent Component,
	child Component,
	name string,
	opts ...ChildOptions,
) error {
	return ctx.newChildComponent(parent, name, child, opts...)
}

func NewChildData(
	ctx MutableContext,
	parent Component,
	name string,
	data ComponentData,
) error {
	return ctx.newChildData(parent, name, data)
}

func RemoveChild(
	ctx MutableContext,
	parent Component,
	name string,
) error {
	return ctx.removeChild(parent, name)
}

func AddTask(
	ctx MutableContext,
	component Component,
	attributes TaskAttributes,
	task any,
) error {
	return ctx.AddTask(component, attributes, task)
}

func Now(ctx Context, component Component) time.Time {
	return ctx.now(component)
}

type Component interface {
	RunningState() ComponentState
}

type NewComponentOptions struct {
	LifecycleOption LifecycleOption
}

type LifecycleOption int

const (
	LifecycleOptionAbandon LifecycleOption = 1 << iota
	LifecycleOptionBlock
)

type ComponentState int

const (
	ComponentStateRunning ComponentState = 1 << iota
	ComponentStatePaused
	ComponentStateCompleted
	ComponentStateFailed
	ComponentStateReset
)

// type ConsistencyToken []byte

type ComponentPath []string

type ComponentRef struct {
	Key InstanceKey

	// From the component name, we can find the component struct definition
	// use reflection to find sub-components and understand if those sub-components
	// needs to be loaded or not. we only need to do this for sub-components of the component.
	// path for parent/ancenstor component can be inferred from the path.
	componentName      string
	path               ComponentPath
	componentInitialVT *persistencespb.VersionedTransition // this identifies a component

	instanceLastUpdateVT *persistencespb.VersionedTransition // this is consistency token
}

// The point here is exactly that if you don't have a ref,
// then you can only interact with the top level instance.
// APIs like CompletedActivityByID should be a API on the workflow
func NewComponentRef(
	key InstanceKey,
	componentName string,
) ComponentRef {
	return ComponentRef{
		Key: key,
		// we probably don't even need this,
		// can make the function generic and find the name from registry
		componentName: componentName,
	}
}

func (r *ComponentRef) Serialize() ([]byte, error) {
	panic("not implemented")
}

func DeserializeComponentRef(data []byte) (ComponentRef, error) {
	panic("not implemented")
}

type SerDe interface {
	Serialize() ([]byte, error)
	Deserialize(data []byte) error
}

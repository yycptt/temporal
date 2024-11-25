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

	Ref(Component) (ComponentRef, bool)
	Now(Component) time.Time

	getContext() context.Context
}

type MutableContext interface {
	Context

	AddTask(component Component, attributes TaskAttributes, task any) error
}

type Component interface {
	RunningState() ComponentState
}

type DataField[D proto.Message] struct {
}

func NewDataField[D proto.Message](
	ctx Context,
	d D,
) *DataField[D] {
	return &DataField[D]{}
}

func (d *DataField[D]) Get(Context) (D, error) {
	panic("not implemented")
}

type ComponentField[C Component] struct {
	// TODO: this value needs to be create via reflection
	// but reflection can't set prviate fields...
	component reflect.Value

	dirty bool
}

type NewComponentOptions struct {
}

func NewComponentField[C Component](
	ctx Context,
	c C,
	options NewComponentOptions,
) *ComponentField[C] {
	// if C is an interface, ignore c and find the ctor from registry

	return &ComponentField[C]{
		dirty:     true,
		component: reflect.ValueOf(c),
	}
}

func (c *ComponentField[C]) Get(
	ctx Context,
) (C, error) {
	// handle c == nil

	panic("not implemented")
}

type ComponentMap[C Component] map[string]*ComponentField[C]

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

func (r *ComponentRef) Path() []string {
	panic("not implemented")
}

func DeserializeComponentRef(data []byte) (ComponentRef, error) {
	panic("not implemented")
}

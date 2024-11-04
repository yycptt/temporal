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
	LifeCycleOption(Component) LifecycleOption
	Now(Component) time.Time
	Intent() OperationIntent

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
	LifecycleOption LifecycleOption
}

type LifecycleOption int

const (
	LifecycleOptionAbandon LifecycleOption = 1 << iota
	LifecycleOptionBlock
)

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

func NewComponentForInterface[I Component](ctx Context) (I, error) {
	// find the ctor for I from registry
	// call the ctor

	panic("not implemented")
}

func (c *ComponentField[C]) Get(
	ctx Context,
) (C, error) {
	// handle c == nil

	panic("not implemented")
}

type ComponentMap[C Component] map[string]*ComponentField[C]

func (m ComponentMap[C]) Get(
	ctx Context,
	key string,
) (*ComponentField[C], error) {
	panic("not implemented")
}

func (m ComponentMap[C]) Set(
	ctx Context,
	key string,
	c C,
) {
	panic("not implemented")
}

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

// type Event[E any, C any] struct {
// 	RawEvent        E
// 	SourceComponent C
// }

// type EventListener[E any, C any] func(Context, Event[E, C]) error

type OperationInterceptor[P, C Component] func(P, MutableContext, C, func() error) error

// type ChildOperationRule[P, C Component] func(P, Context, C) bool

// type ChildStateChangeListener[P, C Component] struct {
// 	PredicateFn func(Context, C) (bool, error)
// 	UpdateFn    func(MutableContext, P, C) error
// }

// cases when workflow is closed
// 1. describe activity, don't want to intercept
// 2. heatbeat activity, want to intercept, write operation
// 3. complete activity, want to intercept, write operation
// 4. dispatch activity, want to intercept, read operation

// parent close policy = terminate
// 5. child workflow start, want to intercept, read + write operation

// parent close policy = abandon
// 6. child workflow start, don't want to intercept read + write operation

// 7. record child completion, want to intercept, write operation

// 8. report to parent, process parent close policy, update es, don't want to intercept, read operation
// 9. callback, don't want to intercept, write operation.
// Except update ES, we do want intercept if got reset
// But ES is kinda special. it's a sync storage provided by the framework.

// 10. query, don't want to intercept, read operation

// for 5 and 6, if you model this using marker interface, which is static,
// this policy has to be on the parent, since it's dynamic.
// you can't say the startChildTask has marker proceedOnClose()

// for 8 and 9, it's notifying external about the current state of the component,
// very similar to observe, but original is different. one from outside, one from internal.
// Also notification should not be delievered if component got reset.

// the marker interface can only define the semantic of the operation,
// not the result (execute or not is a result)

// so markers should be like
type OperationProgressBase interface {
	mustEmbedOperationIntentProgress()
}
type OperationObserveBase interface {
	mustEmbedOperationIntentObserve()
} // by definition should only be used by api
type OperationNotificationBase interface {
	mustEmbedOperationIntentNotification()
} // by definition should only be used by tasks

type OperationIntent int

const (
	OperationIntentProgress OperationIntent = 1 << iota
	OperationIntentObserve
	OperationIntentNotification
)

// each component can define it's own marker interface
// but the semantic should focus on the operation itself.

func NewDefaultChildOperationRule[P, C Component]() RegistrableChildOperationRule[P] {
	return NewRegistrableChildOperationRule[P, C](
		ShouldContinueOperation,
	)
}

func ShouldContinueOperation[P, C Component](
	parentComponent P,
	ctx Context,
	childComponent C,
) bool {
	// default pre-interceptor logic
	// component in running state => allow all three
	// component in paused state => block progress
	// component in completed/failed => block progress
	// component in reset => block progress and notification

	panic("not implemented")
}

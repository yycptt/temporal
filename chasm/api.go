package chasm

import (
	"context"
	"errors"
	"iter"
	"reflect"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
)

// Names in this API are not final, structure is up for debate.
// Alternatives for Instance: StateMachine, Entity, Process, Execution.

type InstanceKey struct {
	NamespaceID, BusinessID, InstanceID string
}

// This will be a proto enum.
type InstanceState int

const (
	InstanceStateRunning = InstanceState(iota)
	InstanceStateClosed
	InstanceStateZombie // hidden?
)

// If everything is an Instance, it should have these fields (and some others).
type Instance struct {
	InstanceKey

	State     InstanceState
	StartTime time.Time
	CloseTime time.Time
}

type Component any

type ComponentTypeOptions struct {
	TypeName string
}

type ComponentType struct {
	ComponentTypeOptions

	typ reflect.Type
}

func NewComponentType[T Component](opts ComponentTypeOptions) ComponentType {
	var t [0]T
	typ := reflect.TypeOf(t).Elem()

	return ComponentType{opts, typ}
}

type ReadContext interface {
	Instance() *Instance
	Child(component Component, path ...string) (Component, bool)
	Walk(component Component) iter.Seq2[[]string, Component]
}

type WriteContext interface {
	ReadContext
	AddTask(component Component, task Task)
	addChild(key string, component Component)
}

func NewComponent[T Component](ctx WriteContext) T {
	panic("todo")
}

func ChildComponent[T Component](ctx ReadContext, comp Component, key ...string) (T, bool) {
	c, ok := ctx.Child(comp, key...)
	if !ok {
		var zero T
		return zero, false
	}
	return c.(T), true
}

type Map[T Component] struct {
	parent Component
	key    string
	rctx   ReadContext
	wctx   WriteContext
}

func (c *Map[T]) Get(key string) (T, bool) {
	return ChildComponent[T](c.rctx, c.parent, c.key, key)
}

func (c *Map[T]) Set(key string, value T) {
	// TODO
	panic("not implemented")
}

type Ptr[T Component] struct {
	parent Component
	key    string
	rctx   ReadContext
	wctx   WriteContext
}

func (c *Ptr[T]) Get() (T, bool) {
	return ChildComponent[T](c.rctx, c.parent, c.key)
}

func (c *Ptr[T]) MustGet() T {
	panic("todo")
}

func (c *Ptr[T]) Set(value T) {
	// TODO
	panic("not implemented")
}

// func SpawnChild[T Component, I any](c *ComponentHandle[T], input I, init func(ctx WriteContext, instance T, input I) error) error {
// 	// TODO: c.wctx.addChild(key, comp)
// 	panic("todo")
// }

type StorageOptions interface {
	mustImplmenentStorageOptions()
}

type StorageOptionsPersistent struct {
}

func (StorageOptionsPersistent) mustImplmenentStorageOptions() {}

type StorageOptionsEphemeralLRU struct {
}

func (StorageOptionsEphemeralLRU) mustImplmenentStorageOptions() {}

type StorageOptionsHistory struct {
}

func (StorageOptionsHistory) mustImplmenentStorageOptions() {}

type ComponentOptions struct {
	Storage StorageOptions
}

var Immediate = time.Time{}

type Task interface {
	Attributes() TaskAttributes
}

type TaskAttributes struct {
	Deadline time.Time
	// This approach works for 99% of the use cases we have in mind.
	Destination string
	// If we need more flexibility, e.g. tiered storage and visibility, we can potentially make this more generic:
	// Queue() tasks.Category
	// Tags() map[string]string
}

type ConsistencyToken []byte

type Ref struct {
	InstanceKey      InstanceKey
	ComponentPath    []string
	ConsistencyToken ConsistencyToken

	// Optional. May be set for task executor.
	// There are other ways to do this, TBD.
	Validate func(ctx ReadContext, component Component) error
}

var ErrStaleReference = errors.New("stale reference")

type TaskOptions[T Task] interface {
	Validate(ctx ReadContext, component Component, task T) error
	Execute(ctx EngineContext, ref Ref, task T) error
}

type untypedTaskOptions struct {
}

func newUntypedTaskOptions[T Task](opts TaskOptions[T]) untypedTaskOptions {
	return untypedTaskOptions{} // TODO
}

type TaskType struct {
	untypedTaskOptions
	typ reflect.Type
}

func NewTaskType[T Task](opts TaskOptions[T]) TaskType {
	var t [0]T
	typ := reflect.TypeOf(t).Elem()
	return TaskType{
		untypedTaskOptions: newUntypedTaskOptions(opts),
		typ:                typ,
	}
}

func (t TaskType) ReflectType() reflect.Type {
	return t.typ
}

type Library interface {
	Components() []ComponentType
	Tasks() []TaskType
	Services() []*nexus.Service
}

type EngineContext interface {
	context.Context

	createInstance(key InstanceKey, ctor func(ctx WriteContext) (Component, error), options ComponentOptions) error
	upsertInstance(key InstanceKey, token ConsistencyToken, ctor func(ctx WriteContext, root Component) (Component, error)) error

	// Do we just want functions to access components directly?
	updateInstance(key InstanceKey, token ConsistencyToken, ctor func(ctx WriteContext, root any) error) error
	readInstance(key InstanceKey, token ConsistencyToken, ctor func(ctx ReadContext, root any) error) error

	updateComponent(ref Ref, ctor func(ctx WriteContext, comp any) error) error
	readComponent(ref Ref, ctor func(ctx ReadContext, comp any) error) error
}

func CreateInstance[T any, I any](ctx EngineContext, key InstanceKey, ctor func(ctx WriteContext, input I) (T, error), input I, options ComponentOptions) error {
	return ctx.createInstance(key, func(ctx WriteContext) (Component, error) {
		return ctor(ctx, input)
	}, options)
}

func UpdateInstance[T any, I any](context.Context, InstanceKey, ConsistencyToken, func(root T, input I) error) error {
	panic("not implemented")
}

type NoValue *struct{}

func Execute[T Component, C ReadContext, I any, O any](
	ctx EngineContext,
	ref Ref,
	fn func(comp T, ctx C, input I) (O, error),
	input I,
) (O, error) {
	panic("not implemented")
}

// Alternative:
type Registry interface {
}

func RegisterTask[T Task](reg Registry, opts TaskOptions[T]) {
}

func RegisterService(reg Registry, service *nexus.Service) {
}

type syncOperation[I, O any] struct {
	nexus.UnimplementedOperation[I, O]

	Handler func(EngineContext, I, nexus.StartOperationOptions) (O, error)
	name    string
}

// NewSyncOperation is a helper for creating a synchronous-only [Operation] from a given name and handler function.
func NewSyncOperation[I, O any](name string, handler func(EngineContext, I, nexus.StartOperationOptions) (O, error)) nexus.Operation[I, O] {
	return &syncOperation[I, O]{
		name:    name,
		Handler: handler,
	}
}

// Name implements Operation.
func (h *syncOperation[I, O]) Name() string {
	return h.name
}

// Start implements Operation.
func (h *syncOperation[I, O]) Start(ctx context.Context, input I, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[O], error) {
	o, err := h.Handler(ctx.(EngineContext), input, options)
	if err != nil {
		return nil, err
	}
	return &nexus.HandlerStartOperationResultSync[O]{Value: o}, err
}

func NewReadOperation[I, O any, C Component](name string, handler func(comp C, ctx ReadContext, i I) (O, error)) nexus.Operation[I, O] {
	return NewSyncOperation(name, func(ctx EngineContext, i I, opts nexus.StartOperationOptions) (O, error) {
		ref := Ref{} // TODO: extract from request.
		return Execute(ctx, ref, handler, i)
	})
}

func NewWriteOperation[I, O any, C Component](name string, handler func(comp C, ctx WriteContext, i I) (O, error)) nexus.Operation[I, O] {
	return NewSyncOperation(name, func(ctx EngineContext, i I, opts nexus.StartOperationOptions) (O, error) {
		ref := Ref{} // TODO: extract from request.
		return Execute(ctx, ref, handler, i)
	})
}

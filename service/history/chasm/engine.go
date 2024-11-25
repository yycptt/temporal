package chasm

import (
	"context"
)

type Engine interface {
	newInstance(
		context.Context,
		InstanceKey,
		BusinessIDReusePolicy,
		DynamicInstanceOptions,
		func(MutableContext) (Component, error),
	) (ComponentRef, error)
	updateWithNewInstance(
		context.Context,
		InstanceKey,
		DynamicInstanceOptions,
		[]ComponentPath,
		func(MutableContext) (Component, error),
		func(MutableContext, Component) error,
	) (ComponentRef, error)

	updateComponent(
		context.Context,
		ComponentRef,
		[]ComponentPath,
		func(MutableContext, Component) error,
	) (ComponentRef, error)
	readComponent(
		context.Context,
		ComponentRef,
		func(Context, Component) error,
	) error

	pollComponent(
		context.Context,
		ComponentRef,
		func(Context, Component) (bool, error),
		func(MutableContext, Component) error,
	) (ComponentRef, error)
}

type BusinessIDReusePolicy int

const (
	BusinessIDReusePolicyAllowDuplicate BusinessIDReusePolicy = iota
	BusinessIDReusePolicyRejectDuplicate
)

// TODO: same for IDConflictPolicy

type NewInstanceRequest[C Component, I any, O any] struct {
	Key                    InstanceKey
	IDReusePolicy          BusinessIDReusePolicy
	DynamicInstanceOptions DynamicInstanceOptions
	NewFn                  func(MutableContext, I) (C, O, error)
	Input                  I
}

func NewInstance[C Component, I any, O any](
	ctx context.Context,
	request NewInstanceRequest[C, I, O],
) (O, ComponentRef, error) {
	var output O
	ref, err := engineFromContext(ctx).newInstance(
		ctx,
		request.Key,
		request.IDReusePolicy,
		request.DynamicInstanceOptions,
		func(ctx MutableContext) (Component, error) {
			var c C
			var err error
			c, output, err = request.NewFn(ctx, request.Input)
			return c, err
		},
	)
	return output, ref, err
}

type UpdateWithNewInstanceRequest[C Component, I any, O1 any, O2 any] struct {
	Key                    InstanceKey
	DynamicInstanceOptions DynamicInstanceOptions
	NewFn                  func(MutableContext, I) (C, O1, error)
	UpdateFn               func(C, MutableContext, I) (O2, error)
	Input                  I

	EagerLoadPaths []ComponentPath // relative to the component (instance here)
}

func UpdateWithNewInstance[C Component, I any, O1 any, O2 any](
	ctx context.Context,
	request UpdateWithNewInstanceRequest[C, I, O1, O2],
) (O1, O2, ComponentRef, error) {
	var output1 O1
	var output2 O2
	ref, err := engineFromContext(ctx).updateWithNewInstance(
		ctx,
		request.Key,
		request.DynamicInstanceOptions,
		request.EagerLoadPaths,
		func(ctx MutableContext) (Component, error) {
			var c C
			var err error
			c, output1, err = request.NewFn(ctx, request.Input)
			return c, err
		},
		func(ctx MutableContext, c Component) error {
			var err error
			output2, err = request.UpdateFn(c.(C), ctx, request.Input)
			return err
		},
	)
	return output1, output2, ref, err
}

type UpdateComponentRequest[C Component, I any, O any] struct {
	Ref      ComponentRef
	UpdateFn func(C, MutableContext, I) (O, error)
	Input    I

	// relative to the component, serve as additional seeds for lookup
	EagerLoadPaths []ComponentPath
}

func UpdateComponent[C Component, I any, O any](
	ctx context.Context,
	request UpdateComponentRequest[C, I, O],
) (O, ComponentRef, error) {
	var output O
	ref, err := engineFromContext(ctx).updateComponent(
		ctx,
		request.Ref,
		request.EagerLoadPaths,
		func(ctx MutableContext, c Component) error {
			var err error
			output, err = request.UpdateFn(c.(C), ctx, request.Input)
			return err
		},
	)
	return output, ref, err
}

type ReadComponentRequest[C Component, I any, O any] struct {
	Ref    ComponentRef
	ReadFn func(C, Context, I) (O, error)
	Input  I
}

func ReadComponent[C Component, I any, O any](
	ctx context.Context,
	request ReadComponentRequest[C, I, O],
) (O, error) {
	var output O
	err := engineFromContext(ctx).readComponent(
		ctx,
		request.Ref,
		func(ctx Context, c Component) error {
			var err error
			output, err = request.ReadFn(c.(C), ctx, request.Input)
			return err
		},
	)
	return output, err
}

type PollComponentRequest[C Component, I any, O any] struct {
	Ref ComponentRef
	// NOTE: this reason we can do this is because those polling requests are dynamically
	// added, so if the predicateFn passed and operationFn executed,
	// we don't need to worry about a later change will pass the predicate again, because the pollRequest
	// is already gone.
	// This is very different from parent listen to child's state changes, which needs to be
	// specified statically. and we can't keep triggering the same operation, over and over again.
	PredicateFn func(C, Context, I) (bool, error)
	OperationFn func(C, MutableContext, I) (O, error)
	Input       I
}

func PollComponent[C Component, I any, O any](
	ctx context.Context,
	request PollComponentRequest[C, I, O],
) (O, ComponentRef, error) {
	var output O
	ref, err := engineFromContext(ctx).pollComponent(
		ctx,
		request.Ref,
		func(ctx Context, c Component) (bool, error) {
			return request.PredicateFn(c.(C), ctx, request.Input)
		},
		func(ctx MutableContext, c Component) error {
			var err error
			output, err = request.OperationFn(c.(C), ctx, request.Input)
			return err
		},
	)
	return output, ref, err
}

type engineCtxKeyType string

const engineCtxKey engineCtxKeyType = "chasmEngine"

func NewEngineContext(
	ctx context.Context,
	engine Engine,
) context.Context {
	return context.WithValue(ctx, engineCtxKey, engine)
}

func engineFromContext(
	ctx context.Context,
) Engine {
	return ctx.Value(engineCtxKey).(Engine)
}

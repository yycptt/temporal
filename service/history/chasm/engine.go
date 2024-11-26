package chasm

import (
	"context"
)

type Engine interface {
	newInstance(
		context.Context,
		InstanceKey,
		func(MutableContext) (Component, error),
		...EngineOption,
	) (ComponentRef, error)
	updateWithNewInstance(
		context.Context,
		InstanceKey,
		func(MutableContext) (Component, error),
		func(MutableContext, Component) error,
		...EngineOption,
	) (ComponentRef, error)

	updateComponent(
		context.Context,
		ComponentRef,
		func(MutableContext, Component) error,
		...EngineOption,
	) (ComponentRef, error)
	readComponent(
		context.Context,
		ComponentRef,
		func(Context, Component) error,
		...EngineOption,
	) error

	pollComponent(
		context.Context,
		ComponentRef,
		func(Context, Component) bool,
		func(MutableContext, Component) error,
		...EngineOption,
	) (ComponentRef, error)
}

type BusinessIDReusePolicy int

const (
	BusinessIDReusePolicyAllowDuplicate BusinessIDReusePolicy = iota
	BusinessIDReusePolicyRejectDuplicate
)

type BusinessIDConflictPolicy int

const (
	BusinessIDConflictPolicyFail BusinessIDConflictPolicy = iota
	BusinessIDConflictPolicyTermiateExisting
	BusinessIDConflictPolicyUseExisting
)

type engineOptions struct {
}

type EngineOption func(*engineOptions)

func EngineStorageOption(
	option InstanceStorageOption,
) EngineOption {
	panic("not implemented")
}

func EngineReplicationOption(
	option InstanceReplicationOption,
) EngineOption {
	panic("not implemented")
}

func EngineEagerLoadOption(
	paths []ComponentPath,
) EngineOption {
	panic("not implemented")
}

func EngineIDReusePolicyOption(
	reusePolicy BusinessIDReusePolicy,
	conflictPolicy BusinessIDConflictPolicy,
) EngineOption {
	panic("not implemented")
}

func NewInstance[C Component, I any, O any](
	ctx context.Context,
	key InstanceKey,
	newFn func(MutableContext, I) (C, O, error),
	input I,
	opts ...EngineOption,
) (O, ComponentRef, error) {
	var output O
	ref, err := engineFromContext(ctx).newInstance(
		ctx,
		key,
		func(ctx MutableContext) (Component, error) {
			var c C
			var err error
			c, output, err = newFn(ctx, input)
			return c, err
		},
		opts...,
	)
	return output, ref, err
}

func UpdateWithNewInstance[C Component, I any, O1 any, O2 any](
	ctx context.Context,
	key InstanceKey,
	newFn func(MutableContext, I) (C, O1, error),
	updateFn func(C, MutableContext, I) (O2, error),
	input I,
	opts ...EngineOption,
) (O1, O2, ComponentRef, error) {
	var output1 O1
	var output2 O2
	ref, err := engineFromContext(ctx).updateWithNewInstance(
		ctx,
		key,
		func(ctx MutableContext) (Component, error) {
			var c C
			var err error
			c, output1, err = newFn(ctx, input)
			return c, err
		},
		func(ctx MutableContext, c Component) error {
			var err error
			output2, err = updateFn(c.(C), ctx, input)
			return err
		},
		opts...,
	)
	return output1, output2, ref, err
}

func UpdateComponent[C Component, I any, O any](
	ctx context.Context,
	ref ComponentRef,
	updateFn func(C, MutableContext, I) (O, error),
	input I,
	opts ...EngineOption,
) (O, ComponentRef, error) {
	var output O
	ref, err := engineFromContext(ctx).updateComponent(
		ctx,
		ref,
		func(ctx MutableContext, c Component) error {
			var err error
			output, err = updateFn(c.(C), ctx, input)
			return err
		},
		opts...,
	)
	return output, ref, err
}

func ReadComponent[C Component, I any, O any](
	ctx context.Context,
	ref ComponentRef,
	readFn func(C, Context, I) (O, error),
	input I,
	opts ...EngineOption,
) (O, error) {
	var output O
	err := engineFromContext(ctx).readComponent(
		ctx,
		ref,
		func(ctx Context, c Component) error {
			var err error
			output, err = readFn(c.(C), ctx, input)
			return err
		},
		opts...,
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
	PredicateFn func(C, Context, I) bool
	OperationFn func(C, MutableContext, I) (O, error)
	Input       I
}

func PollComponent[C Component, I any, O any](
	ctx context.Context,
	ref ComponentRef,
	predicateFn func(C, Context, I) bool,
	operationFn func(C, MutableContext, I) (O, error),
	input I,
	opts ...EngineOption,
) (O, ComponentRef, error) {
	var output O
	ref, err := engineFromContext(ctx).pollComponent(
		ctx,
		ref,
		func(ctx Context, c Component) bool {
			return predicateFn(c.(C), ctx, input)
		},
		func(ctx MutableContext, c Component) error {
			var err error
			output, err = operationFn(c.(C), ctx, input)
			return err
		},
		opts...,
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

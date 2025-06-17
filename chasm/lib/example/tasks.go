package example

import (
	"context"
	"fmt"

	"go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
)

type (
	PayloadTTLPureTaskExecutor  struct{}
	PayloadTTLPureTaskValidator struct{}
)

func (e *PayloadTTLPureTaskExecutor) Execute(
	mutableContext chasm.MutableContext,
	store *PayloadStore,
	task *persistence.PayloadTTLPureTask,
) error {
	fmt.Println("Executing PayloadTTLPureTaskExecutor for payload key:", task.PayloadKey)
	_, err := store.RemovePayload(mutableContext, task.PayloadKey)
	return err
}

func (v *PayloadTTLPureTaskValidator) Validate(
	chasmContext chasm.Context,
	store *PayloadStore,
	task *persistence.PayloadTTLPureTask,
) (bool, error) {
	expirationTime, ok := store.State.ExpirationTimes[task.PayloadKey]
	if !ok {
		return false, nil
	}

	return !expirationTime.AsTime().After(task.ExpirationTime.AsTime()), nil
}

type (
	PayloadTTLSideEffectTaskExecutor  struct{}
	PayloadTTLSideEffectTaskValidator struct{}
)

func (e *PayloadTTLSideEffectTaskExecutor) Execute(
	ctx context.Context,
	ref chasm.ComponentRef,
	task *persistence.PayloadTTLSideEffectTask,
) error {
	fmt.Println("Executing PayloadTTLSideEffectTask for payload key:", task.PayloadKey)
	_, _, err := chasm.UpdateComponent(
		ctx,
		ref,
		(*PayloadStore).RemovePayload,
		task.PayloadKey,
	)
	return err
}

func (v *PayloadTTLSideEffectTaskValidator) Validate(
	chasmContext chasm.Context,
	store *PayloadStore,
	task *persistence.PayloadTTLSideEffectTask,
) (bool, error) {
	expirationTime, ok := store.State.ExpirationTimes[task.PayloadKey]
	if !ok {
		return false, nil
	}

	return !expirationTime.AsTime().After(task.ExpirationTime.AsTime()), nil
}

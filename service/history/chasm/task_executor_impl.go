package chasm

import (
	"context"
	"reflect"
	"strings"

	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/service/history/tasks"
)

type ChasmTaskExecutor struct {
	Registry *Registry
}

func (e *ChasmTaskExecutor) Execute(
	ctx context.Context,
	shardID int32,
	task *tasks.ChasmTask,
) error {
	rt, ok := e.Registry.getTaskByName(task.Info.Name)
	if !ok {
		return serviceerror.NewInternal("task name not registered")
	}

	// deserialize the task
	t, err := e.deserializeTask(*task.Info.Blob)
	if err != nil {
		return err
	}

	handlerValue := reflect.ValueOf(rt.handler)

	ref := ComponentRef{
		EntityKey: EntityKey{
			NamespaceID: task.NamespaceID,
			BusinessID:  task.WorkflowID,
			EntityID:    task.RunID,
		},
		shardID:            shardID,
		componentPath:      strings.Split(task.Info.ComponentPath, "/"),
		componentInitialVT: task.Info.InitialVersionedTransition,
		EntityLastUpdateVT: task.Info.VersionedTransition,
		validationFn: func(
			chasmContext Context,
			instance Component,
		) error {
			// TODO: also do task generation validation here?

			ret := handlerValue.MethodByName("Validate").Call(
				[]reflect.Value{
					reflect.ValueOf(chasmContext),
					reflect.ValueOf(instance),
					reflect.ValueOf(t),
				},
			)
			return ret[0].Interface().(error)
		},
	}

	return handlerValue.MethodByName("Execute").Call(
		[]reflect.Value{
			reflect.ValueOf(ctx),
			reflect.ValueOf(ref),
			reflect.ValueOf(t),
		},
	)[0].Interface().(error)
}

func (e *ChasmTaskExecutor) deserializeTask(
	taskBlob common.DataBlob,
) (any, error) {
	panic("not implemented")
}

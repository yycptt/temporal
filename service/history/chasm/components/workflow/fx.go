package workflow

import (
	"go.temporal.io/server/service/history/chasm"
	"go.uber.org/fx"
)

type Library struct{}

func (l Library) Name() string {
	return "workflow"
}

func (l Library) Components() []chasm.RegistrableComponent {
	return []chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*WorkflowImpl](
			"",
			chasm.EventListener(NewActivityEventListener),
			chasm.OperationRule((*WorkflowImpl).CustomChildOpertionRule),
			chasm.ShardingFn(func(key chasm.InstanceKey) string {
				return key.NamespaceID + key.BusinessID
			}),
		),
	}
}

func (l Library) Tasks() []chasm.RegistrableTask {
	return []chasm.RegistrableTask{}
}

var Module = fx.Options(
	fx.Invoke(
		func(registry chasm.Registry) {
			registry.RegisterLibrary(Library{})
		},
	),
)

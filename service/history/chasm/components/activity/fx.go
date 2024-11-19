package activity

import (
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/service/history/chasm"
	"go.uber.org/fx"
)

type Library struct {
	matchingClient matchingservice.MatchingServiceClient
}

func (l Library) Name() string {
	return "activity"
}

func (l Library) Components() []chasm.RegistrableComponent {
	return []chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*ActivityImpl](
			"",
			chasm.NewComponentShardingOption(
				func(key chasm.InstanceKey) string {
					return key.NamespaceID + key.BusinessID
				},
			),
		),
	}
}

func (l Library) Tasks() []chasm.RegistrableTask {
	return []chasm.RegistrableTask{
		chasm.NewRegistrableTask(
			&DispatchTaskHandler{
				l.matchingClient,
			},
			chasm.RegistrableTaskOptions{
				Name: "dispatchTask",
			},
		),
	}
}

var Module = fx.Options(
	fx.Invoke(
		func(registry chasm.Registry, matchingClient matchingservice.MatchingServiceClient) {
			registry.RegisterLibrary(Library{matchingClient})
		},
	),
)

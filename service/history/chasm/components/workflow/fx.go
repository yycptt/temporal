package workflow

import (
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/service/history/chasm"
	"go.temporal.io/server/service/history/chasm/components/activity"
	"go.uber.org/fx"
)

type Library struct {
	MatchingClient matchingservice.MatchingServiceClient
}

func (l Library) Name() string {
	return "workflow"
}

func (l Library) Components() []chasm.RegistrableComponent {
	return []chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*WorkflowImpl](
			chasm.RegistrableComponentOptions{
				Name: "",
				StaticInstanceOptions: chasm.StaticInstanceOptions{
					ShardingOption: chasm.InstanceShardingOption{
						Sharding: func(key chasm.InstanceKey) string {
							return key.NamespaceID + key.BusinessID
						},
					},
				},
				ServiceHandlers: []chasm.RegistrableServiceHandler{
					chasm.NewRegistrableServiceHandler[activity.Service](&WorkflowActivityHandler{}),
				},
				TaskHandlers: []chasm.RegistrableTaskHandler{
					chasm.NewRegistrableTaskHandler(&ActivityDispatchTaskHandler{
						DispatchTaskHandler: activity.DispatchTaskHandler{
							MatchingClient: l.MatchingClient,
						},
					}),
				},
			},
		),
	}
}

func (l Library) Tasks() []chasm.RegistrableTask {
	panic("not implemented")
}

func (l Library) Services() []chasm.RegistrableService {
	panic("not implemented")
}

var Module = fx.Options(
	fx.Invoke(
		func(registry chasm.Registry, matchingClient matchingservice.MatchingServiceClient) {
			registry.RegisterLibrary(Library{MatchingClient: matchingClient})
		},
	),
)

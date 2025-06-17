package example

import "go.temporal.io/server/chasm"

type (
	library struct {
		chasm.UnimplementedLibrary
	}
)

var Library = &library{}

func (l *library) Name() string {
	return "example"
}

func (l *library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*PayloadStore]("payloadStore"),
	}
}

func (l *library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrablePureTask(
			"payloadStoreTTLPureTask",
			&PayloadTTLPureTaskValidator{},
			&PayloadTTLPureTaskExecutor{},
		),
		chasm.NewRegistrableSideEffectTask(
			"payloadStoreTTLSideEffectTask",
			&PayloadTTLSideEffectTaskValidator{},
			&PayloadTTLSideEffectTaskExecutor{},
		),
	}
}

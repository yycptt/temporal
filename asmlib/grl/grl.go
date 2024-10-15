package grl

import (
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/asm"
)

const (
	asmType = "grl"
)

type (
	asmImpl struct {
		logger         log.Logger
		metricsHandler metrics.Handler
	}
)

func (a *asmImpl) Type() string {
	return asmType
}

func (a *asmImpl) Options() []asm.Option {
	return []asm.Option{
		asm.OptionEphemeral,
	}
}

func (a *asmImpl) New(backend asm.InstanceBackend) asm.Instance {
	return newInstance(
		0, // from dynamic config
		a.logger,
		a.metricsHandler,
		backend,
	)
}

func (a *asmImpl) RoutingKey(key asm.Key) string {
	return key.NamespaceID + key.WorkflowID
}

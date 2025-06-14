package example

import (
	"go.temporal.io/server/chasm"
	"go.uber.org/fx"
)

var Module = fx.Module(
	"example",
	fx.Invoke(func(registry *chasm.Registry) error {
		return registry.Register(Library)
	}),
)

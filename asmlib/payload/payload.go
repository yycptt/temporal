package payload

import (
	"go.temporal.io/server/service/history/asm"
)

const (
	asmType = "payload"
)

var _ asm.ASM = (*asmImpl)(nil)

type (
	asmImpl struct{}
)

func (a *asmImpl) Type() string {
	return asmType
}

func (a *asmImpl) Options() []asm.Option {
	return nil
}

func (a *asmImpl) New(backend asm.InstanceBackend) asm.Instance {
	return &instanceImpl{
		backend: backend,
	}
}

func (a *asmImpl) SerializeInstance(
	instance asm.Instance,
) (*commonpb.DataBlob, error) {
	panic("not implemented")
}

func (a *asmImpl) DeserializeInstance(
	backend asm.InstanceBackend,
	blob *commonpb.DataBlob,
) (asm.Instance, error) {
	panic("not implemented")
}

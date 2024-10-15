package payload

import (
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/service/history/asm"
)

type (
	instanceImpl struct {
		backend asm.InstanceBackend

		payload *commonpb.Payload
	}
)

func (i *instanceImpl) Type() string {
	return asmType
}

func (i *instanceImpl) Set(
	payload *commonpb.Payload,
) {
	i.payload = payload
}

func (i *instanceImpl) Get() *commonpb.Payload {
	// read only, no need to call CloseTransition

	return i.payload
}

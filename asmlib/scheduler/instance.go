package scheduler

import (
	"go.temporal.io/api/common/v1"
	"go.temporal.io/server/asmlib/payload"
	"go.temporal.io/server/service/history/asm"
)

type (
	instanceImpl struct {
		backend asm.InstanceBackend
	}
)

type (
	StartScheduleArgs struct {
		ActionPayload []byte
	}
)

func (i *instanceImpl) Start(args StartScheduleArgs) error {
	var payloadInstance payload.Payload
	if err := i.backend.NewChildASM("start-payload", &payloadInstance); err != nil {
		return err
	}
	payloadInstance.Set(&common.Payload{
		Data: args.ActionPayload,
	})

	// other logic

	return nil
}

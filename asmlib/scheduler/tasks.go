package scheduler

import (
	"context"

	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/asmlib/payload"
	"go.temporal.io/server/service/history/asm"
)

type (
	runTask struct{}

	runTaskHandler struct{}
)

func (h *runTaskHandler) Execute(
	ctx context.Context,
	task asm.Task,
	instanceRef asm.InstanceRef,
	engine asm.Engine,
) error {

	var startWfRequest workflowservice.StartWorkflowExecutionRequest
	if _, err := engine.Read(ctx, asm.ReadRequest{
		Ref: instanceRef,
		ReadFn: func(instance asm.Instance) error {
			scheduler := instance.(*instanceImpl)
			var payload payload.Payload
			if err := scheduler.backend.ReadChildASM("start-payload", &payload); err != nil {
				return err
			}
			startWfRequest.Input = &common.Payloads{
				Payloads: []*common.Payload{payload.Get()},
			}
			return nil
		},
	}); err != nil {
		return err
	}

	// make start workflow call

	// update scheduler
	// engine.Update(ctx, asm.UpdateRequest{}) ...

	return nil
}

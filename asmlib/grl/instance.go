package grl

import (
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/asm"
)

type priorityRateLimiter interface {
	// omitted
}

type (
	instanceImpl struct {
		backend asm.InstanceBackend

		logger         log.Logger
		metricsHandler metrics.Handler

		// NOTE: no lock here

		rl         priorityRateLimiter // priorityRateLimiter used to rate limit requests
		timeWindow int                 // Time window as number of seconds for which this tokenVendor should allocate tokens
		priorities int                 // Number of priorities. Valid priorities are 0 to priorities-1
		rate       float64             // rate value given to  priorityRateLimiter
		burstRatio float64             // burstRatio is used to calculate the burst value given to priorityRateLimiter
		ts         clock.TimeSource
	}
)

func newInstance(
	timeWindow int,
	logger log.Logger,
	metricsHandler metrics.Handler,
	backend asm.InstanceBackend,
) *instanceImpl {
	return &instanceImpl{
		backend: backend,

		logger:         logger,
		metricsHandler: metricsHandler,

		timeWindow: timeWindow,
		ts:         clock.NewRealTimeSource(),
	}
}

func (i *instanceImpl) Type() string {
	return asmType
}

func (i *instanceImpl) Init(
	rate float64,
	burstRatio float64,
	priorities int32,
) {
	panic("not implemented")
}

func (i *instanceImpl) GetTokens(
	priorityTokenRate map[int32]float64,
) (int64, time.Duration) {
	panic("not implemented")
}

func (i *instanceImpl) Update(
	rate float64, burstRatio float64,
) {
	panic("not implemented")
}

func (i *instanceImpl) State() enumsspb.WorkflowExecutionState {
	panic("ephemeral asm doesn't need running state")
}

func (i *instanceImpl) Terminate() error {
	panic("ephemeral asm doesn't need termination")
}

package activity

import (
	"time"

	"go.temporal.io/api/common/v1"
)

type ActivityEventListener interface {
	OnStart(ActivityStartedEvent) error
	OnCompletion(ActivityCompletedEvent) error
}

type ActivityStartedEvent struct {
	StartTime time.Time
}

type ActivityCompletedEvent struct {
	Output *common.Payload
}

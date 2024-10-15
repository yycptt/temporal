package activity

import (
	"time"

	commonpb "go.temporal.io/api/common/v1"
)

type (
	ScheduledEventAttributes struct {
		ScheduledTime time.Time
		Input         *commonpb.Payload
	}
)

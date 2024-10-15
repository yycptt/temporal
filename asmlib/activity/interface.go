package activity

import (
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/service/history/asm"
)

type Activity interface {
	Schedule(input commonpb.Payload) error
	RecordStarted(RecordStartedRequest) (RecordStartedResponse, error)
	RecordCompleted(RecordCompletedRequest) (RecordCompletedResponse, error)
	RecordFailed(RecordFailedRequest) (RecordFailedResponse, error)
	RecordTimedOut(RecordTimedOutRequest) (RecordTimedOutResponse, error)
	RecordHeartbeat(RecordHeartbeatRequest) (RecordHeartbeatResponse, error)

	Describe() DescribeResponse
}

type (
	ActivityStartedEvent      struct{}
	ActivityTimedOutEvent     struct{}
	ActivityCompletedOutEvent struct{}
)

type (
	RecordStartedRequest  struct{}
	RecordStartedResponse struct{}

	RecordCompletedRequest  struct{}
	RecordCompletedResponse struct{}

	RecordFailedRequest  struct{}
	RecordFailedResponse struct{}

	RecordTimedOutRequest  struct{}
	RecordTimedOutResponse struct{}

	RecordHeartbeatRequest  struct{}
	RecordHeartbeatResponse struct{}

	DescribeResponse struct {
		ScheduledTime       time.Time
		StartedTime         time.Time
		CompletedTime       time.Time
		Attempt             int
		ScheduledEventToken asm.HistoryEventToken
	}
)

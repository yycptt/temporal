package activity

import (
	"time"

	"go.temporal.io/server/service/history/chasm"
)

type Activity interface {
	chasm.Component // TODO: this is just an interceptor interface, should not require chasm.Component

	Schedule(chasm.MutableContext, *ScheduleRequest) (*ScheduleResponse, error)
	RecordStarted(chasm.MutableContext, *RecordStartedRequest) (*RecordStartedResponse, error)
	RecordCompleted(chasm.MutableContext, *RecordCompletedRequest) (*RecordCompletedResponse, error)
	Describe(chasm.Context, *DescribeActivityRequest) (*DescribeActivityResponse, error)
}

type ScheduleRequest struct {
	Input []byte
}
type ScheduleResponse struct{}

type RecordStartedRequest struct {
	RefToken []byte
}

type RecordStartedResponse struct {
	RefToken []byte
	Input    []byte
}

type RecordCompletedRequest struct {
	RefToken []byte
	Output   []byte
}

type RecordCompletedResponse struct{}

type DescribeActivityRequest struct{}

type DescribeActivityResponse struct {
	IsAbandonded  bool
	StartedTime   time.Time
	CompletedTime time.Time
}

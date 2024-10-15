package activity

import (
	"errors"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/service/history/asm"
)

type (
	instanceImpl struct {
		backend asm.InstanceBackend

		scheduledTime time.Time
		startedTime   time.Time
		completedTime time.Time
		hbTimeoutTime time.Time

		scheduleToStartTimeout time.Duration
		startToCloseTimeout    time.Duration
		scheduleToCloseTimeout time.Duration
		hbTimeout              time.Duration

		scheduledEventToken asm.HistoryEventToken
	}
)

func newInstance(
	backend asm.InstanceBackend,
) *instanceImpl {
	return &instanceImpl{
		backend: backend,
	}
}

func (i *instanceImpl) Type() string {
	return asmType
}

func (i *instanceImpl) Schedule(
	input *commonpb.Payload,
	scheduleToStartTimeout time.Duration,
	startToCloseTimeout time.Duration,
	scheduleToCloseTimeout time.Duration,
) error {
	now := i.backend.Now()
	events := i.backend.AddHistoryEvents(&ScheduledEventAttributes{
		ScheduledTime: now,
		Input:         input,
	})
	eventToken, ok := events[0].Token()
	if !ok {
		return errors.New("unable to get token for scheduled event")
	}
	i.scheduledEventToken = eventToken
	i.scheduledTime = now
	i.scheduleToStartTimeout = scheduleToStartTimeout
	i.startToCloseTimeout = startToCloseTimeout
	i.scheduleToCloseTimeout = scheduleToCloseTimeout
	i.backend.AddTasks(
		&DispatchTask{},
		&TimeoutTask{
			timeoutTime: now.Add(scheduleToStartTimeout),
			timeoutType: timeoutTypeScheduleToStart,
		},
		&TimeoutTask{
			timeoutTime: now.Add(startToCloseTimeout),
			timeoutType: timeoutTypeScheduleToClose,
		},
	)
	return nil
}

func (i *instanceImpl) RecordStarted(RecordStartedRequest) (RecordStartedResponse, error) {
	now := i.backend.Now()
	i.startedTime = now
	i.hbTimeoutTime = now.Add(i.hbTimeout)
	// TODO: add started event
	i.backend.AddTasks(
		&DispatchTask{},
		&TimeoutTask{
			timeoutTime: now.Add(i.startToCloseTimeout),
			timeoutType: timeoutTypeScheduleToStart,
		},
		&TimeoutTask{
			timeoutTime: i.hbTimeoutTime,
			timeoutType: timeoutTypeHeartbeat,
		},
	)
	return RecordStartedResponse{}, nil
}

func (i *instanceImpl) RecordCompleted(RecordCompletedRequest) (RecordCompletedResponse, error) {
	panic("not implemented")
}

func (i *instanceImpl) RecordFailed(RecordFailedRequest) (RecordFailedResponse, error) {
	panic("not implemented")
}

func (i *instanceImpl) RecordTimedOut(RecordTimedOutRequest) (RecordTimedOutResponse, error) {
	panic("not implemented")
}

func (i *instanceImpl) RecordHeartbeat(RecordHeartbeatRequest) (RecordHeartbeatResponse, error) {
	panic("not implemented")
}

func (i *instanceImpl) Describe() {
	panic("not implemented")
}

func (i *instanceImpl) Terminate() error {
	panic("not implemented")
}

func (i *instanceImpl) State() enumsspb.WorkflowExecutionState {
	// check scheduled/started/completed time and return the state
}

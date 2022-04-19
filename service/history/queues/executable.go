// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination executable_mock.go

package queues

import (
	"context"
	"sync"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/tasks"
)

type (
	Executable interface {
		ctasks.PriorityTask

		Task() tasks.Task
		Attempt() int
		Logger() log.Logger

		QueueType() QueueType
	}

	Executor interface {
		Execute(context.Context, Executable) error
	}

	// TaskFilter determines if the given task should be executed
	// TODO: remove after merging active/standby queue processor
	// task should always be executed as active or verified as standby
	TaskFilter func(task tasks.Task) bool
)

var (
	schedulerRetryPolicy = common.CreateTaskProcessingRetryPolicy()
	reschedulePolicy     = common.CreateTaskReschedulePolicy()
)

const (
	resubmitMaxAttempts = 10
)

type (
	executableImpl struct {
		sync.Mutex
		state    ctasks.State
		priority int
		attempt  int

		task        tasks.Task
		executor    Executor
		scheduler   Scheduler
		rescheduler Rescheduler
		timeSource  clock.TimeSource

		loadTime           time.Time
		userLatency        time.Duration
		logger             log.Logger
		scope              metrics.Scope
		criticalRetryCount dynamicconfig.IntPropertyFn

		queueType     QueueType
		filter        TaskFilter
		shouldProcess bool
	}
)

func NewExecutable(
	task tasks.Task,
	filter TaskFilter,
	executor Executor,
	scheduler Scheduler,
	rescheduler Rescheduler,
	timeSource clock.TimeSource,
	namespaceRegistry namespace.Registry,
	logger log.Logger,
	metricsClient metrics.Client,
	config *configs.Config,
	queueType QueueType,
) Executable {
	logger = tasks.InitializeLogger(task, logger)

	var scopeIdx int
	switch queueType {
	case QueueTypeActiveTransfer:
		scopeIdx = tasks.GetActiveTransferTaskMetricsScope(task)
	case QueueTypeStandbyTransfer:
		scopeIdx = tasks.GetStandbyTransferTaskMetricsScope(task)
	case QueueTypeActiveTimer:
		scopeIdx = tasks.GetActiveTimerTaskMetricScope(task)
	case QueueTypeStandbyTimer:
		scopeIdx = tasks.GetStandbyTimerTaskMetricScope(task)
	case QueueTypeVisibility:
		scopeIdx = tasks.GetVisibilityTaskMetricsScope(task)
	}
	scope := metricsClient.Scope(scopeIdx, getNamespaceTagByID(
		namespaceRegistry,
		namespace.ID(task.GetNamespaceID()),
		logger,
	))

	return &executableImpl{
		state:              ctasks.TaskStatePending,
		attempt:            1,
		task:               task,
		executor:           executor,
		scheduler:          scheduler,
		rescheduler:        rescheduler,
		timeSource:         timeSource,
		loadTime:           timeSource.Now(),
		logger:             logger,
		scope:              scope,
		queueType:          queueType,
		criticalRetryCount: config.TransferTaskMaxRetryCount,
		filter:             filter,
	}
}

func (e *executableImpl) Execute() error {
	// this filter should also contain the logic for overriding
	// results from task allocator (force executing some standby task types)
	e.shouldProcess = e.filter(e.task)
	if !e.shouldProcess {
		return nil
	}

	ctx := metrics.AddMetricsContext(context.Background())
	startTime := e.timeSource.Now()
	err := e.executor.Execute(ctx, e)
	var userLatency time.Duration
	if duration, ok := metrics.ContextCounterGet(ctx, metrics.HistoryWorkflowExecutionCacheLatency); ok {
		userLatency = time.Duration(duration)
	}
	e.userLatency += userLatency

	e.scope.IncCounter(metrics.TaskRequests)
	e.scope.RecordTimer(metrics.TaskProcessingLatency, time.Since(startTime))
	e.scope.RecordTimer(metrics.TaskNoUserProcessingLatency, time.Since(startTime)-userLatency)
	return err
}

func (e *executableImpl) HandleErr(err error) (retErr error) {
	defer func() {
		if retErr != nil {
			e.Lock()
			defer e.Unlock()

			e.attempt++
			if e.attempt > e.criticalRetryCount() {
				e.scope.RecordDistribution(metrics.TaskAttemptTimer, e.attempt)
				e.logger.Error("Critical error processing task, retrying.", tag.Error(err), tag.OperationCritical)
			}
		}
	}()

	if err == nil {
		return nil
	}

	if _, ok := err.(*serviceerror.NotFound); ok {
		return nil
	}

	if err == consts.ErrTaskRetry {
		e.scope.IncCounter(metrics.TaskStandbyRetryCounter)
		return err
	}

	if err == consts.ErrTaskDiscarded {
		e.scope.IncCounter(metrics.TaskDiscarded)
		return nil
	}

	// this is a transient error
	// TODO remove this error check special case
	//  since the new task life cycle will not give up until task processed / verified
	if _, ok := err.(*serviceerror.NamespaceNotActive); ok {
		if e.timeSource.Now().Sub(e.loadTime) > 2*namespace.CacheRefreshInterval {
			e.scope.IncCounter(metrics.TaskNotActiveCounter)
			return nil
		}

		return err
	}

	e.scope.IncCounter(metrics.TaskFailures)

	if _, ok := err.(*persistence.CurrentWorkflowConditionFailedError); ok {
		e.logger.Error("More than 2 workflow are running.", tag.Error(err), tag.LifeCycleProcessingFailed)
		return nil
	}

	e.logger.Error("Fail to process task", tag.Error(err), tag.LifeCycleProcessingFailed)
	return err
}

func (e *executableImpl) IsRetryableError(err error) bool {
	// this determines if the executable should be retried within one submission to scheduler

	// don't retry immediately for resource exhausted which may incur more load
	// context deadline exceed may also suggested downstream is overloaded, so don't retry immediately
	if common.IsResourceExhausted(err) || common.IsContextDeadlineExceededErr(err) {
		return false
	}

	// ErrTaskRetry means mutable state is not ready for standby task processing
	// there's no point for retrying the task immediately which will hold the worker corouinte
	// TODO: change ErrTaskRetry to a better name
	// TODO: add workflow busy error (returned when workflow lock can't be grabbed within specified timeout)
	// here after it's defined.
	return err != consts.ErrTaskRetry
}

func (e *executableImpl) RetryPolicy() backoff.RetryPolicy {
	// this is the retry policy for one submission
	// not for calculating the backoff after the task is nacked
	return schedulerRetryPolicy
}

func (e *executableImpl) Ack() {
	e.Lock()
	defer e.Unlock()

	e.state = ctasks.TaskStateAcked

	if e.shouldProcess {
		e.scope.RecordDistribution(metrics.TaskAttemptTimer, e.attempt)
		e.scope.RecordTimer(metrics.TaskLatency, time.Since(e.loadTime))
		e.scope.RecordTimer(metrics.TaskQueueLatency, time.Since(e.task.GetVisibilityTime()))
		e.scope.RecordTimer(metrics.TaskUserLatency, e.userLatency)
		e.scope.RecordTimer(metrics.TaskNoUserLatency, time.Since(e.loadTime)-e.userLatency)
		e.scope.RecordTimer(metrics.TaskNoUserQueueLatency, time.Since(e.task.GetVisibilityTime())-e.userLatency)
	}
}

func (e *executableImpl) Nack(err error) {
	submitted := false
	attempt := e.Attempt()
	if e.shouldResubmitOnNack(attempt, err) {
		// we do not need to know if there any error during submission
		// as long as it's not submitted, the execuable should be add
		// to the rescheduler
		submitted, _ = e.scheduler.TrySubmit(e)
	}

	if !submitted {
		e.rescheduler.Add(e, e.backoffDuration(attempt))
	}
}

func (e *executableImpl) Reschedule() {
	e.Nack(nil)
}

func (e *executableImpl) State() ctasks.State {
	e.Lock()
	defer e.Unlock()

	return e.state
}

func (e *executableImpl) GetPriority() int {
	e.Lock()
	defer e.Unlock()

	return e.priority
}

func (e *executableImpl) SetPriority(priority int) {
	e.Lock()
	defer e.Unlock()

	e.priority = priority
}

func (e *executableImpl) Task() tasks.Task {
	return e.task
}

func (e *executableImpl) Attempt() int {
	e.Lock()
	defer e.Unlock()

	return e.attempt
}

func (e *executableImpl) Logger() log.Logger {
	return e.logger
}

func (e *executableImpl) QueueType() QueueType {
	return e.queueType
}

func (e *executableImpl) shouldResubmitOnNack(attempt int, err error) bool {
	// this is an optimization for skipping rescheduler and retry the task sooner
	// this can be useful for errors like unable to get workflow lock, which doesn't
	// have to backoff for a long time.
	return e.IsRetryableError(err) && e.Attempt() < resubmitMaxAttempts
}

func (e *executableImpl) backoffDuration(attempt int) time.Duration {
	// elapsedTime (the first parameter) is not relevant here since reschedule policy
	// has no expiration interval.
	return reschedulePolicy.ComputeNextDelay(0, attempt)
}

func getNamespaceTagByID(
	namespaceRegistry namespace.Registry,
	namespaceID namespace.ID,
	logger log.Logger,
) metrics.Tag {
	namespaceName, err := namespaceRegistry.GetNamespaceName(namespaceID)
	if err != nil {
		logger.Debug("Unable to get namespace", tag.Error(err))
		return metrics.NamespaceUnknownTag()
	}
	return metrics.NamespaceTag(namespaceName.String())
}

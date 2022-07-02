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

package queues

import (
	"context"
	"sync"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/predicates"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
)

const (
	defaultReaderId = 0
)

var (
	checkpointRetryPolicy = createCheckpointRetryPolicy()
)

type (
	queueState struct {
		readerScopes                 map[int32][]Scope
		exclusiveReaderHighWatermark tasks.Key
	}

	queueBase struct {
		shard shard.Context

		status     int32
		shutdownCh chan struct{}
		shutdownWG sync.WaitGroup

		category    tasks.Category
		options     *QueueOptions
		rescheduler Rescheduler
		timeSource  clock.TimeSource
		monitor     *monitorImpl
		mitigator   *mitigatorImpl
		logger      log.Logger

		// TODO: allow adding scope to reader directly and get rid of these two fields
		paginationFnProvider  PaginationFnProvider
		executableInitializer ExecutableInitializer

		inclusiveLowWatermark tasks.Key
		nonReadableRange      Range
		readerGroup           *readerGroup
		lastPollTime          time.Time

		checkpointTimer   *time.Timer
		checkpointAttempt int

		actionCh <-chan action
	}

	QueueOptions struct {
		ReaderOptions
		MonitorOptions

		MaxPollInterval                  dynamicconfig.DurationPropertyFn
		MaxPollIntervalJitterCoefficient dynamicconfig.FloatPropertyFn
		CheckpointInterval               dynamicconfig.DurationPropertyFn
		TaskMaxRetryCount                dynamicconfig.IntPropertyFn
		QueueType                        QueueType
	}
)

func newQueueBase(
	shard shard.Context,
	category tasks.Category,
	paginationFnProvider PaginationFnProvider,
	scheduler Scheduler,
	executor Executor,
	options *QueueOptions,
	logger log.Logger,
	metricsHandler metrics.MetricsHandler,
) *queueBase {
	var readerScopes map[int32][]Scope
	var inclusiveLowWatermark tasks.Key
	var exclusiveHighWatermark tasks.Key

	if persistenceState, ok := shard.GetQueueState(category); ok {
		queueState := FromPersistenceQueueState(persistenceState)

		readerScopes = queueState.readerScopes
		exclusiveHighWatermark = queueState.exclusiveReaderHighWatermark
		inclusiveLowWatermark = queueState.exclusiveReaderHighWatermark
	} else {
		ackLevel := shard.GetQueueAckLevel(category)
		if category.Type() == tasks.CategoryTypeImmediate {
			// convert to exclusive ack level
			ackLevel = ackLevel.Next()
		}

		exclusiveHighWatermark = ackLevel
		inclusiveLowWatermark = ackLevel
	}

	timeSource := shard.GetTimeSource()
	rescheduler := NewRescheduler(
		scheduler,
		timeSource,
		logger,
		metricsHandler,
	)

	monitor := newMonitor(&options.MonitorOptions)
	mitigator, actionCh := newMitigator(monitor)
	monitor.registerMitigator(mitigator)

	executableInitializer := func(t tasks.Task) Executable {
		return NewExecutable(
			t,
			nil,
			executor,
			scheduler,
			rescheduler,
			timeSource,
			logger,
			options.TaskMaxRetryCount,
			options.QueueType,
			shard.GetConfig().NamespaceCacheRefreshInterval,
		)
	}

	readerInitializer := func(readerID int32, scopes []Scope) Reader {
		return NewReader(
			readerID,
			paginationFnProvider,
			executableInitializer,
			scopes,
			&options.ReaderOptions,
			scheduler,
			rescheduler,
			timeSource,
			monitor,
			logger,
			metricsHandler,
		)
	}

	readerGroup := newReaderGroup(readerInitializer)
	for readerID, scopes := range readerScopes {
		readerGroup.newReader(readerID, scopes)

		if len(scopes) != 0 {
			inclusiveLowWatermark = tasks.MinKey(inclusiveLowWatermark, scopes[0].Range.InclusiveMin)
		}
	}

	return &queueBase{
		shard: shard,

		status:     common.DaemonStatusInitialized,
		shutdownCh: make(chan struct{}),

		category:    category,
		options:     options,
		rescheduler: rescheduler,
		timeSource:  shard.GetTimeSource(),
		monitor:     monitor,
		mitigator:   mitigator,
		logger:      logger,

		paginationFnProvider:  paginationFnProvider,
		executableInitializer: executableInitializer,

		inclusiveLowWatermark: inclusiveLowWatermark,
		nonReadableRange:      NewRange(exclusiveHighWatermark, tasks.MaximumKey),
		readerGroup:           readerGroup,

		actionCh: actionCh,
	}
}

func (p *queueBase) Start() {
	p.readerGroup.start()
	p.rescheduler.Start()

	p.checkpointTimer = time.NewTimer(backoff.JitDuration(
		p.options.CheckpointInterval(),
		p.options.ShrinkRangeIntervalJitterCoefficient(),
	))
}

func (p *queueBase) Stop() {
	p.mitigator.close()
	p.readerGroup.stop()
	p.rescheduler.Stop()
	p.checkpointTimer.Stop()
}

func (p *queueBase) Category() tasks.Category {
	return p.category
}

func (p *queueBase) FailoverNamespace(
	namespaceIDs map[string]struct{},
) {
	// TODO: reschedule all tasks for namespaces that becomes active
	// no-op
}

func (p *queueBase) LockTaskProcessing() {
	// no-op
}

func (p *queueBase) UnlockTaskProcessing() {
	// no-op
}

func (p *queueBase) processNewRange() {
	newMaxKey := p.shard.GetQueueExclusiveHighReadWatermark(p.category, "")

	if !p.nonReadableRange.CanSplit(newMaxKey) {
		return
	}

	p.lastPollTime = p.timeSource.Now()

	var newRange Range
	newRange, p.nonReadableRange = p.nonReadableRange.Split(newMaxKey)
	newScope := NewScope(newRange, predicates.Universal[tasks.Task]())

	reader, ok := p.readerGroup.readerByID(defaultReaderId)
	if !ok {
		p.readerGroup.newReader(defaultReaderId, []Scope{newScope})
	} else {
		reader.MergeSlices(NewSlice(
			p.paginationFnProvider,
			p.executableInitializer,
			p.monitor,
			newScope,
		))
	}
}

func (p *queueBase) checkpoint() {
	var err error
	defer func() {
		if err == nil {
			p.checkpointAttempt = 0
			p.checkpointTimer.Reset(backoff.JitDuration(
				p.options.CheckpointInterval(),
				p.options.ShrinkRangeIntervalJitterCoefficient(),
			))
		} else {
			p.checkpointAttempt++
			backoff := checkpointRetryPolicy.ComputeNextDelay(0, p.checkpointAttempt)
			p.checkpointTimer.Reset(backoff)
		}
	}()

	newTaskLowWatermark := tasks.MaximumKey
	readerScopes := make(map[int32][]Scope)
	totalSlices := 0

	for id, reader := range p.readerGroup.readers() {
		scopes := reader.Scopes()
		totalSlices += len(scopes)
		readerScopes[id] = scopes
		for _, scope := range scopes {
			newTaskLowWatermark = tasks.MinKey(newTaskLowWatermark, scope.Range.InclusiveMin)
		}
	}

	p.monitor.SetTotalSlices(totalSlices)

	// NOTE: Must range complete task first.
	// Otherwise, if state is updated first, later deletion fails and shard get reloaded
	// some tasks will never be deleted.
	if newTaskLowWatermark != tasks.MaximumKey && newTaskLowWatermark.CompareTo(p.inclusiveLowWatermark) > 0 {
		oldTaskLowWatermark := p.inclusiveLowWatermark
		if p.category.Type() == tasks.CategoryTypeScheduled {
			oldTaskLowWatermark = tasks.NewKey(oldTaskLowWatermark.FireTime, 0)
			newTaskLowWatermark = tasks.NewKey(newTaskLowWatermark.FireTime, 0)
		}

		if err = p.shard.GetExecutionManager().RangeCompleteHistoryTasks(context.TODO(), &persistence.RangeCompleteHistoryTasksRequest{
			ShardID:             p.shard.GetShardID(),
			TaskCategory:        p.category,
			InclusiveMinTaskKey: oldTaskLowWatermark,
			ExclusiveMaxTaskKey: newTaskLowWatermark,
		}); err != nil {
			return
		}

		p.inclusiveLowWatermark = newTaskLowWatermark
	}

	err = p.shard.UpdateQueueState(p.category, ToPersistenceQueueState(&queueState{
		readerScopes:                 readerScopes,
		exclusiveReaderHighWatermark: p.nonReadableRange.InclusiveMin,
	}))
}

func createCheckpointRetryPolicy() backoff.RetryPolicy {
	policy := backoff.NewExponentialRetryPolicy(100 * time.Millisecond)
	policy.SetMaximumInterval(5 * time.Second)
	policy.SetExpirationInterval(backoff.NoInterval)

	return policy
}

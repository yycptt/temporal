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

type (
	queueState struct {
		readerScopes        map[int32][]Scope
		exclusiveMaxReadKey tasks.Key
	}

	queueBase struct {
		shard shard.Context

		status     int32
		shutdownCh chan struct{}
		shutdownWG sync.WaitGroup

		category       tasks.Category
		options        *QueueOptions
		rescheduler    Rescheduler
		timeSource     clock.TimeSource
		monitor        *monitorImpl
		logger         log.Logger
		metricsHandler metrics.MetricsHandler

		paginationFnProvider  PaginationFnProvider
		executableInitializer ExecutableInitializer

		exclusiveCompletedTaskKey tasks.Key
		nonReadableRange          Range
		readers                   map[int32]Reader
		lastPollTime              time.Time

		completeTaskTimer   *time.Timer
		completeTaskAttempt int
		completeRetryPolicy backoff.RetryPolicy
	}

	QueueOptions struct {
		// TODO: remove duplicate for complete task and shrink range
		ReaderOptions

		MaxPollInterval                  dynamicconfig.DurationPropertyFn
		MaxPollIntervalJitterCoefficient dynamicconfig.FloatPropertyFn
		CompleteTaskInterval             dynamicconfig.DurationPropertyFn
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
	monitor *monitorImpl,
	logger log.Logger,
	metricsHandler metrics.MetricsHandler,
) *queueBase {
	timeSource := shard.GetTimeSource()
	rescheduler := NewRescheduler(
		scheduler,
		timeSource,
		logger,
		metricsHandler,
	)

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

	readerScopes := make(map[int32][]Scope)
	var exclusiveCompletedTaskKey tasks.Key
	var exclusiveMaxReadKey tasks.Key
	if persistenceState, ok := shard.GetQueueState(category); ok {
		queueState := FromPersistenceQueueState(persistenceState)
		readerScopes = queueState.readerScopes
		exclusiveMaxReadKey = queueState.exclusiveMaxReadKey
		exclusiveCompletedTaskKey = queueState.exclusiveMaxReadKey
	} else {
		ackLevel := shard.GetQueueAckLevel(category)
		if category.Type() == tasks.CategoryTypeImmediate {
			// convert to exclusive ack level
			ackLevel = ackLevel.Next()
		}
		exclusiveMaxReadKey = ackLevel
		exclusiveCompletedTaskKey = ackLevel
	}

	readers := make(map[int32]Reader, len(readerScopes))
	for key, scopes := range readerScopes {
		readers[key] = NewReader(
			key,
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

		if len(scopes) != 0 {
			exclusiveCompletedTaskKey = tasks.MinKey(exclusiveCompletedTaskKey, scopes[0].Range.InclusiveMin)
		}
	}

	completeRetryPolicy := backoff.NewExponentialRetryPolicy(100 * time.Millisecond)
	completeRetryPolicy.SetMaximumInterval(5 * time.Second)
	completeRetryPolicy.SetExpirationInterval(backoff.NoInterval)

	return &queueBase{
		shard: shard,

		status:     common.DaemonStatusInitialized,
		shutdownCh: make(chan struct{}),

		category:       category,
		options:        options,
		rescheduler:    rescheduler,
		timeSource:     shard.GetTimeSource(),
		logger:         logger,
		metricsHandler: metricsHandler,

		paginationFnProvider:  paginationFnProvider,
		executableInitializer: executableInitializer,

		exclusiveCompletedTaskKey: exclusiveCompletedTaskKey,
		nonReadableRange:          NewRange(exclusiveMaxReadKey, tasks.MaximumKey),
		readers:                   readers,

		completeRetryPolicy: completeRetryPolicy,
	}
}

func (p *queueBase) Start() {
	for _, reader := range p.readers {
		reader.Start()
	}
	p.rescheduler.Start()

	p.completeTaskTimer = time.NewTimer(backoff.JitDuration(
		p.options.CompleteTaskInterval(),
		p.options.ShrinkRangeIntervalJitterCoefficient(),
	))
}

func (p *queueBase) Stop() {
	for _, reader := range p.readers {
		reader.Stop()
	}
	p.rescheduler.Stop()
	p.completeTaskTimer.Stop()
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

	p.readers[defaultReaderId].MergeSlices(NewSlice(
		p.paginationFnProvider,
		p.executableInitializer,
		p.monitor,
		NewScope(newRange, predicates.Universal[tasks.Task]()),
	))
}

func (p *queueBase) completeTaskAndPersistState() {
	var err error
	defer func() {
		if err == nil {
			p.completeTaskAttempt = 0
			p.completeTaskTimer.Reset(backoff.JitDuration(
				p.options.CompleteTaskInterval(),
				p.options.ShrinkRangeIntervalJitterCoefficient(),
			))
		} else {
			p.completeTaskAttempt++
			backoff := p.completeRetryPolicy.ComputeNextDelay(0, p.completeTaskAttempt)
			p.completeTaskTimer.Reset(backoff)
		}
	}()

	exclusiveMaxCompletedTaskKey := tasks.MaximumKey
	readerScopes := make(map[int32][]Scope)
	totalSlices := 0

	for id, reader := range p.readers {
		scopes := reader.Scopes()
		totalSlices += len(scopes)
		readerScopes[id] = scopes
		for _, scope := range scopes {
			exclusiveMaxCompletedTaskKey = tasks.MinKey(exclusiveMaxCompletedTaskKey, scope.Range.InclusiveMin)
		}
	}

	p.monitor.SetTotalSlices(totalSlices)

	// NOTE: Must range complete task first.
	// Otherwise, if state is updated first, later deletion fails and shard get reloaded
	// some tasks will never be deleted.
	if exclusiveMaxCompletedTaskKey != tasks.MaximumKey && exclusiveMaxCompletedTaskKey.CompareTo(p.exclusiveCompletedTaskKey) > 0 {
		lastCompletedTaskKey := p.exclusiveCompletedTaskKey
		if p.category.Type() == tasks.CategoryTypeScheduled {
			lastCompletedTaskKey = tasks.NewKey(lastCompletedTaskKey.FireTime, 0)
			exclusiveMaxCompletedTaskKey = tasks.NewKey(exclusiveMaxCompletedTaskKey.FireTime, 0)
		}

		if err = p.shard.GetExecutionManager().RangeCompleteHistoryTasks(context.TODO(), &persistence.RangeCompleteHistoryTasksRequest{
			ShardID:             p.shard.GetShardID(),
			TaskCategory:        p.category,
			InclusiveMinTaskKey: lastCompletedTaskKey,
			ExclusiveMaxTaskKey: exclusiveMaxCompletedTaskKey,
		}); err != nil {
			return
		}

		p.exclusiveCompletedTaskKey = exclusiveMaxCompletedTaskKey
	}

	persistenceState := ToPersistenceQueueState(&queueState{
		readerScopes:        readerScopes,
		exclusiveMaxReadKey: p.nonReadableRange.InclusiveMin,
	})
	err = p.shard.UpdateQueueState(p.category, persistenceState)
}

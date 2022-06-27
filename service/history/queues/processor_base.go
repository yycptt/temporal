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

	persistencespb "go.temporal.io/server/api/persistence/v1"
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
	processorBase struct {
		shard shard.Context

		status     int32
		shutdownCh chan struct{}
		shutdownWG sync.WaitGroup

		category        tasks.Category
		options         *ProcessorOptions
		rescheduler     Rescheduler
		timeSource      clock.TimeSource
		logger          log.Logger
		metricsProvider metrics.MetricProvider

		paginationFnProvider  PaginationFnProvider
		executableInitializer ExecutableInitializer

		completedTaskKey tasks.Key
		nonReadableRange Range
		readers          map[int32]Reader
		lastPollTime     time.Time

		completeTaskTimer   *time.Timer
		completeTaskAttempt int
		completeRetryPolicy backoff.RetryPolicy
	}

	ProcessorOptions struct {
		// TODO: remove duplicate for complete task and shrink range
		ReaderOptions

		MaxPollInterval                  dynamicconfig.DurationPropertyFn
		MaxPollIntervalJitterCoefficient dynamicconfig.FloatPropertyFn
		CompleteTaskInterval             dynamicconfig.DurationPropertyFn
		TaskMaxRetryCount                dynamicconfig.IntPropertyFn
		QueueType                        QueueType
	}
)

func newProcessorBase(
	shard shard.Context,
	category tasks.Category,
	persistenceState *persistencespb.QueueProcessorState,
	paginationFnProvider PaginationFnProvider,
	scheduler Scheduler,
	executor Executor,
	options *ProcessorOptions,
	logger log.Logger,
	metricsProvider metrics.MetricProvider,
) *processorBase {
	timeSource := shard.GetTimeSource()
	rescheduler := NewRescheduler(
		scheduler,
		timeSource,
		logger,
		metricsProvider,
	)

	executableInitializer := func(t tasks.Task) Executable {
		return NewExecutable(
			t,
			func(task tasks.Task) bool { return true },
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
	if persistenceState != nil {
		readerScopes = FromPersistenceProcessorState(persistenceState, category.Type())
	} else {
		readerScopes[defaultReaderId] = []Scope{NewScope(
			NewRange(
				shard.GetQueueAckLevel(category),
				shard.GetQueueMaxReadLevel(category, ""),
			),
			predicates.All[tasks.Task](),
		)}
	}
	readers := make(map[int32]Reader, len(readerScopes))

	minKey := tasks.MaximumKey
	maxKey := tasks.MinimumKey

	for key, scopes := range readerScopes {
		readers[key] = NewReader(
			paginationFnProvider,
			executableInitializer,
			scopes,
			&options.ReaderOptions,
			scheduler,
			rescheduler,
			logger,
			metricsProvider,
		)

		for _, scope := range scopes {
			minKey = tasks.MinKey(minKey, scope.Range.InclusiveMin)
			maxKey = tasks.MaxKey(maxKey, scope.Range.ExclusiveMax)
		}
	}

	completeRetryPolicy := backoff.NewExponentialRetryPolicy(100 * time.Millisecond)
	completeRetryPolicy.SetMaximumInterval(5 * time.Second)
	completeRetryPolicy.SetExpirationInterval(backoff.NoInterval)

	return &processorBase{
		shard: shard,

		status:     common.DaemonStatusInitialized,
		shutdownCh: make(chan struct{}),

		category:        category,
		options:         options,
		rescheduler:     rescheduler,
		timeSource:      shard.GetTimeSource(),
		logger:          logger,
		metricsProvider: metricsProvider,

		paginationFnProvider:  paginationFnProvider,
		executableInitializer: executableInitializer,

		completedTaskKey: minKey,
		nonReadableRange: NewRange(maxKey, tasks.MaximumKey),
		readers:          readers,

		completeRetryPolicy: completeRetryPolicy,
	}
}

func (p *processorBase) Start() {
	for _, reader := range p.readers {
		reader.Start()
	}
	p.rescheduler.Start()

	p.completeTaskTimer = time.NewTimer(backoff.JitDuration(
		p.options.CompleteTaskInterval(),
		p.options.ShrinkRangeIntervalJitterCoefficient(),
	))
}

func (p *processorBase) Stop() {
	for _, reader := range p.readers {
		reader.Stop()
	}
	p.rescheduler.Stop()
	p.completeTaskTimer.Stop()
}

func (p *processorBase) Category() tasks.Category {
	return p.category
}

func (p *processorBase) FailoverNamespace(
	namespaceIDs map[string]struct{},
) {
	// TODO: reschedule all tasks for failover namespaces
}

func (p *processorBase) LockTaskProcessing() {
	// no-op
}

func (p *processorBase) UnlockTaskProcessing() {
	// no-op
}

func (p *processorBase) processNewRange() {
	newMaxKey := p.shard.GetQueueMaxReadLevel(p.category, "")

	if !p.nonReadableRange.CanSplit(newMaxKey) {
		return
	}

	p.lastPollTime = p.timeSource.Now()

	var newRange Range
	newRange, p.nonReadableRange = p.nonReadableRange.Split(newMaxKey)

	p.readers[defaultReaderId].MergeSlices(NewSlice(
		p.paginationFnProvider,
		p.executableInitializer,
		NewScope(newRange, predicates.All[tasks.Task]()),
	))
}

func (p *processorBase) completeTaskAndPersistState() {
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

	minPendingTaskKey := tasks.MaximumKey
	readerScopes := make(map[int32][]Scope)

	for id, reader := range p.readers {
		scopes := reader.Scopes()
		readerScopes[id] = scopes
		for _, scope := range scopes {
			minPendingTaskKey = tasks.MinKey(minPendingTaskKey, scope.Range.InclusiveMin)
		}
	}

	// must do range complete first
	if err = p.shard.GetExecutionManager().RangeCompleteHistoryTasks(context.TODO(), &persistence.RangeCompleteHistoryTasksRequest{
		ShardID:             p.shard.GetShardID(),
		TaskCategory:        p.category,
		InclusiveMinTaskKey: p.completedTaskKey,
		ExclusiveMaxTaskKey: minPendingTaskKey,
	}); err != nil {
		return
	}

	p.completedTaskKey = minPendingTaskKey

	persistenceState := ToPersistenceProcessorState(readerScopes, p.category.Type())
	err = p.shard.UpdateQueueState(p.category, persistenceState)
}

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
	"sync/atomic"
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
)

var _ Processor = (*immediateProcessor)(nil)

type (
	immediateProcessor struct {
		*processorBase

		notifyCh chan struct{}
	}
)

func newImmediateProcessor(
	shard shard.Context,
	category tasks.Category,
	persistenceState *persistencespb.QueueProcessorState,
	scheduler Scheduler,
	executor Executor,
	options *ProcessorOptions,
	logger log.Logger,
	metricsProvider metrics.MetricProvider,
) *immediateProcessor {
	paginationFnProvider := func(r Range) collection.PaginationFn[tasks.Task] {
		return func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			request := &persistence.GetHistoryTasksRequest{
				ShardID:             shard.GetShardID(),
				TaskCategory:        category,
				InclusiveMinTaskKey: r.InclusiveMin,
				ExclusiveMaxTaskKey: r.ExclusiveMax,
				BatchSize:           options.BatchSize(),
				NextPageToken:       paginationToken,
			}

			resp, err := shard.GetExecutionManager().GetHistoryTasks(context.TODO(), request)
			if err != nil {
				return nil, nil, err
			}

			return resp.Tasks, resp.NextPageToken, nil
		}
	}

	return &immediateProcessor{
		processorBase: newProcessorBase(
			shard,
			category,
			persistenceState,
			paginationFnProvider,
			scheduler,
			executor,
			options,
			logger,
			metricsProvider,
		),

		notifyCh: make(chan struct{}),
	}
}

func (p *immediateProcessor) Start() {
	if !atomic.CompareAndSwapInt32(&p.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	p.logger.Info("", tag.LifeCycleStarting)
	defer p.logger.Info("", tag.LifeCycleStarted)

	p.processorBase.Start()

	p.shutdownWG.Add(1)
	go p.processEventLoop()

	p.notify()
}

func (p *immediateProcessor) Stop() {
	if !atomic.CompareAndSwapInt32(&p.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	p.logger.Info("", tag.LifeCycleStopping)
	defer p.logger.Info("", tag.LifeCycleStopped)

	close(p.shutdownCh)

	if success := common.AwaitWaitGroup(&p.shutdownWG, time.Minute); !success {
		p.logger.Warn("", tag.LifeCycleStopTimedout)
	}

	p.processorBase.Stop()
}

func (p *immediateProcessor) NotifyNewTasks(_ string, tasks []tasks.Task) {
	if len(tasks) == 0 {
		return
	}

	p.notify()
}

func (p *immediateProcessor) processEventLoop() {
	defer p.shutdownWG.Done()

	pollTimer := time.NewTimer(backoff.JitDuration(
		p.options.MaxPollInterval(),
		p.options.MaxPollIntervalJitterCoefficient(),
	))
	defer pollTimer.Stop()

	for {
		select {
		case <-p.shutdownCh:
			return
		case <-p.notifyCh:
			p.processNewRange()
		case <-pollTimer.C:
			if p.lastPollTime.Add(p.options.MaxPollInterval()).Before(p.timeSource.Now()) {
				p.processNewRange()
			}
			pollTimer.Reset(backoff.JitDuration(
				p.options.MaxPollInterval(),
				p.options.MaxPollIntervalJitterCoefficient(),
			))
		case <-p.completeTaskTimer.C:
			p.completeTaskAndPersistState()
			// TODO: add a case for polling a channel of split policy
		}
	}
}

func (p *immediateProcessor) notify() {
	select {
	case p.notifyCh <- struct{}{}:
	default:
	}
}

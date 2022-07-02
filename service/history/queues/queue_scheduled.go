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
	"sync/atomic"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/timer"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
)

var _ Queue = (*scheduledQueue)(nil)

type (
	scheduledQueue struct {
		*queueBase

		timerGate   timer.Gate
		newTimerCh  chan struct{}
		newTimeLock sync.Mutex
		newTime     time.Time
	}
)

func NewScheduledQueue(
	shard shard.Context,
	category tasks.Category,
	scheduler Scheduler,
	executor Executor,
	options *QueueOptions,
	logger log.Logger,
	metricsHandler metrics.MetricsHandler,
) *scheduledQueue {
	paginationFnProvider := func(r Range) collection.PaginationFn[tasks.Task] {
		// TODO: handle time precision issue in persistence layer, time will be truncated
		return func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			request := &persistence.GetHistoryTasksRequest{
				ShardID:             shard.GetShardID(),
				TaskCategory:        category,
				InclusiveMinTaskKey: tasks.NewKey(r.InclusiveMin.FireTime, 0),
				ExclusiveMaxTaskKey: tasks.NewKey(r.ExclusiveMax.FireTime, 0),
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

	return &scheduledQueue{
		queueBase: newQueueBase(
			shard,
			category,
			paginationFnProvider,
			scheduler,
			executor,
			options,
			logger,
			metricsHandler,
		),

		timerGate: timer.NewLocalGate(shard.GetTimeSource()),
	}
}

func (p *scheduledQueue) Start() {
	if !atomic.CompareAndSwapInt32(&p.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	p.logger.Info("", tag.LifeCycleStarting)
	defer p.logger.Info("", tag.LifeCycleStarted)

	p.queueBase.Start()

	p.shutdownWG.Add(1)
	go p.processEventLoop()

	p.notify(time.Time{})
}

func (p *scheduledQueue) Stop() {
	if !atomic.CompareAndSwapInt32(&p.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	p.logger.Info("", tag.LifeCycleStopping)
	defer p.logger.Info("", tag.LifeCycleStopped)

	close(p.shutdownCh)
	p.timerGate.Close()

	if success := common.AwaitWaitGroup(&p.shutdownWG, time.Minute); !success {
		p.logger.Warn("", tag.LifeCycleStopTimedout)
	}

	p.queueBase.Stop()
}

func (p *scheduledQueue) NotifyNewTasks(_ string, tasks []tasks.Task) {
	if len(tasks) == 0 {
		return
	}

	newTime := tasks[0].GetVisibilityTime()
	for _, task := range tasks {
		ts := task.GetVisibilityTime()
		if ts.Before(newTime) {
			newTime = ts
		}
	}

	p.notify(newTime)
}

func (p *scheduledQueue) processEventLoop() {
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
		case <-p.newTimerCh:
			p.processNewTime()
		case <-p.timerGate.FireChan():
			p.processNewRange()
		case <-pollTimer.C:
			if p.lastPollTime.Add(p.options.MaxPollInterval()).Before(p.timeSource.Now()) {
				p.processNewRange()
			}
			pollTimer.Reset(backoff.JitDuration(
				p.options.MaxPollInterval(),
				p.options.MaxPollIntervalJitterCoefficient(),
			))
		case <-p.checkpointTimer.C:
			p.checkpoint()
		case action := <-p.actionCh:
			action.run(p.readerGroup)
		}
	}
}

func (p *scheduledQueue) notify(newTime time.Time) {
	p.newTimeLock.Lock()
	defer p.newTimeLock.Unlock()

	if !p.newTime.IsZero() && !newTime.Before(p.newTime) {
		return
	}

	p.newTime = newTime
	select {
	case p.newTimerCh <- struct{}{}:
	default:
	}
}

func (p *scheduledQueue) processNewTime() {
	p.newTimeLock.Lock()
	newTime := p.newTime
	p.newTime = time.Time{}
	p.newTimeLock.Unlock()

	// New Timer has arrived.
	// t.metricsClient.IncCounter(t.scope, metrics.NewTimerNotifyCounter)
	p.timerGate.Update(newTime)
}

func (p *scheduledQueue) processNewRange() {
	p.queueBase.processNewRange()
	p.lookAheadTask()
}

func (p *scheduledQueue) lookAheadTask() {
	lookAheadMinTime := p.nonReadableRange.InclusiveMin.FireTime
	lookAheadMaxTime := lookAheadMinTime.Add(p.options.MaxPollInterval())

	request := &persistence.GetHistoryTasksRequest{
		ShardID:             p.shard.GetShardID(),
		TaskCategory:        tasks.CategoryTimer,
		InclusiveMinTaskKey: tasks.NewKey(lookAheadMinTime, 0),
		ExclusiveMaxTaskKey: tasks.NewKey(lookAheadMaxTime, 0),
		BatchSize:           1,
		NextPageToken:       nil,
	}
	response, err := p.shard.GetExecutionManager().GetHistoryTasks(context.TODO(), request)
	if err != nil {
		p.timerGate.Update(p.timeSource.Now().Add(p.shard.GetConfig().TimerProcessorMaxTimeShift()))
		return
	}

	if len(response.Tasks) == 1 {
		p.timerGate.Update(response.Tasks[0].GetVisibilityTime())
	}

	// no look ahead task, wait for max poll interval
}

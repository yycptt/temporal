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
	"container/list"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
)

type (
	Reader interface {
		common.Daemon

		Scopes() []Scope
		MergeSlices([]Slice)
		SplitSlices(SliceSplitter)
		Throttle(time.Duration)
	}

	ReaderOptions struct {
		BatchSize                            dynamicconfig.IntPropertyFn
		ShrinkRangeInterval                  dynamicconfig.DurationPropertyFn
		ShrinkRangeIntervalJitterCoefficient dynamicconfig.FloatPropertyFn
		MaxReschdulerSize                    dynamicconfig.IntPropertyFn
		PollBackoffInterval                  dynamicconfig.DurationPropertyFn
		MetricScope                          int
	}

	SliceSplitter func(s Slice) (remaining []Slice)

	ReaderImpl struct {
		sync.Mutex

		paginationFnProvider  PaginationFnProvider
		executableInitializer ExecutableInitializer
		options               *ReaderOptions
		scheduler             Scheduler
		rescheduler           Rescheduler
		timeSource            clock.TimeSource
		logger                log.Logger
		metricsProvider       metrics.MetricProvider

		status     int32
		shutdownCh chan struct{}
		shutdownWG sync.WaitGroup

		slices        *list.List
		nextLoadSlice *list.Element
		notifyCh      chan struct{}

		throttleTimer *time.Timer
	}
)

func NewReader(
	paginationFnProvider PaginationFnProvider,
	executableInitializer ExecutableInitializer,
	scopes []Scope,
	options *ReaderOptions,
	scheduler Scheduler,
	rescheduler Rescheduler,
	timeSource clock.TimeSource,
	logger log.Logger,
	metricsProvider metrics.MetricProvider,
) *ReaderImpl {
	slices := list.New()
	for _, scope := range scopes {
		slices.PushBack(NewSlice(
			paginationFnProvider,
			executableInitializer,
			scope,
		))
	}

	return &ReaderImpl{
		paginationFnProvider:  paginationFnProvider,
		executableInitializer: executableInitializer,
		options:               options,
		scheduler:             scheduler,
		rescheduler:           rescheduler,
		timeSource:            timeSource,
		logger:                logger,
		metricsProvider:       metricsProvider,

		status:     common.DaemonStatusInitialized,
		shutdownCh: make(chan struct{}),

		slices:        slices,
		nextLoadSlice: slices.Front(),
	}
}

func (r *ReaderImpl) Start() {
	if !atomic.CompareAndSwapInt32(
		&r.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	r.rescheduler.Start()

	r.shutdownWG.Add(1)
	go r.eventLoop()

	r.logger.Info("queue reader started", tag.LifeCycleStarted)
}

func (r *ReaderImpl) Stop() {
	if !atomic.CompareAndSwapInt32(
		&r.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	r.rescheduler.Stop()

	close(r.shutdownCh)
	if success := common.AwaitWaitGroup(&r.shutdownWG, time.Minute); !success {
		r.logger.Warn("queue reader shutdown timed out waiting for event loop", tag.LifeCycleStopTimedout)
	}
	r.logger.Info("queue reader stopped", tag.LifeCycleStopped)
}

func (r *ReaderImpl) Scopes() []Scope {
	r.Lock()
	defer r.Unlock()

	scopes := make([]Scope, 0, r.slices.Len())
	for element := r.slices.Front(); element != nil; element = element.Next() {
		scopes = append(scopes, element.Value.(Slice).Scope())
	}

	return scopes
}

func (r *ReaderImpl) MergeSlices(incomingSlices []Slice) {
	r.Lock()
	defer r.Unlock()

	mergedSlices := make([]Slice, 0, r.slices.Len()+len(incomingSlices))
	currentSliceElement := r.slices.Front()
	incomingSliceIdx := 0

	for currentSliceElement != nil && incomingSliceIdx < len(incomingSlices) {
		currentSlice := currentSliceElement.Value.(Slice)
		incomingSlice := incomingSlices[incomingSliceIdx]

		if currentSlice.Scope().Range.InclusiveMin.CompareTo(incomingSlice.Scope().Range.InclusiveMin) < 0 {
			mergedSlices = appendSlice(mergedSlices, currentSlice)
			currentSliceElement = currentSliceElement.Next()
		} else {
			mergedSlices = appendSlice(mergedSlices, incomingSlice)
			incomingSliceIdx++
		}
	}

	for currentSliceElement != nil {
		mergedSlices = appendSlice(mergedSlices, currentSliceElement.Value.(Slice))
		currentSliceElement = currentSliceElement.Next()
	}
	for _, slice := range incomingSlices[incomingSliceIdx:] {
		mergedSlices = appendSlice(mergedSlices, slice)
	}

	r.resetNextLoadSliceLocked()
}

func (r *ReaderImpl) SplitSlices(splitter SliceSplitter) {
	r.Lock()
	defer r.Unlock()

	var next *list.Element
	for element := r.slices.Front(); element != nil; element = next {
		next = element.Next()

		for _, newSlice := range splitter(element.Value.(Slice)) {
			r.slices.InsertAfter(newSlice, element)
		}

		r.slices.Remove(element)
	}

	r.resetNextLoadSliceLocked()
}

func (r *ReaderImpl) Throttle(backoff time.Duration) {
	r.Lock()
	defer r.Unlock()

	r.throttleLocked(backoff)
}

func (r *ReaderImpl) throttleLocked(backoff time.Duration) {
	r.throttleTimer = time.AfterFunc(backoff, func() {
		r.Lock()
		defer r.Unlock()

		r.throttleTimer = nil
		r.notify()
	})
}

func (r *ReaderImpl) eventLoop() {
	defer r.shutdownWG.Done()

	shrinkRangeTimer := time.NewTimer(backoff.JitDuration(
		r.options.ShrinkRangeInterval(),
		r.options.ShrinkRangeIntervalJitterCoefficient(),
	))
	defer shrinkRangeTimer.Stop()

	for {
		select {
		case <-r.shutdownCh:
			return
		case <-r.notifyCh:
			r.loadAndSubmitTasks()
		case <-shrinkRangeTimer.C:
			r.shrinkRanges()
			shrinkRangeTimer.Reset(backoff.JitDuration(
				r.options.ShrinkRangeInterval(),
				r.options.ShrinkRangeIntervalJitterCoefficient(),
			))
		}
	}
}

func (r *ReaderImpl) loadAndSubmitTasks() {
	r.Lock()
	defer r.Unlock()

	r.verifyReschedulerSize()

	if r.throttleTimer != nil {
		return
	}

	if r.nextLoadSlice == nil {
		return
	}

	loadSlice := r.nextLoadSlice.Value.(Slice)
	tasks, err := loadSlice.SelectTasks(r.options.BatchSize())
	if err != nil {
		r.logger.Error("Queue reader unable to retrieve tasks", tag.Error(err))
		r.notify()
	}

	for _, task := range tasks {
		r.submit(task)
	}

	if loadSlice.MoreTasks() {
		r.notify()
		return
	}

	if r.nextLoadSlice = r.nextLoadSlice.Next(); r.nextLoadSlice != nil {
		r.notify()
	}
}

func (r *ReaderImpl) shrinkRanges() {
	r.Lock()
	defer r.Unlock()

	var next *list.Element
	for element := r.slices.Front(); element != nil; element = next {
		next = element.Next()

		slice := element.Value.(Slice)
		slice.ShrinkRange()
		if scope := slice.Scope(); scope.IsEmpty() {
			r.slices.Remove(element)
		}
	}
}

func (r *ReaderImpl) resetNextLoadSliceLocked() {
	r.nextLoadSlice = nil
	for element := r.slices.Front(); element != nil; element = element.Next() {
		if element.Value.(Slice).MoreTasks() {
			r.nextLoadSlice = element
			break
		}
	}

	if r.nextLoadSlice != nil {
		r.notify()
	}
}

func (r *ReaderImpl) notify() {
	select {
	case r.notifyCh <- struct{}{}:
	default:
	}
}

func (r *ReaderImpl) submit(
	executable Executable,
) {

	submitted, err := r.scheduler.TrySubmit(executable)
	if err != nil {
		r.logger.Error("Failed to submit task", tag.Error(err))
		executable.Reschedule()
	} else if !submitted {
		executable.Reschedule()
	}
}

func (r *ReaderImpl) verifyReschedulerSize() {
	if r.rescheduler.Len() >= r.options.MaxReschdulerSize() {
		r.throttleLocked(r.options.PollBackoffInterval())
	}
}

func appendSlice(
	slices []Slice,
	incomingSlice Slice,
) []Slice {
	if len(slices) == 0 {
		return []Slice{incomingSlice}
	}

	lastSlice := slices[len(slices)-1]
	if !lastSlice.CanMergeWithSlice(incomingSlice) {
		slices = append(slices, incomingSlice)
		return slices
	}

	mergedSlices := lastSlice.MergeWithSlice(incomingSlice)
	slices = slices[:len(slices)-1]
	return append(slices, mergedSlices...)
}

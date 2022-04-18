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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination rescheduler_mock.go

package queues

import (
	"sort"
	"sync"
	"time"

	"go.temporal.io/server/common/clock"
)

type (
	// Rescheduler buffers task executables that are failed to process and
	// resubmit them to the task scheduler when the Reschedule method is called.
	// TODO: remove this component when implementing multi-cursor queue processor.
	// Failed task executables can be tracke by task reader/queue range
	Rescheduler interface {
		// Add task executable to the rescheudler.
		// The backoff duration is just a hint for how long the executable
		// should be bufferred before rescheduling.
		Add(task Executable, backoff time.Duration)

		// Reschedule re-submit numTasks executables to the scheduler.
		// reschedule all if targetRescheduleSize is 0.
		Reschedule(targetRescheduleSize int)

		// Len returns the total number of task executables waiting to be rescheduled.
		Len() int
	}

	reschedulerImpl struct {
		scheduler  Scheduler
		timeSource clock.TimeSource

		sync.Mutex
		buckets        map[time.Time][]Executable
		numExecutables int

		rescheduleTimes  []time.Time
		priorityChanFull map[int]bool
	}
)

const (
	defaultRescheduleBucketDuration = 3 * time.Second
)

func NewRescheduler(
	scheduler Scheduler,
	timeSource clock.TimeSource,
) *reschedulerImpl {
	return &reschedulerImpl{
		scheduler:      scheduler,
		timeSource:     timeSource,
		buckets:        make(map[time.Time][]Executable),
		numExecutables: 0,
	}
}

func (r *reschedulerImpl) Add(
	executable Executable,
	backoff time.Duration,
) {
	rescheduleTime := r.timeSource.Now().Add(backoff).Truncate(defaultRescheduleBucketDuration)

	r.Lock()
	defer r.Unlock()

	if _, ok := r.buckets[rescheduleTime]; !ok {
		r.rescheduleTimes = append(r.rescheduleTimes, rescheduleTime)
	}
	r.buckets[rescheduleTime] = append(r.buckets[rescheduleTime], executable)
	r.numExecutables++
}

func (r *reschedulerImpl) Reschedule(
	targetRescheduleSize int,
) {
	r.Lock()
	defer r.Unlock()

	if targetRescheduleSize == 0 {
		targetRescheduleSize = r.numExecutables
	}

	sort.Slice(r.rescheduleTimes, func(i int, j int) bool {
		return r.rescheduleTimes[i].Before(r.rescheduleTimes[j])
	})
	for priority := range r.priorityChanFull {
		r.priorityChanFull[priority] = false
	}

	rescheduled := 0
	for _, rescheduleTime := range r.rescheduleTimes {
		if r.timeSource.Now().Before(rescheduleTime) {
			break
		}

		var newBucket []Executable
		oldBucket := r.buckets[rescheduleTime]
		r.numExecutables -= len(oldBucket)
		for idx, executable := range oldBucket {
			if r.priorityChanFull[executable.GetPriority()] {
				continue
			}

			submitted, err := r.scheduler.TrySubmit(executable)
			if err != nil {
				newBucket = append(newBucket, executable)
			} else if !submitted {
				r.priorityChanFull[executable.GetPriority()] = true
				newBucket = append(newBucket, executable)
			} else {
				// successfully rescheduled
				rescheduled++
				if rescheduled >= targetRescheduleSize {
					// skip the rest executables in the bucket
					newBucket = append(newBucket, oldBucket[idx+1:]...)
					break
				}
			}
		}

		r.numExecutables += len(newBucket)
		if len(newBucket) != 0 {
			r.buckets[rescheduleTime] = newBucket
		} else {
			delete(r.buckets, rescheduleTime)
		}
	}
}

func (r *reschedulerImpl) Len() int {
	r.Lock()
	defer r.Unlock()

	return r.numExecutables
}

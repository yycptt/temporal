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

package shard

import (
	"sync"

	"go.temporal.io/server/service/history/tasks"
)

type (
	taskKeyTracker interface {
		track(map[tasks.Category][]tasks.Task) func(error)
		minPendingTaskKey(tasks.Category) (tasks.Key, bool)
		waitPendingRequests()
		clear()
	}

	taskKeyTrackerImpl struct {
		sync.Mutex

		// TODO: use a priority queue to track the min pending task key
		pendingTaskKeys map[tasks.Category][]tasks.Key
		pendingRequests map[int64]struct{}
		waitChannels    []chan<- struct{}

		nextRequestID int64
	}
)

func newTaskKeyTracker() *taskKeyTrackerImpl {
	return &taskKeyTrackerImpl{
		pendingTaskKeys: make(map[tasks.Category][]tasks.Key),
		pendingRequests: make(map[int64]struct{}),
		waitChannels:    make([]chan<- struct{}, 0),
		nextRequestID:   0,
	}
}

func (t *taskKeyTrackerImpl) track(
	insertTasks map[tasks.Category][]tasks.Task,
) func(error) {
	t.Lock()
	defer t.Unlock()

	requestID := t.nextRequestID
	t.nextRequestID++
	t.pendingRequests[requestID] = struct{}{}

	minKeyByCategory := make(map[tasks.Category]tasks.Key)
	for category, tasksPerCategory := range insertTasks {
		if len(tasksPerCategory) == 0 {
			continue
		}
		minKey := minKeyTask(tasksPerCategory)
		t.pendingTaskKeys[category] = append(t.pendingTaskKeys[category], minKey)
		minKeyByCategory[category] = minKey
	}

	return func(writeErr error) {
		t.Lock()
		defer t.Unlock()

		if writeErr == nil || !OperationPossiblySucceeded(writeErr) {
			// we can only remove the task from the pending task list if we are sure it was inserted
			for category, minKey := range minKeyByCategory {
				pendingTasksForCategory := t.pendingTaskKeys[category]
				for i := range pendingTasksForCategory {
					if pendingTasksForCategory[i].CompareTo(minKey) == 0 {
						pendingTasksForCategory = append(pendingTasksForCategory[:i], pendingTasksForCategory[i+1:]...)
						break
					}
				}
			}
		}

		// always mark the request as completed, otherwise rangeID renew will be blocked forever
		delete(t.pendingRequests, requestID)
		if len(t.pendingRequests) == 0 {
			t.closeWaitChannelsLocked()
		}
	}
}

func (t *taskKeyTrackerImpl) minPendingTaskKey(
	category tasks.Category,
) (tasks.Key, bool) {
	t.Lock()
	defer t.Unlock()

	pendingTasksForCategory := t.pendingTaskKeys[category]
	if len(pendingTasksForCategory) == 0 {
		return tasks.Key{}, false
	}

	minKey := pendingTasksForCategory[0]
	for _, key := range pendingTasksForCategory {
		if key.CompareTo(minKey) < 0 {
			minKey = key
		}
	}

	return minKey, true
}

func (t *taskKeyTrackerImpl) clear() {
	t.Lock()
	defer t.Unlock()

	t.pendingTaskKeys = make(map[tasks.Category][]tasks.Key)
	t.pendingRequests = make(map[int64]struct{})
	t.closeWaitChannelsLocked()
}

func (t *taskKeyTrackerImpl) waitPendingRequests() {
	t.Lock()

	if len(t.pendingRequests) == 0 {
		t.Unlock()
		return
	}

	waitCh := make(chan struct{})
	t.waitChannels = append(t.waitChannels, waitCh)
	t.Unlock()

	<-waitCh
}

func (t *taskKeyTrackerImpl) closeWaitChannelsLocked() {
	for _, waitCh := range t.waitChannels {
		close(waitCh)
	}
	t.waitChannels = nil
}

// minKeyTask returns the min key of the given tasks
func minKeyTask(t []tasks.Task) tasks.Key {
	minKey := tasks.MaximumKey
	for _, task := range t {
		if task.GetKey().CompareTo(minKey) < 0 {
			minKey = task.GetKey()
		}
	}
	return minKey
}

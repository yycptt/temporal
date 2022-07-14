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
	"fmt"

	"go.temporal.io/server/common/namespace"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/tasks"
)

type (
	// TODO: make task tracking a standalone component
	// currently it's used as a implementation detail in Slice
	taskTracker struct {
		pendingExecutables  map[tasks.Key]Executable
		pendingPerNamesapce map[namespace.ID]int
		ackedTasks          map[tasks.Task]struct{}
	}
)

func newTaskTracker() *taskTracker {
	return &taskTracker{
		pendingExecutables:  make(map[tasks.Key]Executable),
		pendingPerNamesapce: make(map[namespace.ID]int),
		ackedTasks:          make(map[tasks.Task]struct{}),
	}
}

func (t *taskTracker) split(
	thisScope Scope,
	thatScope Scope,
) (*taskTracker, *taskTracker) {
	thatPendingExecutables := make(map[tasks.Key]Executable, len(t.pendingExecutables)/2)
	thatPendingPerNamespace := make(map[namespace.ID]int, len(t.pendingPerNamesapce))
	thatAckTasks := make(map[tasks.Task]struct{}, len(t.ackedTasks)/2)

	for key, executable := range t.pendingExecutables {
		if thisScope.Contains(executable) {
			continue
		}

		if !thatScope.Contains(executable) {
			panic(fmt.Sprintf("Queue slice encountered task doesn't belong to either scopes during split, scope: %v and %v, task: %v, task type: %v",
				thisScope, thatScope, executable.GetTask(), executable.GetType()))
		}

		namespaceID := namespace.ID(executable.GetNamespaceID())

		delete(t.pendingExecutables, key)
		t.pendingPerNamesapce[namespaceID]--

		thatPendingExecutables[key] = executable
		thatPendingPerNamespace[namespaceID]++
	}

	for task := range t.ackedTasks {
		if thisScope.Contains(task) {
			continue
		}

		if !thatScope.Contains(task) {
			panic(fmt.Sprintf("Queue slice encountered task doesn't belong to either scopes during split, scope: %v and %v, task: %v, task type: %v",
				thisScope, thatScope, task, task.GetType()))
		}

		delete(t.ackedTasks, task)
		thatAckTasks[task] = struct{}{}
	}

	return t, &taskTracker{
		pendingExecutables:  thatPendingExecutables,
		pendingPerNamesapce: thatPendingPerNamespace,
		ackedTasks:          thatAckTasks,
	}
}

func (t *taskTracker) merge(incomingTracker *taskTracker) *taskTracker {
	thisExecutables, thisPendingTasks := t.pendingExecutables, t.pendingPerNamesapce
	thatExecutables, thatPendingTasks := incomingTracker.pendingExecutables, incomingTracker.pendingPerNamesapce
	if len(thisExecutables) < len(thatExecutables) {
		thisExecutables, thatExecutables = thatExecutables, thisExecutables
		thisPendingTasks, thatPendingTasks = thatPendingTasks, thisPendingTasks
	}

	for key, executable := range thatExecutables {
		thisExecutables[key] = executable
		thisPendingTasks[namespace.ID(executable.GetNamespaceID())]++
	}

	thisAckedTasks, thatAckTasks := t.ackedTasks, incomingTracker.ackedTasks
	if len(thisAckedTasks) < len(thatAckTasks) {
		thisAckedTasks, thatAckTasks = thatAckTasks, thisAckedTasks
	}
	for task := range thatAckTasks {
		thisAckedTasks[task] = struct{}{}
	}

	t.pendingExecutables = thisExecutables
	t.pendingPerNamesapce = thisPendingTasks
	t.ackedTasks = thisAckedTasks
	return t
}

func (t *taskTracker) add(
	executable Executable,
) {
	t.pendingExecutables[executable.GetKey()] = executable
	t.pendingPerNamesapce[namespace.ID(executable.GetNamespaceID())]++
}

func (t *taskTracker) shrinkPendingTasks() tasks.Key {
	minPendingTaskKey := tasks.MaximumKey
	for key, executable := range t.pendingExecutables {
		if t.pendingExecutables[key].State() == ctasks.TaskStateAcked {
			t.pendingPerNamesapce[namespace.ID(executable.GetNamespaceID())]--
			delete(t.pendingExecutables, key)
			t.ackedTasks[executable.GetTask()] = struct{}{}
			continue
		}

		minPendingTaskKey = tasks.MinKey(minPendingTaskKey, key)
	}

	return minPendingTaskKey
}

func (t *taskTracker) shrinkAckedTasks(
	scope Scope,
) {
	for task := range t.ackedTasks {
		if !scope.Contains(task) {
			delete(t.ackedTasks, task)
		}
	}
}

func (t *taskTracker) clear() {
	for _, executable := range t.pendingExecutables {
		executable.Cancel()
	}

	t.pendingExecutables = make(map[tasks.Key]Executable)
	t.pendingPerNamesapce = make(map[namespace.ID]int)
	t.ackedTasks = make(map[tasks.Task]struct{})
}

func (t *taskTracker) destory() {
	t.pendingExecutables = nil
	t.pendingPerNamesapce = nil
	t.ackedTasks = nil
}

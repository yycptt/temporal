// The MIT License
//
// Copyright (c) 2023 Temporal Technologies Inc.  All rights reserved.
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

package tasks

import (
	"sync"

	"github.com/dgryski/go-farm"
	"go.temporal.io/server/common/definition"
)

type (
	SequentialTaskQueueFactory[T Task] func(task T) SequentialTaskQueue[T]

	SequentialTaskQueue[T Task] interface {
		// ID return the ID of the queue, as well as the tasks inside (same)
		ID() interface{}
		// Add push a task to the task set
		Add(T)
		// Remove pop a task from the task set
		Remove() T
		// IsEmpty indicate if the task set is empty
		IsEmpty() bool
		// Len return the size of the queue
		Len() int
	}
)

type (
	SequentialTaskQueueImpl[T Task] struct {
		id interface{}

		sync.Mutex
		taskQueue []T
	}
)

func NewSequentialTaskQueue[T Task](id any) *SequentialTaskQueueImpl[T] {
	return &SequentialTaskQueueImpl[T]{
		id: id,
	}
}

func (q *SequentialTaskQueueImpl[T]) ID() interface{} {
	return q.id
}

func (q *SequentialTaskQueueImpl[T]) Add(task T) {
	q.Lock()
	defer q.Unlock()
	q.taskQueue = append(q.taskQueue, task)
}

func (q *SequentialTaskQueueImpl[T]) Remove() T {
	q.Lock()
	defer q.Unlock()

	var t T
	if len(q.taskQueue) == 0 {
		return t
	}

	t = q.taskQueue[0]
	q.taskQueue = q.taskQueue[1:]
	return t
}

func (q *SequentialTaskQueueImpl[T]) IsEmpty() bool {
	q.Lock()
	defer q.Unlock()
	return len(q.taskQueue) == 0
}

func (q *SequentialTaskQueueImpl[T]) Len() int {
	q.Lock()
	defer q.Unlock()
	return len(q.taskQueue)
}

func WorkflowKeyHashFn(
	item interface{},
) uint32 {
	workflowKey, ok := item.(definition.WorkflowKey)
	if !ok {
		return 0
	}
	idBytes := []byte(workflowKey.NamespaceID + "_" + workflowKey.WorkflowID + "_" + workflowKey.RunID)
	return farm.Fingerprint32(idBytes)
}

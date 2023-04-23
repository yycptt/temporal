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
	"fmt"
	"time"

	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/tasks"
)

type (
	taskKeyAllocator interface {
		allocate(definition.WorkflowKey, map[tasks.Category][]tasks.Task) error
		peekNextTaskKey(tasks.Category) tasks.Key
		generateTaskKey(tasks.Category) (tasks.Key, error)
		setRangeID(int64)
		setTaskMinScheduledTime(time.Time)
	}

	renewRangeIDFn func() error

	taskKeyAllocatorImpl struct {
		nextTaskID         int64
		exclusiveMaxTaskID int64

		taskMinScheduledTime time.Time

		rangeSizeBits          uint
		timeSource             clock.TimeSource
		taskScheduledTimeShift dynamicconfig.DurationPropertyFn
		logger                 log.Logger

		renewRangeIDFn renewRangeIDFn
	}
)

func newTaskKeyAllocator(
	rangeSizeBits uint,
	timeSource clock.TimeSource,
	taskScheduleTimeShift dynamicconfig.DurationPropertyFn,
	logger log.Logger,
	renewRangeIDFn renewRangeIDFn,
) *taskKeyAllocatorImpl {
	// TODO: assert rangeID and minScheduledTime are set
	return &taskKeyAllocatorImpl{
		rangeSizeBits:          rangeSizeBits,
		timeSource:             timeSource,
		taskScheduledTimeShift: taskScheduleTimeShift,
		logger:                 logger,
		renewRangeIDFn:         renewRangeIDFn,
	}
}

func (a *taskKeyAllocatorImpl) allocate(
	workflowKey definition.WorkflowKey,
	insertTasks map[tasks.Category][]tasks.Task,
) error {
	now := a.timeSource.Now()

	for category, tasksByCategory := range insertTasks {
		isScheduledTask := category.Type() == tasks.CategoryTypeScheduled
		for _, task := range tasksByCategory {
			id, err := a.generateTaskID()
			if err != nil {
				return err
			}

			a.logger.Debug("Assigning task ID", tag.TaskID(id))
			task.SetTaskID(id)

			if !isScheduledTask {
				task.SetVisibilityTime(now)
				continue
			}

			taskScheduleTime := task.GetVisibilityTime()
			if taskScheduleTime.Truncate(persistence.ScheduledTaskMinPrecision).Before(a.taskMinScheduledTime) {
				a.logger.Debug("New timer generated is less than read level",
					tag.WorkflowNamespaceID(workflowKey.NamespaceID),
					tag.WorkflowID(workflowKey.WorkflowID),
					tag.WorkflowRunID(workflowKey.RunID),
					tag.Timestamp(taskScheduleTime),
					tag.CursorTimestamp(a.taskMinScheduledTime),
					tag.ValueShardAllocateTimerBeforeRead,
				)
				task.SetVisibilityTime(a.taskMinScheduledTime.Add(persistence.ScheduledTaskMinPrecision))
			}
			a.logger.Debug("Assigning new timer",
				tag.Timestamp(task.GetVisibilityTime()),
				tag.TaskID(task.GetTaskID()),
				tag.MaxQueryLevel(a.taskMinScheduledTime),
			)
		}
	}

	return nil
}

func (a *taskKeyAllocatorImpl) peekNextTaskKey(
	category tasks.Category,
) tasks.Key {
	switch category.Type() {
	case tasks.CategoryTypeImmediate:
		return tasks.NewImmediateKey(a.nextTaskID)
	case tasks.CategoryTypeScheduled:
		// Truncation here is just to make sure max read level has the same precision as the old logic
		// in case existing code can't work correctly with precision higher than 1ms.
		// Once we validate the rest of the code can worker correctly with higher precision, the truncation should be removed.

		// TODO: do this during task allocation as well
		a.setTaskMinScheduledTime(
			a.timeSource.Now().
				Add(a.taskScheduledTimeShift()).
				Truncate(persistence.ScheduledTaskMinPrecision),
		)
		return tasks.NewKey(a.taskMinScheduledTime, a.nextTaskID)
	default:
		panic(fmt.Sprintf("Unknown category type: %v", category.Type()))
	}
}

func (a *taskKeyAllocatorImpl) generateTaskKey(
	category tasks.Category,
) (tasks.Key, error) {
	id, err := a.generateTaskID()
	if err != nil {
		return tasks.Key{}, err
	}

	switch category.Type() {
	case tasks.CategoryTypeImmediate:
		return tasks.NewImmediateKey(id), nil
	case tasks.CategoryTypeScheduled:
		return tasks.NewKey(a.taskMinScheduledTime, id), nil
	default:
		panic(fmt.Sprintf("Unknown category type: %v", category.Type()))
	}
}

func (a *taskKeyAllocatorImpl) setRangeID(rangeID int64) {
	a.nextTaskID = rangeID << a.rangeSizeBits
	a.exclusiveMaxTaskID = (rangeID + 1) << a.rangeSizeBits

	a.logger.Info("Task key range updated",
		tag.Number(a.nextTaskID),
		tag.NextNumber(a.exclusiveMaxTaskID),
	)
}

func (a *taskKeyAllocatorImpl) setTaskMinScheduledTime(
	taskMinScheduledTime time.Time,
) {
	a.taskMinScheduledTime = util.MaxTime(a.taskMinScheduledTime, taskMinScheduledTime)
}

func (a *taskKeyAllocatorImpl) generateTaskID() (int64, error) {
	if a.nextTaskID == a.exclusiveMaxTaskID {
		if err := a.renewRangeIDFn(); err != nil {
			return -1, err
		}
	}

	taskID := a.nextTaskID
	a.nextTaskID++
	return taskID, nil
}

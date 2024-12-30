// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = &ChasmTask{}

// ChasmTask is a base for all tasks emitted by hierarchical state machines.
type ChasmTask struct {
	definition.WorkflowKey
	VisibilityTimestamp time.Time
	TaskID              int64
	Info                *persistencespb.ChasmTaskInfo
}

var _ HasStateMachineTaskType = &ChasmTask{}

func (t *ChasmTask) GetKey() Key {
	return NewImmediateKey(t.TaskID)
}

func (t *ChasmTask) GetTaskID() int64 {
	return t.TaskID
}

func (t *ChasmTask) SetTaskID(id int64) {
	t.TaskID = id
}

func (t *ChasmTask) GetVisibilityTime() time.Time {
	return t.VisibilityTimestamp
}

func (t *ChasmTask) SetVisibilityTime(timestamp time.Time) {
	t.VisibilityTimestamp = timestamp
}

func (t *ChasmTask) StateMachineTaskType() string {
	return t.Info.Name
}

func (*ChasmTask) GetCategory() Category {
	return CategoryTransfer
}

func (*ChasmTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_CHASM_TRANSFER
}

// ChasmOutboundTask is a task on the outbound queue.
type ChasmOutboundTask struct {
	ChasmTask
	Destination string
}

// GetDestination is used for grouping outbound tasks into a per source namespace and destination scheduler and in multi-cursor predicates.
func (t *ChasmOutboundTask) GetDestination() string {
	return t.Destination
}

func (*ChasmOutboundTask) GetCategory() Category {
	return CategoryOutbound
}

func (*ChasmOutboundTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_CHASM_OUTBOUND
}

func (t *ChasmOutboundTask) GetKey() Key {
	return NewImmediateKey(t.TaskID)
}

var _ Task = &ChasmOutboundTask{}
var _ HasDestination = &ChasmOutboundTask{}

// StateMachineCallbackTask is a generic timer task that can be emitted by any hierarchical state machine.
type ChasmTimerTask struct {
	definition.WorkflowKey
	VisibilityTimestamp time.Time
	TaskID              int64
	Version             int64
}

func (*ChasmTimerTask) GetCategory() Category {
	return CategoryTimer
}

func (*ChasmTimerTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_CHASM_TIMER
}

func (t *ChasmTimerTask) GetKey() Key {
	return NewKey(t.VisibilityTimestamp, t.TaskID)
}

func (t *ChasmTimerTask) GetTaskID() int64 {
	return t.TaskID
}

func (t *ChasmTimerTask) SetTaskID(id int64) {
	t.TaskID = id
}

func (t *ChasmTimerTask) GetVisibilityTime() time.Time {
	return t.VisibilityTimestamp
}

func (t *ChasmTimerTask) SetVisibilityTime(timestamp time.Time) {
	t.VisibilityTimestamp = timestamp
}

var _ Task = &ChasmTimerTask{}

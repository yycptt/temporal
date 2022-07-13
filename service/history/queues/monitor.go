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
	"sync"
	"time"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/tasks"
	"golang.org/x/exp/maps"
)

var _ Monitor = (*monitorImpl)(nil)

const (
	monitorWatermarkPrecision = time.Second
)

type (
	Monitor interface {
		GetTotalTasks() int
		GetTasksPerNamespace() map[namespace.ID]int
		GetTasksPerSlice(namespace.ID) map[Slice]int
		SetTasksPerSlice(Slice, map[namespace.ID]int)

		GetReaderWatermark(readerID int32) tasks.Key
		SetReaderWatermark(readerID int32, watermark tasks.Key)

		GetTotalSlices() int
		SetTotalSlices(int)
	}

	MonitorOptions struct {
		CriticalTotalTasks        dynamicconfig.IntPropertyFn
		CriticalWatermarkAttempts dynamicconfig.IntPropertyFn
		CriticalTotalSlices       dynamicconfig.IntPropertyFn
	}

	monitorImpl struct {
		sync.Mutex

		mitigator Mitigator

		taskStats
		readerStats
		sliceStats

		options *MonitorOptions
	}

	taskStats struct {
		totalTasks int

		// track per namespace stats so that we can know which namespace is offending
		tasksPerNamespace map[namespace.ID]int

		// track per slice stats so that we can know which slice to split when a namespace is offending
		tasksPerSlicePerNamespace map[Slice]map[namespace.ID]int
	}

	readerStats struct {
		progressByReader map[int32]*readerProgess
	}

	readerProgess struct {
		watermark tasks.Key
		attempts  int
	}

	sliceStats struct {
		totalSlices int
	}
)

func newMonitor(
	options *MonitorOptions,
) *monitorImpl {
	return &monitorImpl{
		taskStats: taskStats{
			tasksPerNamespace:         make(map[namespace.ID]int),
			tasksPerSlicePerNamespace: make(map[Slice]map[namespace.ID]int),
		},
		readerStats: readerStats{
			progressByReader: make(map[int32]*readerProgess),
		},
		options: options,
	}
}

func (m *monitorImpl) GetTotalTasks() int {
	m.Lock()
	defer m.Unlock()

	return m.totalTasks
}

func (m *monitorImpl) GetTasksPerNamespace() map[namespace.ID]int {
	m.Lock()
	defer m.Unlock()

	taskPerNamespace := make(map[namespace.ID]int, len(m.tasksPerNamespace))
	maps.Copy(taskPerNamespace, m.tasksPerNamespace)

	return taskPerNamespace
}

func (m *monitorImpl) GetTasksPerSlice(namespaceID namespace.ID) map[Slice]int {
	m.Lock()
	defer m.Unlock()

	tasksPerSlice := make(map[Slice]int, len(m.tasksPerSlicePerNamespace))
	for slice, tasksPerNamespace := range m.tasksPerSlicePerNamespace {
		if pendingTask, ok := tasksPerNamespace[namespaceID]; ok {
			tasksPerSlice[slice] = pendingTask
		}
	}

	return tasksPerSlice
}

func (m *monitorImpl) SetTasksPerSlice(slice Slice, newTasksPerNamespace map[namespace.ID]int) {
	m.Lock()
	defer m.Unlock()

	if newTasksPerNamespace == nil {
		newTasksPerNamespace = make(map[namespace.ID]int)
	}

	if _, ok := m.tasksPerSlicePerNamespace[slice]; !ok {
		m.tasksPerSlicePerNamespace[slice] = make(map[namespace.ID]int, len(newTasksPerNamespace))
	}
	oldTasksPerNamespace := m.tasksPerSlicePerNamespace[slice]
	for namespaceID, oldPendingTasks := range oldTasksPerNamespace {
		if _, ok := newTasksPerNamespace[namespaceID]; !ok {
			m.tasksPerNamespace[namespaceID] -= oldPendingTasks
			m.totalTasks -= oldPendingTasks
		}
	}

	hasPendingTasks := false
	for namespaceID, newPendingTasks := range newTasksPerNamespace {
		oldPendingTasks := oldTasksPerNamespace[namespaceID]
		oldTasksPerNamespace[namespaceID] = newPendingTasks
		if newPendingTasks > 0 {
			hasPendingTasks = true
		}

		delta := newPendingTasks - oldPendingTasks
		m.tasksPerNamespace[namespaceID] += delta
		m.totalTasks += delta
	}

	if !hasPendingTasks {
		delete(m.taskStats.tasksPerSlicePerNamespace, slice)
	}

	maxTotalTasks := m.options.CriticalTotalTasks()
	if m.totalTasks > maxTotalTasks && m.mitigator != nil {
		m.mitigator.Alert(Alert{
			AlertType: AlertTypeQueuePendingTask,
			AlertQueuePendingTaskAttributes: &AlertQueuePendingTaskAttributes{
				CurrentPendingTasks: m.totalTasks,
				MaxPendingTasks:     maxTotalTasks,
			},
		})
	}
}

func (m *monitorImpl) GetReaderWatermark(readerID int32) tasks.Key {
	m.Lock()
	defer m.Unlock()

	return m.progressByReader[readerID].watermark
}

func (m *monitorImpl) SetReaderWatermark(readerID int32, watermark tasks.Key) {
	if readerID != defaultReaderId {
		// for now we only track watermark for the default reader
		return
	}

	// TODO: do not track watermark to max readerID

	if watermark.FireTime == tasks.DefaultFireTime {
		// for now we only track watermark for scheduled queue
		return
	}

	m.Lock()
	defer m.Unlock()

	if _, ok := m.progressByReader[readerID]; !ok {
		m.progressByReader[readerID] = &readerProgess{
			watermark: tasks.NewKey(tasks.DefaultFireTime, 0),
			attempts:  1,
		}
	}

	watermark.FireTime = watermark.FireTime.Truncate(monitorWatermarkPrecision)
	progress := m.progressByReader[readerID]
	if !watermark.FireTime.Equal(progress.watermark.FireTime) {
		progress.watermark = watermark
		progress.attempts = 1
		return
	}

	progress.attempts++
	if progress.attempts > m.options.CriticalWatermarkAttempts() && m.mitigator != nil {
		m.mitigator.Alert(Alert{
			AlertType: AlertTypeReaderWatermark,
			AlertReaderWatermarkAttributes: &AlertReaderWatermarkAttributes{
				ReaderID:         readerID,
				CurrentWatermark: progress.watermark,
			},
		})
	}
}

func (m *monitorImpl) GetTotalSlices() int {
	m.Lock()
	defer m.Unlock()

	return m.totalSlices
}

func (m *monitorImpl) SetTotalSlices(totalSlices int) {
	m.Lock()
	defer m.Unlock()

	m.totalSlices = totalSlices

	maxSliceCount := m.options.CriticalTotalSlices()
	if totalSlices > maxSliceCount && m.mitigator != nil {
		m.mitigator.Alert(Alert{
			AlertType: AlertTypeSliceCount,
			AlertSliceCountAttributes: &AlertSliceCountAttributes{
				CurrentSliceCount: m.totalSlices,
				MaxSliceCount:     maxSliceCount,
			},
		})
	}
}

func (m *monitorImpl) registerMitigator(
	mitigator Mitigator,
) {
	if m.mitigator != nil {
		panic("Mitigator already registered on queue monitor")
	}

	m.mitigator = mitigator
}

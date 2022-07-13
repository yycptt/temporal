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
	"time"

	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/tasks"
	"golang.org/x/exp/slices"
)

var (
	targetThresholdCoefficient = 0.8
)

type (
	action interface {
		run(*readerGroup)
	}

	actionQueuePendingTask struct {
		mitigator *mitigatorImpl
		monitor   Monitor

		attributes *AlertQueuePendingTaskAttributes
	}

	actionReaderWatermark struct {
		mitigator *mitigatorImpl

		attributes *AlertReaderWatermarkAttributes
	}

	actionSliceCount struct {
		mitigator *mitigatorImpl

		attributes *AlertSliceCountAttributes
	}
)

func newQueuePendingTaskAction(
	mitigator *mitigatorImpl,
	monitor Monitor,
	attributes *AlertQueuePendingTaskAttributes,
) action {
	return &actionQueuePendingTask{
		mitigator:  mitigator,
		monitor:    monitor,
		attributes: attributes,
	}
}

type slicePendingTask struct {
	slice                Slice
	namespacePendingTask int
}

func (a *actionQueuePendingTask) run(readerGroup *readerGroup) {
	defer a.mitigator.resolve(AlertTypeQueuePendingTask)

	tasksPerNamespace := a.monitor.GetTasksPerNamespace()
	pq := collection.NewPriorityQueue(func(this, that namespace.ID) bool {
		return tasksPerNamespace[this] > tasksPerNamespace[that]
	})
	for namespaceID := range tasksPerNamespace {
		pq.Add(namespaceID)
	}

	slicesToClear := make(map[Slice][]string)
	slicesPerNamespace := make(map[namespace.ID][]slicePendingTask) // slices reversely ordered

	currentPendingTasks := a.attributes.CurrentPendingTasks
	targetPendingTasks := int(float64(a.attributes.MaxPendingTasks) * targetThresholdCoefficient)
	for currentPendingTasks > targetPendingTasks && !pq.IsEmpty() {
		namespaceID := pq.Remove()

		namespaceSlices, ok := slicesPerNamespace[namespaceID]
		if !ok {
			namespaceSlicesMap := a.monitor.GetTasksPerSlice(namespaceID)
			namespaceSlices = make([]slicePendingTask, 0, len(namespaceSlices))
			for slice, pendingTask := range namespaceSlicesMap {
				namespaceSlices = append(namespaceSlices, slicePendingTask{
					slice:                slice,
					namespacePendingTask: pendingTask,
				})
			}

			slices.SortFunc(namespaceSlices, func(this, that slicePendingTask) bool {
				thisMin := this.slice.Scope().Range.InclusiveMin
				thatMin := that.slice.Scope().Range.InclusiveMin
				return thisMin.CompareTo(thatMin) > 0
			})

			slicesPerNamespace[namespaceID] = namespaceSlices
		}

		if len(namespaceSlices) == 0 {
			continue
		}

		sliceToClear := namespaceSlices[0].slice
		slicesToClear[sliceToClear] = append(slicesToClear[sliceToClear], namespaceID.String())

		tasksPerNamespace[namespaceID] -= namespaceSlices[0].namespacePendingTask
		if tasksPerNamespace[namespaceID] > 0 {
			pq.Add(namespaceID)
		}

		namespaceSlices = namespaceSlices[1:]
		slicesPerNamespace[namespaceID] = namespaceSlices
	}

	for readerID, reader := range readerGroup.readers() {
		// TODO: change the check to max readerID
		if readerID != defaultReaderId {
			cleared := false
			reader.ClearSlices(func(s Slice) bool {
				_, ok := slicesToClear[s]
				cleared = cleared || ok
				return ok
			})
			if cleared {
				reader.Throttle(time.Second) // TODO: add an options for it
			}
			continue
		}

		var splitSlices []Slice
		reader.SplitSlices(func(s Slice) (remaining []Slice) {
			namespaceIDs, ok := slicesToClear[s]
			if !ok {
				return []Slice{s}
			}

			split, remain := s.SplitByPredicate(tasks.NewNamespacePredicate(namespaceIDs))
			split.Clear()
			splitSlices = append(splitSlices, split)
			return []Slice{remain}
		})

		if len(splitSlices) == 0 {
			continue
		}

		nextReader, ok := readerGroup.readerByID(readerID + 1)
		if ok {
			nextReader.MergeSlices(splitSlices...)
		} else {
			nextReader = readerGroup.newReaderWithSlices(readerID+1, splitSlices...)
		}
		nextReader.Throttle(time.Second) // TODO: add an options for it
	}
}

func newReaderWatermarkAction(
	mitigator *mitigatorImpl,
	attributes *AlertReaderWatermarkAttributes,
) action {
	return &actionReaderWatermark{
		mitigator:  mitigator,
		attributes: attributes,
	}
}

func (a *actionReaderWatermark) run(readerGroup *readerGroup) {
	defer a.mitigator.resolve(AlertTypeReaderWatermark)

	// TODO: do not split when readerID is already at max allowed

	reader, ok := readerGroup.readerByID(a.attributes.ReaderID)
	if !ok {
		return
	}

	stuckRange := NewRange(
		a.attributes.CurrentWatermark,
		tasks.NewKey(
			a.attributes.CurrentWatermark.FireTime.Add(monitorWatermarkPrecision),
			a.attributes.CurrentWatermark.TaskID,
		),
	)

	var splitSlices []Slice
	reader.SplitSlices(func(s Slice) []Slice {
		r := s.Scope().Range
		if stuckRange.ContainsRange(r) {
			splitSlices = append(splitSlices, s)
			return nil
		}

		remaining := make([]Slice, 0, 2)
		if s.CanSplitByRange(stuckRange.InclusiveMin) {
			left, right := s.SplitByRange(stuckRange.InclusiveMin)
			remaining = append(remaining, left)
			s = right
		}

		if s.CanSplitByRange(stuckRange.ExclusiveMax) {
			left, right := s.SplitByRange(stuckRange.ExclusiveMax)
			remaining = append(remaining, right)
			s = left
		}

		splitSlices = append(splitSlices, s)
		return remaining
	})

	if len(splitSlices) == 0 {
		return
	}

	nextReader, ok := readerGroup.readerByID(a.attributes.ReaderID + 1)
	if ok {
		nextReader.MergeSlices(splitSlices...)
		return
	}

	readerGroup.newReaderWithSlices(a.attributes.ReaderID+1, splitSlices...)
}

func newSliceCountAction(
	mitigator *mitigatorImpl,
	attributes *AlertSliceCountAttributes,
) action {
	return &actionSliceCount{
		mitigator:  mitigator,
		attributes: attributes,
	}
}

type compactCandidate struct {
	slice    Slice
	distance tasks.Key
}

func (a *actionSliceCount) run(readerGroup *readerGroup) {
	defer a.mitigator.resolve(AlertTypeSliceCount)

	targetSliceCount := int(float64(a.attributes.MaxSliceCount) * targetThresholdCoefficient)
	numSliceToCompact := a.attributes.CurrentSliceCount - targetSliceCount
	pq := collection.NewPriorityQueue(func(this, that *compactCandidate) bool {
		return this.distance.CompareTo(this.distance) < 0
	})

	readers := readerGroup.readerMap
	for readerID, reader := range readers {
		if readerID == defaultReaderId {
			continue
		}

		var prevRange *Range
		reader.WalkSlices(func(s Slice) {
			if prevRange == nil {
				return
			}

			currentRange := s.Scope().Range
			pq.Add(&compactCandidate{
				slice:    s,
				distance: currentRange.InclusiveMin.Sub(prevRange.ExclusiveMax),
			})
			prevRange = &currentRange
		})
	}

	sliceToCompact := make(map[Slice]struct{}, numSliceToCompact)
	for numSliceToCompact > 0 && !pq.IsEmpty() {
		sliceToCompact[pq.Remove().slice] = struct{}{}
	}

	for readerID, reader := range readers {
		if readerID == defaultReaderId {
			continue
		}

		reader.CompactSlices(func(s Slice) bool {
			_, ok := sliceToCompact[s]
			return ok
		})
	}
}

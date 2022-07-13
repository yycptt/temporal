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
	"go.temporal.io/server/common/predicates"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/tasks"
)

type (

	// Slice manages the loading and status tracking of all
	// tasks within its Scope.
	// It also provides methods for splitting or merging with
	// another slice either by range or by predicate.
	Slice interface {
		Scope() Scope
		CanSplitByRange(tasks.Key) bool
		SplitByRange(tasks.Key) (left Slice, right Slice)
		SplitByPredicate(tasks.Predicate) (pass Slice, fail Slice)
		CanMergeWithSlice(Slice) bool
		MergeWithSlice(Slice) []Slice
		CompactWithSlice(Slice) Slice
		ShrinkRange()
		SelectTasks(int) ([]Executable, error)
		MoreTasks() bool
		Clear()
	}

	ExecutableInitializer func(tasks.Task) Executable

	SliceImpl struct {
		paginationFnProvider  PaginationFnProvider
		executableInitializer ExecutableInitializer

		destroyed bool

		scope     Scope
		iterators []Iterator

		// TODO: make task tracking a separate component
		monitor             Monitor
		pendingExecutables  map[tasks.Key]Executable
		pendingPerNamesapce map[namespace.ID]int
	}
)

func NewSlice(
	paginationFnProvider PaginationFnProvider,
	executableInitializer ExecutableInitializer,
	monitor Monitor,
	scope Scope,
) *SliceImpl {
	return &SliceImpl{
		paginationFnProvider:  paginationFnProvider,
		executableInitializer: executableInitializer,
		scope:                 scope,
		iterators: []Iterator{
			NewIterator(paginationFnProvider, scope.Range),
		},
		monitor:             monitor,
		pendingExecutables:  make(map[tasks.Key]Executable),
		pendingPerNamesapce: make(map[namespace.ID]int),
	}
}

func (s *SliceImpl) Scope() Scope {
	s.stateSanityCheck()
	return s.scope
}

func (s *SliceImpl) CanSplitByRange(key tasks.Key) bool {
	s.stateSanityCheck()
	return s.scope.CanSplitByRange(key)
}

func (s *SliceImpl) SplitByRange(key tasks.Key) (left Slice, right Slice) {
	if !s.CanSplitByRange(key) {
		panic(fmt.Sprintf("Unable to split queue slice with range %v at %v", s.scope.Range, key))
	}

	return s.splitByRange(key)
}

func (s *SliceImpl) splitByRange(key tasks.Key) (left *SliceImpl, right *SliceImpl) {

	leftScope, rightScope := s.scope.SplitByRange(key)
	leftExecutables, leftPendingPerNamespace, rightExecutables, rightPendingPerNamespace := s.splitExecutables(leftScope, rightScope)

	leftIterators := make([]Iterator, 0, len(s.iterators)/2)
	rightIterators := make([]Iterator, 0, len(s.iterators)/2)
	for _, iter := range s.iterators {
		iterRange := iter.Range()
		if leftScope.Range.ContainsRange(iterRange) {
			leftIterators = append(leftIterators, iter)
			continue
		}

		if rightScope.Range.ContainsRange(iterRange) {
			rightIterators = append(rightIterators, iter)
			continue
		}

		leftIter, rightIter := iter.Split(key)
		leftIterators = append(leftIterators, leftIter)
		rightIterators = append(rightIterators, rightIter)
	}

	left = &SliceImpl{
		paginationFnProvider:  s.paginationFnProvider,
		executableInitializer: s.executableInitializer,
		scope:                 leftScope,
		iterators:             leftIterators,
		monitor:               s.monitor,
		pendingExecutables:    leftExecutables,
		pendingPerNamesapce:   leftPendingPerNamespace,
	}
	right = &SliceImpl{
		paginationFnProvider:  s.paginationFnProvider,
		executableInitializer: s.executableInitializer,
		scope:                 rightScope,
		iterators:             rightIterators,
		monitor:               s.monitor,
		pendingExecutables:    rightExecutables,
		pendingPerNamesapce:   rightPendingPerNamespace,
	}

	s.monitor.SetTasksPerSlice(left, leftPendingPerNamespace)
	s.monitor.SetTasksPerSlice(right, rightPendingPerNamespace)

	s.destroy()
	return left, right
}

func (s *SliceImpl) SplitByPredicate(predicate tasks.Predicate) (pass Slice, fail Slice) {
	s.stateSanityCheck()

	passScope, failScope := s.scope.SplitByPredicate(predicate)
	passExecutables, passPendingPerNamespace, failExecutables, failPendingPerNamespace := s.splitExecutables(passScope, failScope)

	passIterators := make([]Iterator, 0, len(s.iterators))
	failIterators := make([]Iterator, 0, len(s.iterators))
	for _, iter := range s.iterators {
		// iter.Remaining() is basically a deep copy of iter
		passIterators = append(passIterators, iter)
		failIterators = append(failIterators, iter.Remaining())
	}

	pass = &SliceImpl{
		paginationFnProvider:  s.paginationFnProvider,
		executableInitializer: s.executableInitializer,
		scope:                 passScope,
		iterators:             passIterators,
		monitor:               s.monitor,
		pendingExecutables:    passExecutables,
		pendingPerNamesapce:   passPendingPerNamespace,
	}
	fail = &SliceImpl{
		paginationFnProvider:  s.paginationFnProvider,
		executableInitializer: s.executableInitializer,
		scope:                 failScope,
		iterators:             failIterators,
		monitor:               s.monitor,
		pendingExecutables:    failExecutables,
		pendingPerNamesapce:   failPendingPerNamespace,
	}

	s.monitor.SetTasksPerSlice(pass, passPendingPerNamespace)
	s.monitor.SetTasksPerSlice(fail, failPendingPerNamespace)

	s.destroy()
	return pass, fail
}

func (s *SliceImpl) splitExecutables(
	thisScope Scope,
	thatScope Scope,
) (map[tasks.Key]Executable, map[namespace.ID]int, map[tasks.Key]Executable, map[namespace.ID]int) {
	thisExecutables, thisPendingPerNamesapce := s.pendingExecutables, s.pendingPerNamesapce
	thatExecutables := make(map[tasks.Key]Executable, len(s.pendingExecutables)/2)
	thatPendingPerNamespace := make(map[namespace.ID]int)
	for key, executable := range s.pendingExecutables {
		if thisScope.Contains(executable) {
			continue
		}

		if !thatScope.Contains(executable) {
			panic(fmt.Sprintf("Queue slice encountered task doesn't belong to its scope, scope: %v, task: %v, task type: %v",
				s.scope, executable.GetTask(), executable.GetType()))
		}

		namespaceID := namespace.ID(executable.GetNamespaceID())

		delete(thisExecutables, key)
		thisPendingPerNamesapce[namespaceID]--

		thatExecutables[key] = executable
		thatPendingPerNamespace[namespaceID]++
	}
	return thisExecutables, thisPendingPerNamesapce, thatExecutables, thatPendingPerNamespace
}

func (s *SliceImpl) CanMergeWithSlice(slice Slice) bool {
	s.stateSanityCheck()

	return s != slice && s.scope.Range.CanMerge(slice.Scope().Range)
}

func (s *SliceImpl) MergeWithSlice(slice Slice) []Slice {
	if s.scope.Range.InclusiveMin.CompareTo(slice.Scope().Range.InclusiveMin) > 0 {
		return slice.MergeWithSlice(s)
	}

	if !s.CanMergeWithSlice(slice) {
		panic(fmt.Sprintf("Unable to merge queue slice having scope %v with slice having scope %v", s.scope, slice.Scope()))
	}

	incomingSlice, ok := slice.(*SliceImpl)
	if !ok {
		panic(fmt.Sprintf("Unable to merge queue slice of type %T with type %T", s, slice))
	}

	if s.scope.CanMergeByRange(incomingSlice.scope) {
		return []Slice{s.mergeByRange(incomingSlice)}
	}

	mergedSlices := make([]Slice, 0, 3)
	currentLeftSlice, currentRightSlice := s.splitByRange(incomingSlice.Scope().Range.InclusiveMin)
	if !currentLeftSlice.scope.IsEmpty() {
		mergedSlices = append(mergedSlices, currentLeftSlice)
	}

	if currentRightMax := currentRightSlice.Scope().Range.ExclusiveMax; incomingSlice.CanSplitByRange(currentRightMax) {
		leftIncomingSlice, rightIncomingSlice := incomingSlice.splitByRange(currentRightMax)
		mergedMidSlice := currentRightSlice.mergeByPredicate(leftIncomingSlice)
		if !mergedMidSlice.scope.IsEmpty() {
			mergedSlices = append(mergedSlices, mergedMidSlice)
		}
		if !rightIncomingSlice.scope.IsEmpty() {
			mergedSlices = append(mergedSlices, rightIncomingSlice)
		}
	} else {
		currentMidSlice, currentRightSlice := currentRightSlice.splitByRange(incomingSlice.Scope().Range.ExclusiveMax)
		mergedMidSlice := currentMidSlice.mergeByPredicate(incomingSlice)
		if !mergedMidSlice.scope.IsEmpty() {
			mergedSlices = append(mergedSlices, mergedMidSlice)
		}
		if !currentRightSlice.scope.IsEmpty() {
			mergedSlices = append(mergedSlices, currentRightSlice)
		}
	}

	return mergedSlices

}

func (s *SliceImpl) mergeByRange(incomingSlice *SliceImpl) *SliceImpl {
	mergedOutstandingExecutables, mergedPendingTasks := s.mergeExecutables(incomingSlice)

	mergedSlice := &SliceImpl{
		paginationFnProvider:  s.paginationFnProvider,
		executableInitializer: s.executableInitializer,
		scope:                 s.scope.MergeByRange(incomingSlice.scope),
		iterators:             s.mergeIterators(incomingSlice),
		monitor:               s.monitor,
		pendingExecutables:    mergedOutstandingExecutables,
		pendingPerNamesapce:   mergedPendingTasks,
	}

	s.destroy()
	incomingSlice.destroy()

	mergedSlice.monitor.SetTasksPerSlice(mergedSlice, mergedSlice.pendingPerNamesapce)

	return mergedSlice
}

func (s *SliceImpl) mergeByPredicate(incomingSlice *SliceImpl) *SliceImpl {
	mergedOutstandingExecutables, mergedPendingTasks := s.mergeExecutables(incomingSlice)

	mergedSlice := &SliceImpl{
		paginationFnProvider:  s.paginationFnProvider,
		executableInitializer: s.executableInitializer,
		scope:                 s.scope.MergeByPredicate(incomingSlice.scope),
		iterators:             s.mergeIterators(incomingSlice),
		monitor:               s.monitor,
		pendingExecutables:    mergedOutstandingExecutables,
		pendingPerNamesapce:   mergedPendingTasks,
	}

	s.destroy()
	incomingSlice.destroy()

	mergedSlice.monitor.SetTasksPerSlice(mergedSlice, mergedSlice.pendingPerNamesapce)

	return mergedSlice
}

func (s *SliceImpl) mergeExecutables(incomingSlice *SliceImpl) (map[tasks.Key]Executable, map[namespace.ID]int) {
	thisExecutables, thisPendingTasks := s.pendingExecutables, s.pendingPerNamesapce
	thatExecutables, thatPendingTasks := incomingSlice.pendingExecutables, incomingSlice.pendingPerNamesapce
	if len(thisExecutables) < len(thatExecutables) {
		thisExecutables, thatExecutables = thatExecutables, thisExecutables
		thisPendingTasks, thatPendingTasks = thatPendingTasks, thisPendingTasks
	}

	for key, executable := range thatExecutables {
		thisExecutables[key] = executable
		thisPendingTasks[namespace.ID(executable.GetNamespaceID())]++
	}

	return thisExecutables, thisPendingTasks
}

func (s *SliceImpl) mergeIterators(incomingSlice *SliceImpl) []Iterator {
	mergedIterators := make([]Iterator, 0, len(s.iterators)+len(incomingSlice.iterators))
	currentIterIdx := 0
	incomingIterIdx := 0
	for currentIterIdx < len(s.iterators) && incomingIterIdx < len(incomingSlice.iterators) {
		currentIter := s.iterators[currentIterIdx]
		incomingIter := incomingSlice.iterators[incomingIterIdx]

		if currentIter.Range().InclusiveMin.CompareTo(incomingIter.Range().InclusiveMin) < 0 {
			mergedIterators = s.appendIterator(mergedIterators, currentIter)
			currentIterIdx++
		} else {
			mergedIterators = s.appendIterator(mergedIterators, incomingIter)
			incomingIterIdx++
		}
	}

	for _, iterator := range s.iterators[currentIterIdx:] {
		mergedIterators = s.appendIterator(mergedIterators, iterator)
	}
	for _, iterator := range incomingSlice.iterators[incomingIterIdx:] {
		mergedIterators = s.appendIterator(mergedIterators, iterator)
	}

	validateIteratorsOrderedDisjoint(mergedIterators)

	return mergedIterators
}

func (s *SliceImpl) appendIterator(
	iterators []Iterator,
	iterator Iterator,
) []Iterator {
	if len(iterators) == 0 {
		return []Iterator{iterator}
	}

	size := len(iterators)
	if iterators[size-1].CanMerge(iterator) {
		iterators[size-1] = iterators[size-1].Merge(iterator)
		return iterators
	}

	return append(iterators, iterator)
}

func (s *SliceImpl) CompactWithSlice(slice Slice) Slice {
	s.stateSanityCheck()

	incomingSlice, ok := slice.(*SliceImpl)
	if !ok {
		panic(fmt.Sprintf("Unable to compact queue slice of type %T with type %T", s, slice))
	}
	incomingSlice.stateSanityCheck()

	compactedScope := NewScope(
		NewRange(
			tasks.MinKey(s.scope.Range.InclusiveMin, incomingSlice.scope.Range.InclusiveMin),
			tasks.MaxKey(s.scope.Range.ExclusiveMax, incomingSlice.scope.Range.ExclusiveMax),
		),
		// TODO: special check if the predicates are the same type
		// define tasks.UnionPredicate?
		predicates.Or(s.scope.Predicate, incomingSlice.scope.Predicate),
	)

	mergedOutstandingExecutables, mergedPendingTasks := s.mergeExecutables(incomingSlice)

	compactedSlice := &SliceImpl{
		paginationFnProvider:  s.paginationFnProvider,
		executableInitializer: s.executableInitializer,
		scope:                 compactedScope,
		iterators:             s.mergeIterators(incomingSlice),
		monitor:               s.monitor,
		pendingExecutables:    mergedOutstandingExecutables,
		pendingPerNamesapce:   mergedPendingTasks,
	}

	s.destroy()
	incomingSlice.destroy()

	compactedSlice.monitor.SetTasksPerSlice(compactedSlice, compactedSlice.pendingPerNamesapce)

	return nil
}

func (s *SliceImpl) ShrinkRange() {
	s.stateSanityCheck()

	minPendingTaskKey := tasks.MaximumKey
	for key, executable := range s.pendingExecutables {
		if s.pendingExecutables[key].State() == ctasks.TaskStateAcked {
			s.pendingPerNamesapce[namespace.ID(executable.GetNamespaceID())]--
			delete(s.pendingExecutables, key)
			continue
		}

		minPendingTaskKey = tasks.MinKey(minPendingTaskKey, key)
	}

	s.monitor.SetTasksPerSlice(s, s.pendingPerNamesapce)

	minIteratorKey := tasks.MaximumKey
	if len(s.iterators) != 0 {
		minIteratorKey = s.iterators[0].Range().InclusiveMin
	}

	// pick min key for tasks in memory and in persistence
	newRangeMin := tasks.MinKey(minPendingTaskKey, minIteratorKey)

	// no pending task in memory and in persistence
	if newRangeMin == tasks.MaximumKey {
		newRangeMin = s.scope.Range.ExclusiveMax
	}

	s.scope.Range.InclusiveMin = newRangeMin
}

func (s *SliceImpl) SelectTasks(batchSize int) ([]Executable, error) {
	s.stateSanityCheck()

	if len(s.iterators) == 0 {
		return []Executable{}, nil
	}

	defer s.monitor.SetTasksPerSlice(s, s.pendingPerNamesapce)

	executables := make([]Executable, 0, batchSize)
	for len(executables) < batchSize && len(s.iterators) != 0 {
		if s.iterators[0].HasNext() {
			task, err := s.iterators[0].Next()
			if err != nil {
				s.iterators[0] = s.iterators[0].Remaining()
				// NOTE: we must return the executables here
				return executables, err
			}

			taskKey := task.GetKey()
			if !s.scope.Range.ContainsKey(taskKey) {
				panic(fmt.Sprintf("Queue slice get task from iterator doesn't belong to its range, range: %v, task key %v",
					s.scope.Range, taskKey))
			}

			if !s.scope.Predicate.Test(task) {
				continue
			}

			executable := s.executableInitializer(task)
			s.pendingExecutables[taskKey] = executable
			s.pendingPerNamesapce[namespace.ID(task.GetNamespaceID())]++
			executables = append(executables, executable)
		} else {
			s.iterators = s.iterators[1:]
		}
	}

	return executables, nil
}

func (s *SliceImpl) MoreTasks() bool {
	s.stateSanityCheck()

	return len(s.iterators) != 0
}

func (s *SliceImpl) Clear() {
	s.stateSanityCheck()

	s.ShrinkRange()

	executableMaxKey := tasks.MinimumKey
	for key, executable := range s.pendingExecutables {
		executable.Cancel()

		executableMaxKey = tasks.MaxKey(executableMaxKey, key)
		delete(s.pendingExecutables, key)
	}

	if executableMaxKey != tasks.MinimumKey {
		iterator := NewIterator(
			s.paginationFnProvider,
			NewRange(s.scope.Range.InclusiveMin, executableMaxKey.Next()),
		)

		iterators := make([]Iterator, 0, len(s.iterators)+1)
		iterators = append(iterators, iterator)
		iterators = append(iterators, s.iterators...)
		s.iterators = iterators
	}

	s.pendingPerNamesapce = make(map[namespace.ID]int)
	s.monitor.SetTasksPerSlice(s, s.pendingPerNamesapce)
}

func (s *SliceImpl) destroy() {
	s.destroyed = true
	s.iterators = nil
	s.pendingExecutables = nil
	s.pendingPerNamesapce = nil
	s.monitor.SetTasksPerSlice(s, nil)
}

func (s *SliceImpl) stateSanityCheck() {
	if s.destroyed {
		panic("Can not invoke method on destroyed queue slice")
	}
}

func validateIteratorsOrderedDisjoint(
	iterators []Iterator,
) {
	if len(iterators) <= 1 {
		return
	}

	for idx, iterator := range iterators[:len(iterators)-1] {
		nextIterator := iterators[idx+1]
		if iterator.Range().ExclusiveMax.CompareTo(nextIterator.Range().InclusiveMin) >= 0 {
			panic(fmt.Sprintf(
				"Found overlapping iterators in iterator list, left range: %v, right range: %v",
				iterator.Range(),
				nextIterator.Range(),
			))
		}
	}
}

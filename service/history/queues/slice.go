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
	"sort"

	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/tasks"
)

type (
	Slice interface {
		Scope() Scope
		SplitRange(tasks.Key) (left Slice, right Slice)
		SplitPredicate(tasks.Predicate) (pass Slice, fail Slice)
		MergeRange(Slice) Slice // how to validate predicates are the same? best effort?
		MergePredicate(Slice) Slice
		ShrinkRange() // basically update ack level
		SelectTasks(int) ([]Executable, error)
		// TODO: CanSplit/MergeRange/Predicate() ?
	}

	executableInitializer func(tasks.Task) Executable

	SliceImpl struct {
		paginationFnProvider  paginationFnProvider // TODO: do we need this?
		executableInitializer executableInitializer

		scope                  Scope
		outstandingExecutables map[tasks.Key]Executable // use a btree instead
		iterators              []Iterator
	}
)

func NewSlice(
	paginationFnProvider paginationFnProvider,
	executableInitializer executableInitializer,
	scope Scope,
) *SliceImpl {
	return &SliceImpl{
		paginationFnProvider:   paginationFnProvider,
		executableInitializer:  executableInitializer,
		scope:                  scope,
		outstandingExecutables: make(map[tasks.Key]Executable),
		iterators: []Iterator{
			NewIterator(paginationFnProvider, scope.Range),
		},
	}
}

func (s *SliceImpl) Scope() Scope {
	return s.scope
}

func (s *SliceImpl) SplitRange(key tasks.Key) (leftSlice Slice, rightSlice Slice) {
	if !s.scope.CanSplitRange(key) {
		panic(fmt.Sprintf("Unable to split queue slice with range %v at %v", s.scope.Range, key))
	}

	leftScope, rightScope := s.scope.SplitRange(key)
	leftExecutables, rightExecutables := s.splitExecutables(leftScope, rightScope)

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

	leftSlice = &SliceImpl{
		paginationFnProvider:   s.paginationFnProvider,
		executableInitializer:  s.executableInitializer,
		scope:                  leftScope,
		outstandingExecutables: leftExecutables,
		iterators:              leftIterators,
	}
	rightSlice = &SliceImpl{
		paginationFnProvider:   s.paginationFnProvider,
		executableInitializer:  s.executableInitializer,
		scope:                  rightScope,
		outstandingExecutables: rightExecutables,
		iterators:              rightIterators,
	}

	return leftSlice, rightSlice
}

func (s *SliceImpl) SplitPredicate(predicate tasks.Predicate) (passSlice Slice, failSlice Slice) {
	passScope, failScope := s.scope.SplitPredicate(predicate)
	passExecutables, failExecutables := s.splitExecutables(passScope, failScope)

	passIterators := make([]Iterator, 0, len(s.iterators))
	failIterators := make([]Iterator, 0, len(s.iterators))
	for _, iter := range s.iterators {
		passIterators = append(passIterators, iter.Remaining())
		failIterators = append(failIterators, iter.Remaining())
	}

	passSlice = &SliceImpl{
		paginationFnProvider:   s.paginationFnProvider,
		executableInitializer:  s.executableInitializer,
		scope:                  passScope,
		outstandingExecutables: passExecutables,
		iterators:              passIterators,
	}
	failSlice = &SliceImpl{
		paginationFnProvider:   s.paginationFnProvider,
		executableInitializer:  s.executableInitializer,
		scope:                  failScope,
		outstandingExecutables: failExecutables,
		iterators:              failIterators,
	}

	return passSlice, failSlice
}

func (s *SliceImpl) splitExecutables(
	thisScope Scope,
	thatScope Scope,
) (map[tasks.Key]Executable, map[tasks.Key]Executable) {
	thisExecutables := make(map[tasks.Key]Executable, len(s.outstandingExecutables)/2)
	thatExecutables := make(map[tasks.Key]Executable, len(s.outstandingExecutables)/2)
	for key, executable := range s.outstandingExecutables {
		if thisScope.Contains(executable) {
			thisExecutables[key] = executable
			continue
		}

		if !thatScope.Contains(executable) {
			panic(fmt.Sprintf("Queue slice encountered task doesn't belong to its scope, scope: %v, task: %v, task type: %v",
				s.scope, executable.GetTask(), executable.GetType()))
		}
		thatExecutables[key] = executable
	}
	return thisExecutables, thatExecutables
}

func (s *SliceImpl) MergeRange(slice Slice) Slice {
	// TODO: how to validate predicates are the same?

	incomingSlice, ok := slice.(*SliceImpl)
	if !ok {
		panic(fmt.Sprintf("Unabled to merge queue slice of type %T with type %T", s, slice))
	}

	if !s.scope.CanMergeRange(incomingSlice.scope.Range) {
		panic(fmt.Sprintf("Unalbed to merge queue slice having range %v with slice having range %v", s.scope, incomingSlice.scope))
	}

	mergedScope := s.scope.MergeRange(incomingSlice.scope.Range)

	return &SliceImpl{
		paginationFnProvider:   s.paginationFnProvider,
		executableInitializer:  s.executableInitializer,
		scope:                  mergedScope,
		outstandingExecutables: s.mergeExecutables(incomingSlice),
		iterators:              s.mergeIterators(incomingSlice),
	}
}

func (s *SliceImpl) MergePredicate(slice Slice) Slice {
	incomingSlice, ok := slice.(*SliceImpl)
	if !ok {
		panic(fmt.Sprintf("Unabled to merge queue slice of type %T with type %T", s, slice))
	}

	if !s.scope.Range.Equal(incomingSlice.scope.Range) {
		panic(fmt.Sprintf("Unalbed to merge queue slice having range %v with slice having range %v", s.scope, incomingSlice.scope))
	}

	mergedScope := s.scope.MergePredicate(incomingSlice.scope.Predicate)

	return &SliceImpl{
		paginationFnProvider:   s.paginationFnProvider,
		executableInitializer:  s.executableInitializer,
		scope:                  mergedScope,
		outstandingExecutables: s.mergeExecutables(incomingSlice),
		iterators:              s.mergeIterators(incomingSlice),
	}
}

func (s *SliceImpl) mergeExecutables(incomingSlice *SliceImpl) map[tasks.Key]Executable {
	mergedExecutables := make(map[tasks.Key]Executable, len(s.outstandingExecutables)+len(incomingSlice.outstandingExecutables))
	for key, executable := range s.outstandingExecutables {
		mergedExecutables[key] = executable
	}
	for key, executable := range incomingSlice.outstandingExecutables {
		mergedExecutables[key] = executable
	}

	return mergedExecutables
}

func (s *SliceImpl) mergeIterators(incomingSlice *SliceImpl) []Iterator {
	mergedIterators := make([]Iterator, 0, len(s.iterators)+len(incomingSlice.iterators))
	currentIterIdx := 0
	incomingIterIdx := 0
	for currentIterIdx < len(s.iterators) && incomingIterIdx < len(incomingSlice.iterators) {
		currentIter := s.iterators[currentIterIdx]
		incomingIter := incomingSlice.iterators[currentIterIdx]
		if currentIter.CanMerge(incomingIter) {
			mergedIterators = append(mergedIterators, currentIter.Merge(incomingIter))
			currentIterIdx++
			incomingIterIdx++
			continue
		}

		if currentIter.Range().ExclusiveMax.CompareTo(incomingIter.Range().InclusiveMin) < 0 {
			mergedIterators = append(mergedIterators, currentIter)
			currentIterIdx++
		} else {
			mergedIterators = append(mergedIterators, incomingIter)
			incomingIterIdx++
		}
	}
	mergedIterators = append(mergedIterators, s.iterators[currentIterIdx:]...)
	mergedIterators = append(mergedIterators, incomingSlice.iterators[incomingIterIdx:]...)

	return mergedIterators
}

func (s *SliceImpl) ShrinkRange() {
	var taskKeys tasks.Keys
	taskKeys = make([]tasks.Key, 0, len(s.outstandingExecutables))
	for key := range s.outstandingExecutables {
		taskKeys = append(taskKeys, key)
	}
	sort.Sort(taskKeys)

	for _, key := range taskKeys {
		if s.outstandingExecutables[key].State() == ctasks.TaskStateAcked {
			delete(s.outstandingExecutables, key)
			continue
		}

		s.scope.Range.InclusiveMin = key
		break
	}

	if len(s.outstandingExecutables) == 0 {
		s.scope.Range.InclusiveMin = s.scope.Range.ExclusiveMax
	}
}

func (s *SliceImpl) SelectTasks(batchSize int) ([]Executable, error) {
	if len(s.iterators) == 0 {
		return []Executable{}, nil
	}

	executables := make([]Executable, 0, batchSize)
	for len(executables) < batchSize && len(s.iterators) != 0 {
		if s.iterators[0].HasNext() {
			task, err := s.iterators[0].Next()
			if err != nil {
				s.iterators[0] = s.iterators[0].Remaining()
				return nil, err
			}

			taskKey := task.GetKey()
			if s.scope.Range.ContainsKey(taskKey) {
				panic(fmt.Sprintf("Queue slice get task from iterator doesn't belong to its range, range: %v, task key %v",
					s.scope.Range, taskKey))
			}

			if !s.scope.Predicate.Test(task) {
				continue
			}

			executable := s.executableInitializer(task)
			s.outstandingExecutables[taskKey] = executable
			executables = append(executables, executable)
		} else {
			s.iterators = s.iterators[1:]
		}
	}

	return executables, nil
}

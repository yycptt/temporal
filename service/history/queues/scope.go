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
	"go.temporal.io/server/common/predicates"
	"go.temporal.io/server/service/history/tasks"
)

type (
	Scope struct {
		Range     tasks.Range
		Predicate tasks.Predicate
	}
)

func NewScope(
	r tasks.Range,
	predicate tasks.Predicate,
) Scope {
	return Scope{
		Range:     r,
		Predicate: predicate,
	}
}

func (s *Scope) CanSplitRange(
	key tasks.Key,
) bool {
	return s.Range.CanSplit(key)
}

func (s *Scope) SplitRange(
	key tasks.Key,
) (left Scope, right Scope) {
	leftRange, rightRange := s.Range.Split(key)
	return NewScope(leftRange, s.Predicate), NewScope(rightRange, s.Predicate)
}

func (s *Scope) SplitPredicate(
	predicate tasks.Predicate,
) (pass Scope, fail Scope) {
	passScope := NewScope(
		s.Range,
		predicates.NewAnd(s.Predicate, predicate),
	)
	failScope := NewScope(
		s.Range,
		predicates.NewAnd[tasks.Task](
			s.Predicate,
			predicates.NewNot(predicate),
		),
	)
	return passScope, failScope
}

func (s *Scope) CanMergeRange(
	input tasks.Range,
) bool {
	return s.Range.CanMerge(input)
}

func (s *Scope) MergeRange(
	input tasks.Range,
) Scope {
	return NewScope(s.Range.Merge(input), s.Predicate)
}

func (s *Scope) MergePredicate(
	predicate tasks.Predicate,
) Scope {
	return NewScope(s.Range, predicates.NewOr(s.Predicate, predicate))
}

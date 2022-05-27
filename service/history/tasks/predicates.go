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

package tasks

import (
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/predicates"
)

var (
	_ predicates.Predicate[Task] = (*NamespacePredicate)(nil)
	_ predicates.Predicate[Task] = (*TypePredicate)(nil)
)

type (
	NamespacePredicate struct {
		namespaceIDs map[string]struct{}
	}

	TypePredicate struct {
		types map[enumsspb.TaskType]struct{}
	}
)

func NewNamespacePredicate(
	namespaceIDs []string,
) *NamespacePredicate {
	namespaceIDMap := make(map[string]struct{}, len(namespaceIDs))
	for _, id := range namespaceIDs {
		namespaceIDMap[id] = struct{}{}
	}

	return &NamespacePredicate{
		namespaceIDs: namespaceIDMap,
	}
}

func (n *NamespacePredicate) Test(task Task) bool {
	_, ok := n.namespaceIDs[task.GetNamespaceID()]
	return ok
}

func NewTypePredicate(
	types []enumsspb.TaskType,
) *TypePredicate {
	typeMap := make(map[enumsspb.TaskType]struct{}, len(types))
	for _, taskType := range types {
		typeMap[taskType] = struct{}{}
	}

	return &TypePredicate{
		types: typeMap,
	}
}

func (t *TypePredicate) Test(task Task) bool {
	_, ok := t.types[task.GetType()]
	return ok
}

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

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/predicates"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/tasks"
)

func ToPersistenceQueueState(
	queueState *queueState,
	categoryType tasks.CategoryType,
) *persistencespb.QueueState {
	readerStates := make(map[int32]*persistencespb.QueueReaderState)
	for id, scopes := range queueState.readerScopes {
		persistenceScopes := make([]*persistencespb.QueueSliceScope, 0, len(scopes))
		for _, scope := range scopes {
			persistenceScopes = append(persistenceScopes, ToPersistenceScope(scope, categoryType))
		}
		readerStates[id] = &persistencespb.QueueReaderState{
			Scopes: persistenceScopes,
		}
	}

	return &persistencespb.QueueState{
		ReaderStates:        readerStates,
		ExclusiveMaxReadKey: ToPersistenceTaskKey(queueState.exclusiveMaxReadKey, categoryType),
	}
}

func FromPersistenceQueueState(
	state *persistencespb.QueueState,
	categoryType tasks.CategoryType,
) *queueState {
	readerScopes := make(map[int32][]Scope, len(state.ReaderStates))
	for id, persistenceReaderState := range state.ReaderStates {
		scopes := make([]Scope, 0, len(persistenceReaderState.Scopes))
		for _, persistenceScope := range persistenceReaderState.Scopes {
			scopes = append(scopes, FromPersistenceScope(persistenceScope, categoryType))
		}
		readerScopes[id] = scopes
	}

	return &queueState{
		readerScopes:        readerScopes,
		exclusiveMaxReadKey: FromPersistenceTaskKey(state.ExclusiveMaxReadKey, categoryType),
	}
}

func ToPersistenceScope(
	scope Scope,
	categoryType tasks.CategoryType,
) *persistencespb.QueueSliceScope {
	return &persistencespb.QueueSliceScope{
		Range:     ToPersistenceRange(scope.Range, categoryType),
		Predicate: ToPersistencePredicate(scope.Predicate),
	}
}

func FromPersistenceScope(
	scope *persistencespb.QueueSliceScope,
	categoryType tasks.CategoryType,
) Scope {
	return NewScope(
		FromPersistenceRange(scope.Range, categoryType),
		FromPersistencePredicate(scope.Predicate),
	)
}

func ToPersistenceRange(
	r Range,
	categoryType tasks.CategoryType,
) *persistencespb.QueueSliceRange {
	return &persistencespb.QueueSliceRange{
		InclusiveMin: ToPersistenceTaskKey(r.InclusiveMin, categoryType),
		ExclusiveMax: ToPersistenceTaskKey(r.ExclusiveMax, categoryType),
	}
}

func FromPersistenceRange(
	r *persistencespb.QueueSliceRange,
	categoryType tasks.CategoryType,
) Range {
	return NewRange(
		FromPersistenceTaskKey(r.InclusiveMin, categoryType),
		FromPersistenceTaskKey(r.ExclusiveMax, categoryType),
	)
}

func ToPersistenceTaskKey(
	key tasks.Key,
	categoryType tasks.CategoryType,
) int64 {
	if categoryType == tasks.CategoryTypeImmediate {
		return key.TaskID
	}
	return key.FireTime.UnixNano()
}

func FromPersistenceTaskKey(
	key int64,
	categoryType tasks.CategoryType,
) tasks.Key {
	if categoryType == tasks.CategoryTypeImmediate {
		return tasks.NewImmediateKey(key)
	}
	return tasks.NewKey(timestamp.UnixOrZeroTime(key), 0)
}

func ToPersistencePredicate(
	predicate tasks.Predicate,
) *persistencespb.Predicate {
	switch predicate := predicate.(type) {
	case *predicates.AllImpl[tasks.Task]:
		return ToPersistenceAllPredicate(predicate)
	case *predicates.EmptyImpl[tasks.Task]:
		return ToPersistenceEmptyPredicate(predicate)
	case *predicates.AndImpl[tasks.Task]:
		return ToPersistenceAndPredicate(predicate)
	case *predicates.OrImpl[tasks.Task]:
		return ToPersistenceOrPredicate(predicate)
	case *predicates.NotImpl[tasks.Task]:
		return ToPersistenceNotPredicate(predicate)
	case *tasks.NamespacePredicate:
		return ToPersistenceNamespaceIDPredicate(predicate)
	case *tasks.TypePredicate:
		return ToPersistenceTaskTypePredicate(predicate)
	default:
		panic(fmt.Sprintf("unknown task predicate type: %T", predicate))
	}
}

func FromPersistencePredicate(
	predicate *persistencespb.Predicate,
) tasks.Predicate {
	switch predicate.GetPredicateType() {
	case enumsspb.PREDICATE_TYPE_ALL:
		return FromPersistenceAllPredicate(predicate.GetAllPredicateAttributes())
	case enumsspb.PREDICATE_TYPE_EMPTY:
		return FromPersistenceEmptyPredicate(predicate.GetEmptyPredicateAttributes())
	case enumsspb.PREDICATE_TYPE_AND:
		return FromPersistenceAndPredicate(predicate.GetAndPredicateAttributes())
	case enumsspb.PREDICATE_TYPE_OR:
		return FromPersistenceOrPredicate(predicate.GetOrPredicateAttributes())
	case enumsspb.PREDICATE_TYPE_NOT:
		return FromPersistenceNotPredicate(predicate.GetNotPredicateAttributes())
	case enumsspb.PREDICATE_TYPE_NAMESPACEID:
		return FromPersistenceNamespaceIDPredicate(predicate.GetNamespaceIdPredicateAttributes())
	case enumsspb.PREDICATE_TYPE_TASKTYPE:
		return FromPersistenceTaskTypePredicate(predicate.GetTaskTypePredicateAttributes())
	default:
		panic(fmt.Sprintf("unknown persistence task predicate type: %v", predicate.GetPredicateType()))
	}
}

func ToPersistenceAllPredicate(
	_ *predicates.AllImpl[tasks.Task],
) *persistencespb.Predicate {
	return &persistencespb.Predicate{
		PredicateType: enumsspb.PREDICATE_TYPE_ALL,
		Attributes:    &persistencespb.Predicate_AllPredicateAttributes{},
	}
}

func FromPersistenceAllPredicate(
	_ *persistencespb.AllPredicateAttributes,
) tasks.Predicate {
	return predicates.All[tasks.Task]()
}

func ToPersistenceEmptyPredicate(
	_ *predicates.EmptyImpl[tasks.Task],
) *persistencespb.Predicate {
	return &persistencespb.Predicate{
		PredicateType: enumsspb.PREDICATE_TYPE_EMPTY,
		Attributes:    &persistencespb.Predicate_EmptyPredicateAttributes{},
	}
}

func FromPersistenceEmptyPredicate(
	_ *persistencespb.EmptyPredicateAttributes,
) tasks.Predicate {
	return predicates.Empty[tasks.Task]()
}

func ToPersistenceAndPredicate(
	andPredicate *predicates.AndImpl[tasks.Task],
) *persistencespb.Predicate {
	persistencePredicates := make([]*persistencespb.Predicate, 0, len(andPredicate.Predicates))
	for _, p := range andPredicate.Predicates {
		persistencePredicates = append(persistencePredicates, ToPersistencePredicate(p))
	}

	return &persistencespb.Predicate{
		PredicateType: enumsspb.PREDICATE_TYPE_AND,
		Attributes: &persistencespb.Predicate_AndPredicateAttributes{
			AndPredicateAttributes: &persistencespb.AndPredicateAttributes{
				Predicates: persistencePredicates,
			},
		},
	}
}

func FromPersistenceAndPredicate(
	attributes *persistencespb.AndPredicateAttributes,
) tasks.Predicate {
	taskPredicates := make([]predicates.Predicate[tasks.Task], 0, len(attributes.Predicates))
	for _, p := range attributes.Predicates {
		taskPredicates = append(taskPredicates, FromPersistencePredicate(p))
	}

	return predicates.And(taskPredicates...)
}

func ToPersistenceOrPredicate(
	orPredicate *predicates.OrImpl[tasks.Task],
) *persistencespb.Predicate {
	persistencePredicates := make([]*persistencespb.Predicate, 0, len(orPredicate.Predicates))
	for _, p := range orPredicate.Predicates {
		persistencePredicates = append(persistencePredicates, ToPersistencePredicate(p))
	}

	return &persistencespb.Predicate{
		PredicateType: enumsspb.PREDICATE_TYPE_OR,
		Attributes: &persistencespb.Predicate_OrPredicateAttributes{
			OrPredicateAttributes: &persistencespb.OrPredicateAttributes{
				Predicates: persistencePredicates,
			},
		},
	}
}

func FromPersistenceOrPredicate(
	attributes *persistencespb.OrPredicateAttributes,
) tasks.Predicate {
	taskPredicates := make([]predicates.Predicate[tasks.Task], 0, len(attributes.Predicates))
	for _, p := range attributes.Predicates {
		taskPredicates = append(taskPredicates, FromPersistencePredicate(p))
	}

	return predicates.Or(taskPredicates...)
}

func ToPersistenceNotPredicate(
	notPredicate *predicates.NotImpl[tasks.Task],
) *persistencespb.Predicate {
	return &persistencespb.Predicate{
		PredicateType: enumsspb.PREDICATE_TYPE_NOT,
		Attributes: &persistencespb.Predicate_NotPredicateAttributes{
			NotPredicateAttributes: &persistencespb.NotPredicateAttributes{
				Predicate: ToPersistencePredicate(notPredicate.Predicate),
			},
		},
	}
}

func FromPersistenceNotPredicate(
	attributes *persistencespb.NotPredicateAttributes,
) tasks.Predicate {
	return predicates.Not(FromPersistencePredicate(attributes.Predicate))
}

func ToPersistenceNamespaceIDPredicate(
	namespaceIDPredicate *tasks.NamespacePredicate,
) *persistencespb.Predicate {
	namespaceIDs := make([]string, 0, len(namespaceIDPredicate.NamespaceIDs))
	for namespaceID := range namespaceIDPredicate.NamespaceIDs {
		namespaceIDs = append(namespaceIDs, namespaceID)
	}

	return &persistencespb.Predicate{
		PredicateType: enumsspb.PREDICATE_TYPE_NAMESPACEID,
		Attributes: &persistencespb.Predicate_NamespaceIdPredicateAttributes{
			NamespaceIdPredicateAttributes: &persistencespb.NamespaceIdPredicateAttributes{
				NamespaceIds: namespaceIDs,
			},
		},
	}
}

func FromPersistenceNamespaceIDPredicate(
	attributes *persistencespb.NamespaceIdPredicateAttributes,
) tasks.Predicate {
	return tasks.NewNamespacePredicate(attributes.NamespaceIds)
}

func ToPersistenceTaskTypePredicate(
	taskTypePredicate *tasks.TypePredicate,
) *persistencespb.Predicate {
	taskTypes := make([]enumsspb.TaskType, 0, len(taskTypePredicate.Types))
	for taskType := range taskTypePredicate.Types {
		taskTypes = append(taskTypes, taskType)
	}

	return &persistencespb.Predicate{
		PredicateType: enumsspb.PREDICATE_TYPE_TASKTYPE,
		Attributes: &persistencespb.Predicate_TaskTypePredicateAttributes{
			TaskTypePredicateAttributes: &persistencespb.TaskTypePredicateAttributes{
				TaskTypes: taskTypes,
			},
		},
	}
}

func FromPersistenceTaskTypePredicate(
	attributes *persistencespb.TaskTypePredicateAttributes,
) tasks.Predicate {
	return tasks.NewTypePredicate(attributes.TaskTypes)
}

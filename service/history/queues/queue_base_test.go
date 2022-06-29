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
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/predicates"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
)

type (
	processorBaseSuite struct {
		suite.Suite
		*require.Assertions

		controller      *gomock.Controller
		mockScheduler   *MockScheduler
		mockRescheduler *MockRescheduler

		config         *configs.Config
		options        *QueueOptions
		logger         log.Logger
		metricsHandler metrics.MetricsHandler
	}
)

func TestProcessorBaseSuite(t *testing.T) {
	s := new(processorBaseSuite)
	suite.Run(t, s)
}

func (s *processorBaseSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockScheduler = NewMockScheduler(s.controller)
	s.mockRescheduler = NewMockRescheduler(s.controller)

	s.config = tests.NewDynamicConfig()
	s.options = &QueueOptions{
		ReaderOptions: ReaderOptions{
			BatchSize:                            dynamicconfig.GetIntPropertyFn(10),
			ShrinkRangeInterval:                  dynamicconfig.GetDurationPropertyFn(100 * time.Millisecond),
			ShrinkRangeIntervalJitterCoefficient: dynamicconfig.GetFloatPropertyFn(0.15),
			MaxReschdulerSize:                    dynamicconfig.GetIntPropertyFn(100),
			PollBackoffInterval:                  dynamicconfig.GetDurationPropertyFn(200 * time.Millisecond),
		},
		MaxPollInterval:                  dynamicconfig.GetDurationPropertyFn(time.Minute * 5),
		MaxPollIntervalJitterCoefficient: dynamicconfig.GetFloatPropertyFn(0.15),
		CompleteTaskInterval:             dynamicconfig.GetDurationPropertyFn(100 * time.Millisecond),
		TaskMaxRetryCount:                dynamicconfig.GetIntPropertyFn(100),
	}
	s.logger = log.NewTestLogger()
	s.metricsHandler = metrics.NoopMetricsHandler

}

func (s *processorBaseSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *processorBaseSuite) TestNewProcessBase_NoPreviousState() {
	ackLevel := int64(1024)
	rangeID := int64(10)

	mockShard := shard.NewTestContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId: 0,
				RangeId: rangeID,
				QueueAckLevels: map[int32]*persistencespb.QueueAckLevel{
					tasks.CategoryIDTransfer: {
						AckLevel: ackLevel,
					},
				},
			},
		},
		s.config,
	)

	base := newQueueBase(
		mockShard,
		tasks.CategoryTransfer,
		nil,
		s.mockScheduler,
		nil,
		s.options,
		s.logger,
		s.metricsHandler,
	)

	s.Len(base.readers, 1)
	defaultReader := base.readers[defaultReaderId]
	readerScopes := defaultReader.Scopes()
	s.Len(readerScopes, 1)
	s.Equal(ackLevel, readerScopes[0].Range.InclusiveMin.TaskID)
	s.Equal(rangeID<<s.config.RangeSizeBits, readerScopes[0].Range.ExclusiveMax.TaskID)
	s.True(predicates.Universal[tasks.Task]().Equals(readerScopes[0].Predicate))
}

func (s *processorBaseSuite) TestNewProcessBase_WithPreviousState() {
	persistenceState := &persistencespb.QueueState{
		ReaderStates: map[int32]*persistencespb.QueueReaderState{
			defaultReaderId: {
				Scopes: []*persistencespb.QueueSliceScope{
					{
						Range: &persistencespb.QueueSliceRange{
							InclusiveMin: &persistencespb.TaskKey{FireTime: timestamp.TimePtr(tasks.DefaultFireTime), TaskId: 1000},
							ExclusiveMax: &persistencespb.TaskKey{FireTime: timestamp.TimePtr(tasks.DefaultFireTime), TaskId: 2000},
						},
						Predicate: &persistencespb.Predicate{
							PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
							Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
						},
					},
					{
						Range: &persistencespb.QueueSliceRange{
							InclusiveMin: &persistencespb.TaskKey{FireTime: timestamp.TimePtr(tasks.DefaultFireTime), TaskId: 2000},
							ExclusiveMax: &persistencespb.TaskKey{FireTime: timestamp.TimePtr(tasks.DefaultFireTime), TaskId: 3000},
						},
						Predicate: &persistencespb.Predicate{
							PredicateType: enumsspb.PREDICATE_TYPE_TASK_TYPE,
							Attributes: &persistencespb.Predicate_TaskTypePredicateAttributes{
								TaskTypePredicateAttributes: &persistencespb.TaskTypePredicateAttributes{
									TaskTypes: []enumsspb.TaskType{enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER},
								},
							},
						},
					},
				},
			},
			1: {
				Scopes: []*persistencespb.QueueSliceScope{
					{
						Range: &persistencespb.QueueSliceRange{
							InclusiveMin: &persistencespb.TaskKey{FireTime: timestamp.TimePtr(tasks.DefaultFireTime), TaskId: 2000},
							ExclusiveMax: &persistencespb.TaskKey{FireTime: timestamp.TimePtr(tasks.DefaultFireTime), TaskId: 3000},
						},
						Predicate: &persistencespb.Predicate{
							PredicateType: enumsspb.PREDICATE_TYPE_NAMESPACE_ID,
							Attributes: &persistencespb.Predicate_NamespaceIdPredicateAttributes{
								NamespaceIdPredicateAttributes: &persistencespb.NamespaceIdPredicateAttributes{
									NamespaceIds: []string{uuid.New()},
								},
							},
						},
					},
				},
			},
		},
		ExclusiveReaderHighWatermark: &persistencespb.TaskKey{FireTime: timestamp.TimePtr(tasks.DefaultFireTime), TaskId: 4000},
	}

	mockShard := shard.NewTestContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId: 0,
				RangeId: 10,
				QueueStates: map[int32]*persistencespb.QueueState{
					tasks.CategoryIDTransfer: persistenceState,
				},
			},
		},
		s.config,
	)

	base := newQueueBase(
		mockShard,
		tasks.CategoryTransfer,
		nil,
		s.mockScheduler,
		nil,
		s.options,
		s.logger,
		s.metricsHandler,
	)

	readerScopes := make(map[int32][]Scope)
	for id, reader := range base.readers {
		readerScopes[id] = reader.Scopes()
	}
	queueState := &queueState{
		readerScopes:        readerScopes,
		exclusiveMaxReadKey: base.nonReadableRange.InclusiveMin,
	}

	s.Equal(persistenceState, ToPersistenceQueueState(queueState))
}

func (s *processorBaseSuite) TestStartStop() {
	mockShard := shard.NewTestContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId: 0,
				RangeId: 10,
				QueueAckLevels: map[int32]*persistencespb.QueueAckLevel{
					tasks.CategoryIDTransfer: {
						AckLevel: 1024,
					},
				},
			},
		},
		s.config,
	)

	paginationFnProvider := func(paginationRange Range) collection.PaginationFn[tasks.Task] {
		return func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			mockTask := tasks.NewMockTask(s.controller)
			key := NewRandomKeyInRange(paginationRange)
			mockTask.EXPECT().GetKey().Return(key).AnyTimes()
			mockTask.EXPECT().GetVisibilityTime().Return(key.FireTime).AnyTimes() // TODO: remove this
			return []tasks.Task{mockTask}, nil, nil
		}
	}

	doneCh := make(chan struct{})
	s.mockScheduler.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(_ Executable) (bool, error) {
		close(doneCh)
		return true, nil
	}).Times(1)
	s.mockRescheduler.EXPECT().Len().Return(0).AnyTimes()

	base := newQueueBase(
		mockShard,
		tasks.CategoryTransfer,
		paginationFnProvider,
		s.mockScheduler,
		nil,
		s.options,
		s.logger,
		s.metricsHandler,
	)
	base.rescheduler = s.mockRescheduler // replace with mock to verify Start/Stop

	s.mockRescheduler.EXPECT().Start().Times(1)
	base.Start()
	<-doneCh
	<-base.completeTaskTimer.C

	s.mockRescheduler.EXPECT().Stop().Times(1)
	base.Stop()
	s.False(base.completeTaskTimer.Stop())
}

func (s *processorBaseSuite) TestProcessNewRange() {
	queueState := &queueState{
		readerScopes: map[int32][]Scope{
			defaultReaderId: {},
		},
		exclusiveMaxReadKey: tasks.MinimumKey,
	}

	persistenceState := ToPersistenceQueueState(queueState)

	mockShard := shard.NewTestContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId: 0,
				RangeId: 10,
				QueueStates: map[int32]*persistencespb.QueueState{
					tasks.CategoryIDTimer: persistenceState,
				},
			},
		},
		s.config,
	)

	base := newQueueBase(
		mockShard,
		tasks.CategoryTimer,
		nil,
		s.mockScheduler,
		nil,
		s.options,
		s.logger,
		s.metricsHandler,
	)
	s.True(base.nonReadableRange.Equals(NewRange(tasks.MinimumKey, tasks.MaximumKey)))

	base.processNewRange()
	scopes := base.readers[defaultReaderId].Scopes()
	s.Len(scopes, 1)
	s.True(scopes[0].Range.InclusiveMin.CompareTo(tasks.MinimumKey) == 0)
	s.True(scopes[0].Predicate.Equals(predicates.Universal[tasks.Task]()))
	s.True(time.Since(scopes[0].Range.ExclusiveMax.FireTime) <= time.Second)
	s.True(base.nonReadableRange.Equals(NewRange(scopes[0].Range.ExclusiveMax, tasks.MaximumKey)))
}

func (s *processorBaseSuite) TestCompleteTaskAndPersistState() {
	minKey := tasks.MaximumKey
	readerScopes := map[int32][]Scope{}
	for _, readerID := range []int32{defaultReaderId, 2, 3} {
		scopes := NewRandomScopes(10)
		readerScopes[readerID] = scopes
		if len(scopes) != 0 {
			minKey = tasks.MinKey(minKey, scopes[0].Range.InclusiveMin)
		}
	}
	queueState := &queueState{
		readerScopes: map[int32][]Scope{
			defaultReaderId: {},
		},
		exclusiveMaxReadKey: tasks.MaximumKey,
	}

	persistenceState := ToPersistenceQueueState(queueState)

	mockShard := shard.NewTestContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId: 0,
				RangeId: 10,
				QueueStates: map[int32]*persistencespb.QueueState{
					tasks.CategoryIDTimer: persistenceState,
				},
			},
		},
		s.config,
	)

	base := newQueueBase(
		mockShard,
		tasks.CategoryTimer,
		nil,
		s.mockScheduler,
		nil,
		s.options,
		s.logger,
		s.metricsHandler,
	)
	base.completeTaskTimer = time.NewTimer(s.options.CompleteTaskInterval())

	s.Equal(minKey.FireTime.UTC(), base.exclusiveCompletedTaskKey.FireTime)
	base.exclusiveCompletedTaskKey = tasks.MinimumKey // set to a smaller value to that delete will be triggered

	mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	mockShard.Resource.ClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()

	gomock.InOrder(
		mockShard.Resource.ExecutionMgr.EXPECT().RangeCompleteHistoryTasks(gomock.Any(), &persistence.RangeCompleteHistoryTasksRequest{
			ShardID:             0,
			TaskCategory:        tasks.CategoryTimer,
			InclusiveMinTaskKey: tasks.MinimumKey,
			ExclusiveMaxTaskKey: tasks.NewKey(minKey.FireTime.UTC(), 0),
		}).Return(nil),
		mockShard.Resource.ShardMgr.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, request *persistence.UpdateShardRequest) error {
				s.Equal(persistenceState, request.ShardInfo.QueueStates[tasks.CategoryIDTimer])
				return nil
			},
		),
	)

	base.completeTaskAndPersistState()

	s.Equal(minKey.FireTime.UTC(), base.exclusiveCompletedTaskKey.FireTime)
}

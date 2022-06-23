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
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/predicates"
	"go.temporal.io/server/service/history/tasks"
)

type (
	readerSuite struct {
		suite.Suite
		*require.Assertions

		controller      *gomock.Controller
		mockScheduler   *MockScheduler
		mockRescheduler *MockRescheduler

		logger                log.Logger
		metricsProvider       metrics.MetricProvider
		executableInitializer ExecutableInitializer
	}
)

func TestReaderSuite(t *testing.T) {
	s := new(readerSuite)
	suite.Run(t, s)
}

func (s *readerSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockScheduler = NewMockScheduler(s.controller)
	s.mockRescheduler = NewMockRescheduler(s.controller)

	s.logger = log.NewTestLogger()
	s.metricsProvider = metrics.NoopMetricProvider

	s.executableInitializer = func(t tasks.Task) Executable {
		return NewMockExecutable(s.controller)
	}
}

func (s *readerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *readerSuite) TestStartLoadStop() {
	r := NewRandomRange()
	scopes := []Scope{NewScope(r, predicates.All[tasks.Task]())}

	paginationFnProvider := func(paginationRange Range) collection.PaginationFn[tasks.Task] {
		s.Equal(r, paginationRange)
		return func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			mockTask := tasks.NewMockTask(s.controller)
			mockTask.EXPECT().GetKey().Return(NewRandomKeyInRange(r)).AnyTimes()
			return []tasks.Task{mockTask}, nil, nil
		}
	}

	doneCh := make(chan struct{})
	s.mockScheduler.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(_ Executable) (bool, error) {
		close(doneCh)
		return true, nil
	}).Times(1)
	s.mockRescheduler.EXPECT().Len().Return(0).AnyTimes()

	reader := s.newTestReader(scopes, paginationFnProvider)
	reader.Start()
	<-doneCh
	reader.Stop()
}

func (s *readerSuite) TestScopes() {
	scopes := s.newRandomScopes(10)

	reader := s.newTestReader(scopes, nil)
	actualScopes := reader.Scopes()
	for idx, expectedScope := range scopes {
		s.True(expectedScope.Equals(actualScopes[idx]))
	}
}

func (s *readerSuite) TestShrinkRanges() {
	numScopes := 10
	scopes := s.newRandomScopes(numScopes)

	// manually set some scopes to be empty
	emptyIdx := map[int]struct{}{0: {}, 2: {}, 5: {}, 9: {}}
	for idx := range emptyIdx {
		scopes[idx].Range.InclusiveMin = scopes[idx].Range.ExclusiveMax
	}

	reader := s.newTestReader(scopes, nil)
	reader.shrinkRanges()

	actualScopes := reader.Scopes()
	s.Len(actualScopes, numScopes-len(emptyIdx))

	expectedScopes := make([]Scope, 0, numScopes-len(emptyIdx))
	for idx, scope := range scopes {
		if _, ok := emptyIdx[idx]; !ok {
			expectedScopes = append(expectedScopes, scope)
		}
	}

	for idx, expectedScope := range expectedScopes {
		s.True(expectedScope.Equals(actualScopes[idx]))
	}
}

func (s *readerSuite) TestSubmitTask() {
	r := NewRandomRange()
	scopes := []Scope{NewScope(r, predicates.All[tasks.Task]())}
	reader := s.newTestReader(scopes, nil)

	mockExecutable := NewMockExecutable(s.controller)

	s.mockScheduler.EXPECT().TrySubmit(gomock.Any()).Return(true, nil).Times(1)
	reader.submit(mockExecutable)

	s.mockScheduler.EXPECT().TrySubmit(gomock.Any()).Return(false, nil).Times(1)
	mockExecutable.EXPECT().Reschedule().Times(1)
	reader.submit(mockExecutable)

	s.mockScheduler.EXPECT().TrySubmit(gomock.Any()).Return(false, errors.New("some random error")).Times(1)
	mockExecutable.EXPECT().Reschedule().Times(1)
	reader.submit(mockExecutable)
}

func (s *readerSuite) newRandomScopes(
	numScopes int,
) []Scope {
	ranges := NewRandomOrderedRangesInRange(
		NewRandomRange(),
		numScopes,
	)

	scopes := make([]Scope, 0, 10)
	for _, r := range ranges {
		scopes = append(scopes, NewScope(r, predicates.All[tasks.Task]()))
	}

	return scopes
}

func (s *readerSuite) newTestReader(
	scopes []Scope,
	paginationFnProvider PaginationFnProvider,
) *ReaderImpl {
	return NewReader(
		paginationFnProvider,
		s.executableInitializer,
		scopes,
		&ReaderOptions{
			BatchSize:                            dynamicconfig.GetIntPropertyFn(100),
			ShrinkRangeInterval:                  dynamicconfig.GetDurationPropertyFn(100 * time.Millisecond),
			ShrinkRangeIntervalJitterCoefficient: dynamicconfig.GetFloatPropertyFn(0.15),
			MaxReschdulerSize:                    dynamicconfig.GetIntPropertyFn(100),
			PollBackoffInterval:                  dynamicconfig.GetDurationPropertyFn(200 * time.Millisecond),
		},
		s.mockScheduler,
		s.mockRescheduler,
		s.logger,
		s.metricsProvider,
	)
}

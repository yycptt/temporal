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

package predicates

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

var _ Predicate[int] = (*testPredicate)(nil)

type (
	predicatesSuite struct {
		suite.Suite
		*require.Assertions
	}

	testPredicate struct {
		nums map[int]struct{}
	}
)

func TestPredicateSuite(t *testing.T) {
	s := new(predicatesSuite)
	suite.Run(t, s)
}

func (s *predicatesSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *predicatesSuite) TestAnd_Normal() {
	p1 := newTestPredicate(1, 2, 6)
	p2 := NewAnd[int](
		newTestPredicate(3, 4, 6),
		newTestPredicate(4, 5, 6),
	)
	p := NewAnd[int](p1, p2)

	for i := 1; i != 6; i++ {
		s.False(p.Test(i))
	}
	s.True(p.Test(6))
}

func (s *predicatesSuite) TestAnd_All() {
	p := NewAnd[int](
		newTestPredicate(1, 2, 3),
		NewAll[int](),
	)

	for i := 1; i != 4; i++ {
		s.True(p.Test(i))
	}
	for i := 4; i != 7; i++ {
		s.False(p.Test(i))
	}

	p = NewAnd(
		NewAll[int](),
		NewAll[int](),
	)
	for i := 1; i != 7; i++ {
		s.True(p.Test(i))
	}
}

func (s *predicatesSuite) TestAnd_None() {
	p := NewAnd[int](
		newTestPredicate(1, 2, 3),
		NewNone[int](),
	)

	for i := 1; i != 7; i++ {
		s.False(p.Test(i))
	}
}

func (s *predicatesSuite) TestOr_Normal() {
	p1 := newTestPredicate(1, 2, 6)
	p2 := NewOr[int](
		newTestPredicate(3, 4, 6),
		newTestPredicate(4, 5, 6),
	)
	p := NewOr[int](p1, p2)

	for i := 1; i != 7; i++ {
		s.True(p.Test(i))
	}
	s.False(p.Test(7))
}

func (s *predicatesSuite) TestOr_All() {
	p := NewOr[int](
		newTestPredicate(1, 2, 3),
		NewAll[int](),
	)

	for i := 1; i != 7; i++ {
		s.True(p.Test(i))
	}
}

func (s *predicatesSuite) TestOr_None() {
	p := NewOr[int](
		newTestPredicate(1, 2, 3),
		NewNone[int](),
	)

	for i := 1; i != 4; i++ {
		s.True(p.Test(i))
	}
	for i := 4; i != 7; i++ {
		s.False(p.Test(i))
	}

	p = NewOr(
		NewNone[int](),
		NewNone[int](),
	)
	for i := 1; i != 7; i++ {
		s.False(p.Test(i))
	}
}

func (s *predicatesSuite) TestNot() {
	p1 := newTestPredicate(1, 2, 3)
	p := NewNot[int](p1)

	for i := 1; i != 4; i++ {
		s.False(p.Test(i))
	}
	for i := 4; i != 7; i++ {
		s.True(p.Test(i))
	}

	p = NewNot(p)
	for i := 1; i != 4; i++ {
		s.True(p.Test(i))
	}
	for i := 4; i != 7; i++ {
		s.False(p.Test(i))
	}

	p = NewNot(NewAll[int]())
	for i := 1; i != 7; i++ {
		s.False(p.Test(i))
	}

	p = NewNot(NewNone[int]())
	for i := 1; i != 7; i++ {
		s.True(p.Test(i))
	}
}

func (s *predicatesSuite) TestAll() {
	p := NewAll[int]()

	for i := 1; i != 10; i++ {
		s.True(p.Test(i))
	}
}

func (s *predicatesSuite) TestNone() {
	p := NewNone[int]()

	for i := 1; i != 10; i++ {
		s.False(p.Test(i))
	}
}

func newTestPredicate(nums ...int) *testPredicate {
	numsMap := make(map[int]struct{}, len(nums))
	for _, x := range nums {
		numsMap[x] = struct{}{}
	}
	return &testPredicate{
		nums: numsMap,
	}
}

func (p *testPredicate) Test(x int) bool {
	_, ok := p.nums[x]
	return ok
}

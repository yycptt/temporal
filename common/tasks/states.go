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
	"fmt"
	"strconv"
)

// State represents the current state of a task
type State int

const (
	// TaskStateLoaded is the state for a task when it has been loaded into memory
	TaskStateLoaded State = iota + 1
	// TaskStateSubmitted is the state for a task when it's waiting to be processed or currently being processed
	TaskStateSubmitted
	// TaskStateBackoff is the state for a task when it's rescheduled and in retry backoff duration
	TaskStateBackoff
	// TaskStateAcked is the state for a task if it has been successfully completed
	TaskStateAcked
	// TaskStateNacked is the state for a task if it can not be processed
	TaskStateNacked
)

var (
	StateName = map[State]string{
		TaskStateLoaded:    "loaded",
		TaskStateSubmitted: "submitted",
		TaskStateBackoff:   "backoff",
		TaskStateAcked:     "acked",
		TaskStateNacked:    "nacked",
	}
)

func (s *State) Transit(nextState State) {
	switch *s {
	case TaskStateLoaded:
		switch nextState {
		case TaskStateSubmitted:
			*s = nextState
			return
		}
	case TaskStateSubmitted:
		switch nextState {
		case TaskStateBackoff, TaskStateAcked, TaskStateNacked:
			*s = nextState
			return
		}
	case TaskStateBackoff:
		switch nextState {
		case TaskStateLoaded:
			*s = nextState
			return
		}
	case TaskStateAcked, TaskStateNacked:
		// no-op
	default:
		panic(fmt.Sprintf("unknown task state: %v", s))
	}

	panic(fmt.Sprintf("can not transit task state from %v to %v", *s, nextState))
}

func (s State) String() string {
	str, ok := StateName[s]
	if ok {
		return str
	}
	return strconv.Itoa(int(s))
}

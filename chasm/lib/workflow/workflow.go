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

package workflow

import (
	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/payload"
)

var _ chasm.Component = (*Workflow)(nil)

type Workflow struct {
	chasm.UnimplementedComponent

	Data *persistencespb.Workflow

	Memo chasm.Field[*commonpb.Memo]
	// Memo chasm.Collection[*commonpb.Payload]
}

func New(
	mutableContext chasm.MutableContext,
) *Workflow {
	return &Workflow{
		Data: &persistencespb.Workflow{},
	}
}

func (w *Workflow) LifecycleState() chasm.LifecycleState {
	panic("not implemented")
}

func (w *Workflow) UpsertMemo(
	mutableContext chasm.MutableContext,
	updatedMemo *commonpb.Memo,
) error {
	memoValue, err := w.Memo.Get(mutableContext)
	if err != nil {
		return err
	}

	if memoValue == nil {
		w.Memo = *chasm.NewDataField(mutableContext, common.CloneProto(updatedMemo))
		return nil
	}

	memoValue.Fields = payload.MergeMapOfPayload(
		updatedMemo.Fields,
		memoValue.Fields,
	)

	return nil
}

func (w *Workflow) GetMemo(
	context chasm.Context,
) (*commonpb.Memo, error) {
	return w.Memo.Get(context)
}

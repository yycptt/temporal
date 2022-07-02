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
	"sync"
	"sync/atomic"

	"go.temporal.io/server/common"
	"golang.org/x/exp/maps"
)

type (
	readerInitializer func(readerID int32, scopes []Scope) Reader

	readerGroup struct {
		sync.Mutex

		initializer readerInitializer

		status    int32
		readerMap map[int32]Reader
	}
)

func newReaderGroup(
	initializer readerInitializer,
) *readerGroup {
	return &readerGroup{
		initializer: initializer,
		status:      common.DaemonStatusInitialized,
		readerMap:   make(map[int32]Reader),
	}
}

func (g *readerGroup) start() {
	if !atomic.CompareAndSwapInt32(&g.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	g.Lock()
	defer g.Unlock()

	for _, reader := range g.readerMap {
		reader.Start()
	}
}

func (g *readerGroup) stop() {
	if !atomic.CompareAndSwapInt32(&g.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	g.Lock()
	defer g.Unlock()

	for _, reader := range g.readerMap {
		reader.Stop()
	}
}

func (g *readerGroup) readers() map[int32]Reader {
	g.Lock()
	defer g.Unlock()

	readerMapCopy := make(map[int32]Reader, len(g.readerMap))
	maps.Copy(readerMapCopy, g.readerMap)

	return readerMapCopy
}

func (g *readerGroup) readerByID(readerID int32) (Reader, bool) {
	g.Lock()
	defer g.Unlock()

	reader, ok := g.readerMap[readerID]
	return reader, ok
}

func (g *readerGroup) newReader(readerID int32, scopes []Scope) Reader {
	reader := g.initializer(readerID, scopes)

	g.Lock()
	defer g.Unlock()

	if _, ok := g.readerMap[readerID]; ok {
		panic(fmt.Sprintf("reader with ID %v already exists", readerID))
	}

	g.readerMap[readerID] = reader
	if !g.isStopped() {
		reader.Start()
	}
	return reader
}

func (g *readerGroup) isStopped() bool {
	return atomic.LoadInt32(&g.status) == common.DaemonStatusStopped
}

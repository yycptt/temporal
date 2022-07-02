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
	"sync"
)

var _ Mitigator = (*mitigatorImpl)(nil)

type (
	Mitigator interface {
		Alert(Alert) bool
	}

	mitigatorImpl struct {
		sync.Mutex

		monitor Monitor

		pendingAlerts map[AlertType]Alert
		actionCh      chan<- action
	}
)

func newMitigator(
	monitor Monitor,
) (*mitigatorImpl, <-chan action) {
	actionCh := make(chan action, 10)

	return &mitigatorImpl{
		monitor:       monitor,
		pendingAlerts: make(map[AlertType]Alert),
		actionCh:      actionCh,
	}, actionCh
}

func (m *mitigatorImpl) Alert(alert Alert) bool {
	m.Lock()

	if _, ok := m.pendingAlerts[alert.AlertType]; ok {
		m.Unlock()
		return false
	}

	m.pendingAlerts[alert.AlertType] = alert
	m.Unlock()

	// handle alert here

	return true
}

func (m *mitigatorImpl) resolve(alertType AlertType) {
	m.Lock()
	defer m.Unlock()

	delete(m.pendingAlerts, alertType)
}

func (m *mitigatorImpl) close() {
	close(m.actionCh)
}

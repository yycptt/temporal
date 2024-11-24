package chasm

import (
	"context"
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
)

// 2. History event as a append only, immutable ordered storaged model
// 3. parent notify/intercept child component

// Open questions:
// 2. event notification?
// 3. history storage model?
// 4. CompletedActivityByID??? (this is obviously an API on workflow, not Activity)

type InstanceKey struct {
	NamespaceID string
	BusinessID  string
	InstanceID  string
}

type Context interface {
	// TODO: don't provide any methods here, make all of them
	// chasm.XXX(Context, Component, ...)

	// NOTE: component created in the current transaction won't have a ref

	Ref() (ComponentRef, bool)
	Now() time.Time

	getContext() context.Context
}

type MutableContext interface {
	Context

	AddTask(t Task) error
	WithTaskWrapper(wrapper TaskWrapper) MutableContext
}

type TaskWrapper func(task Task) Task

type Component interface {
	RunningState() ComponentState
}

type ComponentState int

const (
	ComponentStateRunning ComponentState = 1 << iota
	ComponentStatePaused
	ComponentStateCompleted
	ComponentStateFailed
	ComponentStateReset
)

type DataField[D any] struct {
}

func NewDataField[D any](
	ctx Context,
	d D,
) *DataField[D] {
	return &DataField[D]{}
}

func (d *DataField[D]) Get(Context) (D, error) {
	panic("not implemented")
}

type DataMap[C any] map[string]*DataField[C]

type ComponentRef struct {
	Key InstanceKey

	instanceTypeName     string
	instanceLastUpdateVT *persistencespb.VersionedTransition // this is consistency token
}

func (r *ComponentRef) Serialize() ([]byte, error) {
	panic("not implemented")
}

func DeserializeComponentRef(data []byte) (ComponentRef, error) {
	panic("not implemented")
}

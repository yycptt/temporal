package asm

import (
	"context"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
)

type (
	ASM interface {
		Type() string
		Options() []Option

		TaskHandlers() map[string]TaskHandler
		EventHandlers() map[string]HistoryEventHanlder

		New(InstanceBackend) Instance

		SerializeInstance(Instance) (*commonpb.DataBlob, error)
		DeserializeInstance(InstanceBackend, *commonpb.DataBlob) (Instance, error)

		Interceptors() map[any]Interceptor     // child interface value ptr -> Interceptor
		EventListeners() map[any]EventListener // child event value ptr -> listener

		// optional: only for top level ASM
		RoutingKey(Key) string
	}

	Instance interface {
		Type() string

		// optional: only needed for non-ephemeral, top level ASM
		State() enumsspb.WorkflowExecutionState
		Terminate() error // this is for replication
	}
)

type (
	InstanceBackend interface {
		InstanceRef() InstanceRef

		AddTasks(...Task) error

		AddHistoryEvents(...HistoryEventAttributes) []HistoryEvent
		LoadHistoryEvent(HistoryEventToken) (HistoryEvent, error)
		LoadHistoryEvents(HistoryEventToken, HistoryEventToken) HistoryEventIterator

		EmitEvents(...any)

		NewChildASM(
			id string,
			childInstance any, // value ptr
		) error
		UpdateChildASM(
			id string,
			childInstance any, // value ptr
		) error
		ReadChildASM(
			id string,
			childInstance any, // value ptr
		) error
		DeleteChildASM(id string) error

		Now() time.Time
	}
)

type (
	Interceptor func(Instance) error

	EventListener func(Instance, any) error
)

/////////////// Option //////////////////////

type (
	Option int
)

const (
	OptionUnspecified Option = iota
	OptionEphemeral
	OptionSingleCluster
)

/////////////// Task //////////////////////

type (
	TaskProperties struct {
		ScheduledTime time.Time
		Destination   string
	}

	Task interface {
		TaskType() string
		Properties() TaskProperties
	}

	TaskHandler interface {
		Validate(Task, Instance) error
		Execute(context.Context, Task, InstanceRef, Engine) error
		SerializeTask(Task) (*commonpb.DataBlob, error)
		DeserializeTask(TaskProperties, *commonpb.DataBlob) (Task, error)
	}
)

/////////////// History Event //////////////////////

type (
	// considerations:
	// 1. deserializer should simple. If Event also contains apply logic then
	// deserializer needs to depends on lots of other things. Making Task & Event
	// data only will make deserializer simple.
	// 2. Apply can be on the instance
	// 3. how to decided if reapply is needed?
	// based on the asm instance generated the event? no, the event may create that instance
	// based on it's parent?

	HistoryEventAttributes any

	// we can use a struct with private fields here
	// but most likely caller need to store/return this token and
	// then needs to worry about encoding/decoding
	// so using []byte here
	HistoryEventToken []byte

	HistoryEvent interface {
		ID() int64
		Time() time.Time
		Token() (HistoryEventToken, bool)
		Attributes() HistoryEventAttributes
	}

	HistoryEventHanlder interface {
		Apply(HistoryEvent, Instance) error
		// TODO: can an ASM decide if an event should be reapply or not on it's own?
		// the decision needs to be made at a higher level?
		// e.g. 1. need to dedup ASM 2. instance may not even exist right now
		Reapply(HistoryEvent, Instance) error

		SerializeHistoryEventAttributes(HistoryEventAttributes) (*commonpb.DataBlob, error)
		DeserializeHistoryEventAttributes(*commonpb.DataBlob) (HistoryEventAttributes, error)
	}

	HistoryEventIterator interface {
		Next() (HistoryEvent, error)
		HasNext() bool
	}
)

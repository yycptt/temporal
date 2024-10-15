package asm

import "context"

type (
	ConsistenceToken []byte

	TransitionFn func(Instance) error

	ReadFn func(Instance) error

	NewASMRequest struct {
		ASMType      string
		Key          Key
		TransitionFn TransitionFn
	}

	NewASMResponse struct {
		Ref InstanceRef
	}

	UpdateWithNewRequest struct {
		ASMType string
		Key     Key
		// ChildKeys          []string
		NewTransitionFn    TransitionFn
		UpdateTransitionFn TransitionFn
	}

	UpdateWithNewResponse struct {
		Ref InstanceRef
	}

	UpdateRequest struct {
		Ref InstanceRef
		// ChildKeys    []string
		TransitionFn TransitionFn
	}

	UpdateResponse struct {
		Ref InstanceRef
	}

	ReadRequest struct {
		Ref InstanceRef
		// ChildKeys []string
		ReadFn ReadFn
	}

	ReadResponse struct{}

	PollRequest struct {
		Ref    InstanceRef
		Event  any // asm event value ptr
		ReadFn ReadFn
	}

	PollResponse struct{}

	Engine interface {
		New( // optimistic locking
			context.Context,
			NewASMRequest,
		) (NewASMResponse, error)
		UpdateWithNew( // pessimistic locking
			context.Context,
			UpdateWithNewRequest,
		) (UpdateWithNewResponse, error)
		Update(
			context.Context,
			UpdateRequest,
		) (UpdateResponse, error)
		Read(
			context.Context,
			ReadRequest,
		) (ReadResponse, error)
		Poll(
			context.Context,
			PollRequest,
		) (PollResponse, error)

		LoadHistoryEvent(
			context.Context,
			HistoryEventToken,
		) (HistoryEvent, error)
		LoadHistoryEvents(
			context.Context,
			HistoryEventToken,
			HistoryEventToken,
		) HistoryEventIterator

		// TODO: poll state/events
	}
)

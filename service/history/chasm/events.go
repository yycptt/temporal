package chasm

type EventListenerField[T any] struct {
}

func (e *EventListenerField[T]) Get(ctx Context) T {
	panic("not implemented")
}

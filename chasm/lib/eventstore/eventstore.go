package eventstore

import (
	"strconv"

	"go.temporal.io/server/chasm"

	"github.com/nexus-rpc/sdk-go/nexus"
)

type Library struct {
}

func (Library) Components() (comps []chasm.ComponentType) {
	comps = append(comps, chasm.NewComponentType[EmbeddedEventStore](chasm.ComponentTypeOptions{}))
	return
}

// Components implements chasm.Library.
func (Library) Tasks() (defs []chasm.TaskType) {
	return
}

func (Library) Services() (defs []*nexus.Service) {
	return
}

type Event interface {
	ID() int64
}

type EventStore interface {
	// TODO: Use tokens
	Add(ctx chasm.WriteContext, event Event)
	// TODO: Use tokens
	Get(ctx chasm.ReadContext, id int64) Event
}

type EmbeddedEventStore struct {
	State *struct{ Exclude []string }

	Events *chasm.Map[Event]
}

func (s EmbeddedEventStore) Add(ctx chasm.WriteContext, event Event) {
	s.Events.Set(strconv.FormatInt(event.ID(), 10), event)
}

func (s EmbeddedEventStore) Get(ctx chasm.ReadContext, id int64) Event {
	panic("todo")
}

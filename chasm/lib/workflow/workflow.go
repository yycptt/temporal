package workflow

import (
	"go.temporal.io/server/chasm"
)

const (
	// Archetype for today's workflow implementation.
	// This value is NOT persisted today, and ok to be changed.
	//
	// TODO: change this to the actual archetype name
	Archetype chasm.Archetype = "Workflow"
)

type Workflow struct {
	chasm.UnimplementedComponent

	State emptyProtoMessage

	// approach 2: special in-memory field for getting mutableState access
	// CHASM framework needs to populate this field.
	MSPointer chasm.MSPointer

	// This is like normal map, make sure you check for nil before using it.
	Callbacks chasm.Map[string, *CallbackComponent]
}

func NewWorkflow(
	mutableContext chasm.MutableContext,
) *Workflow {
	return &Workflow{}
}

func (w *Workflow) LifecycleState(
	_ chasm.Context,
) chasm.LifecycleState {
	// NOTE: closeTransactionHandleRootLifecycleChange() is bypassed in tree.go
	//
	// NOTE: detached mode is not implemented yet, so always return Running here.
	// Otherwise tasks for callback component can't be executed after workflow is closed.
	return chasm.LifecycleStateRunning
}

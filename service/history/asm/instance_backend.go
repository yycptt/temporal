package asm

import "strings"

type (
	instanceBackendImpl struct {
		asmRegistry  Registry
		mutableState MutableState
		basePath     string
	}
)

func newInstanceBackend(
	asmRegistry Registry,
	mutableState MutableState,
	basePath string,
) *instanceBackendImpl {
	return &instanceBackendImpl{
		asmRegistry:  asmRegistry,
		mutableState: mutableState,
		basePath:     basePath,
	}
}

func (i *instanceBackendImpl) CloseTransition(
	transitionOutput TransitionOutput,
) error {
	return i.mutableState.ApplyASMTransition(
		i.basePath,
		transitionOutput,
	)
}

func (i *instanceBackendImpl) NewChildASM(
	childASMType string,
	id string,
) (Instance, error) {
	asm, ok := i.asmRegistry.Get(childASMType)
	if !ok {
		return nil, ErrASMTypeNotRegistered
	}

	childPath := i.basePath + "/" + id
	childASMInstance := asm.New(newInstanceBackend(
		i.asmRegistry,
		i.mutableState,
		childPath,
	))

	if err := i.mutableState.AddASMInstance(childPath, childASMInstance); err != nil {
		return nil, err
	}
	return childASMInstance, nil
}

func (i *instanceBackendImpl) GetChildASM(
	id string,
) (Instance, error) {
	return i.mutableState.GetASMInstance(i.basePath+"/"+id, nil)
}

func (i *instanceBackendImpl) DeleteChildASM(
	id string,
) error {
	asmPathToRemove := map[string]struct{}{}
	msImpl := i.mutableState.(*MutableStateImpl)
	for path := range msImpl.pendingPersistedASMInfos {
		if strings.HasPrefix(path, id) {
			asmPathToRemove[path] = struct{}{}
		}
	}
	for path := range msImpl.pendingInMemoryASMInfos {
		if strings.HasPrefix(path, id) {
			asmPathToRemove[path] = struct{}{}
		}
	}

	for path := range asmPathToRemove {
		if err := i.mutableState.RemoveASM(path); err != nil {
			return err
		}
	}

	return nil
}

func (i *instanceBackendImpl) AddEvents(
	events ...HistoryEventAttributes,
) []HistoryEvent {
	return nil
}

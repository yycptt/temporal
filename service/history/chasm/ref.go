package chasm

import (
	"strings"

	persistencespb "go.temporal.io/server/api/persistence/v1"
)

type EntityKey struct {
	NamespaceID string
	BusinessID  string
	EntityID    string
}

type ComponentRef struct {
	EntityKey

	// This is needed for routing
	rootComponentName string
	// or maybe make it specific
	// routingKey string
	shardID int32

	// From the component name, we can find the component struct definition
	// use reflection to find sub-components and understand if those sub-components
	// needs to be loaded or not. we only need to do this for sub-components of the component.
	// path for parent/ancenstor component can be inferred from the path.
	// Not neede for v1 as this is for partial loading.
	// componentName      string

	// (EntityKey + ) path + initial VT uniquely identifies a component
	componentPath      componentPath
	componentInitialVT *persistencespb.VersionedTransition // this identifies a component

	// TODO: should not be exported
	EntityLastUpdateVT *persistencespb.VersionedTransition // this is consistency token

	// TODO: need a function ptr to the task validation logic
	// or maybe put that in to the context.
	validationFn func(Context, Component) error
}

// In V1, if you don't have a ref,
// then you can only interact with the top level entity.
func NewComponentRef(
	entityKey EntityKey,
	rootComponentName string,
) ComponentRef {
	return ComponentRef{
		EntityKey: entityKey,
		// we probably don't even need this,
		// can make the function generic and find the name from registry
		rootComponentName: rootComponentName,
	}
}

func (r *ComponentRef) Serialize() []byte {
	panic("not implemented")
}

func DeserializeComponentRef(data []byte) (ComponentRef, error) {
	panic("not implemented")
}

// we may need to export this later for partial loading
type componentPath []string

func (p componentPath) String() string {
	return strings.Join(p, "/")
}

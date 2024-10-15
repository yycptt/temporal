package asm

import persistencespb "go.temporal.io/server/api/persistence/v1"

type (
	InstanceRef struct {
		key                     Key
		topInstanceLastUpdateVT *persistencespb.VersionedTransition
		topInstanceType         string

		instancePath               string
		initialVersionedTransition *persistencespb.VersionedTransition
	}
)

func NewInstanceRef(
	key Key,
	consistencyToken ConsistenceToken,
	instanceType string,
) InstanceRef {
	return InstanceRef{
		// TODO
	}
}

func (r *InstanceRef) Key() Key {
	return r.key
}

func (r *InstanceRef) ConsistenceToken() ConsistenceToken {
	// TODO: return topInstance lastUpdateVT
	panic("not implemented")
}

func (r *InstanceRef) Serialize() ([]byte, error) {
	panic("not implemented")
}

func DeserializeInstanceRef(data []byte) (InstanceRef, error) {
	panic("not implemented")
}

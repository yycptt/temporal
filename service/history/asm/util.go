package asm

import (
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

func ToConsistenceToken(
	versionedTransition *persistencespb.VersionedTransition,
) (ConsistenceToken, error) {
	return versionedTransition.Marshal()
}

func FromConsistenceToken(
	token ConsistenceToken,
) (*persistencespb.VersionedTransition, error) {
	var vt persistencespb.VersionedTransition
	err := vt.Unmarshal(token)
	return &vt, err
}

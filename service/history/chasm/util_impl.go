package chasm

import persistencespb "go.temporal.io/server/api/persistence/v1"

func CompareVersionedTransition(
	a, b *persistencespb.VersionedTransition,
) int {
	if a.GetNamespaceFailoverVersion() < b.GetNamespaceFailoverVersion() {
		return -1
	}
	if a.GetNamespaceFailoverVersion() > b.GetNamespaceFailoverVersion() {
		return 1
	}

	if a.GetTransitionCount() < b.GetTransitionCount() {
		return -1
	}
	if a.GetTransitionCount() > b.GetTransitionCount() {
		return 1
	}

	return 0
}

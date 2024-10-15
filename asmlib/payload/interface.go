package payload

import (
	commonpb "go.temporal.io/api/common/v1"
)

type (
	Payload interface {
		Set(*commonpb.Payload)
		Get() *commonpb.Payload
	}
)

package callback

import "go.temporal.io/server/service/history/chasm"

type Callback struct{}

type InvocationInfo struct{}

func (c *Callback) LoadInvocationInfo(
	chasmContext chasm.Context,
) (*InvocationInfo, error) {
	panic("not implemented")
}

type InvocationTask struct{}

type InvocationTaskHandler struct{}

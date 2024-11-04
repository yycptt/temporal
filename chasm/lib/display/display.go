package display

import (
	"encoding/json"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/server/chasm"
)

type Library struct {
}

func (Library) Services() (services []*nexus.Service) {
	service := nexus.NewService("display")
	_ = service.Register(describeOperation)
	// _ = service.Register(listOperation)
	services = append(services, service)
	return
}

// NOTE all of this will be in proto definitions. Actual structure of Describe and List operations is not yet defined, this is just an example.
type DescribeRequest struct {
	Key chasm.InstanceKey
}

type DescribeResponse struct {
	Components []ComponentDescription
}

type ComponentDescription struct {
	Path []string
	Data json.RawMessage
}

type Describable interface {
	Describe() json.RawMessage
}

var describeOperation = chasm.NewReadOperation("Describe", func (root chasm.Component, ctx chasm.ReadContext, request *DescribeRequest) (*DescribeResponse, error) {
	descriptions := make([]ComponentDescription, 0)
		for path, node := range ctx.Walk(root) {
			if desc, ok := node.(Describable); ok {
				descriptions = append(descriptions, ComponentDescription{
					Path: path,
					Data: desc.Describe(),
				})
			}
		}
	return &DescribeResponse{
		Components: descriptions,
	}, nil
})

package ratelimiter

import (
	"context"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/server/chasm"
	"golang.org/x/time/rate"
)

type Library struct {
}

func (Library) Tasks() []chasm.TaskType {
	return nil
}


func (Library) Services() (defs []*nexus.Service) {
	service := nexus.NewService("ratelimiter")
	_ = service.Register(waitOperation)
	defs = append(defs, service)
	return
}

var _ chasm.Library = Library{}

type RateLimiter struct {
	lim *rate.Limiter
}

func New(ctx chasm.ReadContext, options *struct{}) (chasm.Component, error) {
	return RateLimiter{
		rate.NewLimiter(rate.Every(time.Second), 100),
	}, nil
}

// This will have codegen.
type WaitRequest struct {
	Ref chasm.Ref
}

type WaitResponse struct {
}

var waitOperation = chasm.NewSyncOperation[*WaitRequest, *WaitResponse]("Wait", func(ctx context.Context, engine chasm.Engine, request *WaitRequest, options nexus.StartOperationOptions) (*WaitResponse, error) {
	err := chasm.UpdateComponent(ctx, engine, request.Ref, func(sm RateLimiter) error {
		return sm.lim.Wait(ctx)
	})
	if err != nil {
		return nil, err
	}

	return &WaitResponse{}, nil
})

package grl

import (
	"context"
	"time"

	"go.temporal.io/server/service/history/asm"
)

type (
	handlerImpl struct {
		//
		engine asm.Engine
	}

	Config struct {
		Rate       float64
		BurstRatio float64
		Priorities int32
	}

	GetTokensRequest struct {
		Name              string
		Config            Config
		PriorityTokenRate map[int32]float64
	}

	GetTokensResponse struct {
		Tokens int64
		Expiry time.Duration
	}
)

func (h *handlerImpl) GetTokens(
	ctx context.Context,
	request *GetTokensRequest,
) (*GetTokensResponse, error) {
	key := asm.Key{
		NamespaceID: "",
		WorkflowID:  request.Name,
		RunID:       "",
	}

	resp := &GetTokensResponse{}
	_, err := h.engine.UpdateWithNew(
		ctx,
		asm.UpdateWithNewRequest{
			ASMType: asmType,
			Key:     key,
			NewTransitionFn: func(i asm.Instance) error {
				instance := i.(*instanceImpl)
				instance.Init(
					request.Config.Rate,
					request.Config.BurstRatio,
					request.Config.Priorities,
				)
				resp.Tokens, resp.Expiry = instance.GetTokens(request.PriorityTokenRate)
				return nil
			},
			UpdateTransitionFn: func(i asm.Instance) error {
				instance := i.(*instanceImpl)
				resp.Tokens, resp.Expiry = instance.GetTokens(request.PriorityTokenRate)
				return nil
			},
		},
	)
	return resp, err
}

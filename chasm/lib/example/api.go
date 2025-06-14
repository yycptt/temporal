package example

import (
	"context"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
)

func NewPayloadStoreHandler(
	ctx context.Context,
	request *historyservice.NewPayloadStoreRequest,
) (*historyservice.NewPayloadStoreResponse, error) {
	_, entityKey, _, err := chasm.NewEntity(
		ctx,
		chasm.EntityKey{
			NamespaceID: request.NamespaceId,
			BusinessID:  request.StoreId,
		},
		func(_ chasm.MutableContext, _ any) (*PayloadStore, any, error) {
			return NewPayloadStore(), nil, nil
		},
		nil,
	)
	if err != nil {
		return nil, err
	}
	return &historyservice.NewPayloadStoreResponse{
		RunId: entityKey.EntityID,
	}, nil
}

func DescribePayloadStoreHandler(
	ctx context.Context,
	request *historyservice.DescribePayloadStoreRequest,
) (*historyservice.DescribePayloadStoreResponse, error) {
	state, err := chasm.ReadComponent(
		ctx,
		chasm.NewComponentRef[*PayloadStore](
			chasm.EntityKey{
				NamespaceID: request.NamespaceId,
				BusinessID:  request.StoreId,
			},
		),
		(*PayloadStore).Describe,
		request,
	)
	if err != nil {
		return nil, err
	}
	return &historyservice.DescribePayloadStoreResponse{
		TotalCount: state.TotalCount,
		TotalSize:  state.TotalSize,
	}, nil
}

func AddPayloadHandler(
	ctx context.Context,
	request *historyservice.AddPayloadRequest,
) (*historyservice.AddPayloadResponse, error) {
	state, _, err := chasm.UpdateComponent(
		ctx,
		chasm.NewComponentRef[*PayloadStore](
			chasm.EntityKey{
				NamespaceID: request.NamespaceId,
				BusinessID:  request.StoreId,
			},
		),
		(*PayloadStore).AddPayload,
		request,
	)
	if err != nil {
		return nil, err
	}
	return &historyservice.AddPayloadResponse{
		TotalCount: state.TotalCount,
		TotalSize:  state.TotalSize,
	}, nil
}

func GetPayloadHandler(
	ctx context.Context,
	request *historyservice.GetPayloadRequest,
) (*historyservice.GetPayloadResponse, error) {
	payload, err := chasm.ReadComponent(
		ctx,
		chasm.NewComponentRef[*PayloadStore](
			chasm.EntityKey{
				NamespaceID: request.NamespaceId,
				BusinessID:  request.StoreId,
			},
		),
		(*PayloadStore).GetPayload,
		request.PayloadKey,
	)
	if err != nil {
		return nil, err
	}
	return &historyservice.GetPayloadResponse{
		Payload: payload,
	}, nil
}

func RemovePayloadHandler(
	ctx context.Context,
	request *historyservice.RemovePayloadRequest,
) (*historyservice.RemovePayloadResponse, error) {
	state, _, err := chasm.UpdateComponent(
		ctx,
		chasm.NewComponentRef[*PayloadStore](
			chasm.EntityKey{
				NamespaceID: request.NamespaceId,
				BusinessID:  request.StoreId,
			},
		),
		(*PayloadStore).RemovePayload,
		request.PayloadKey,
	)
	if err != nil {
		return nil, err
	}
	return &historyservice.RemovePayloadResponse{
		TotalCount: state.TotalCount,
		TotalSize:  state.TotalSize,
	}, nil
}

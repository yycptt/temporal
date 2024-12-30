package chasm

import (
	"context"
)

type ContextImpl struct {
	ctx context.Context

	*Tree
}

type MutableContextImpl struct {
	*ContextImpl
}

func NewContextImpl(
	ctx context.Context,
	tree *Tree,
) *ContextImpl {
	return &ContextImpl{
		ctx:  ctx,
		Tree: tree,
	}
}

func (c *ContextImpl) getContext() context.Context {
	return c.ctx
}

func NewMutableContextImpl(
	ctx context.Context,
	tree *Tree,
) *MutableContextImpl {
	return &MutableContextImpl{
		ContextImpl: NewContextImpl(ctx, tree),
	}
}

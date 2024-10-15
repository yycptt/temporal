package asm

import (
	"fmt"
)

type (
	Registry interface {
		Register(asm ASM)
		Get(asmType string) (ASM, bool)
		List() []ASM
	}

	registeryImpl struct {
		asms map[string]ASM
	}
)

func NewRegistry() Registry {
	return &registeryImpl{
		asms: make(map[string]ASM),
	}
}

func (r *registeryImpl) Register(asm ASM) {
	asmType := asm.Type()
	if _, ok := r.asms[asmType]; ok {
		panic(fmt.Sprintf("ASM %v already registered", asmType))
	}
	r.asms[asmType] = asm
}

func (r *registeryImpl) Get(asmType string) (ASM, bool) {
	asm, ok := r.asms[asmType]
	return asm, ok
}

func (r *registeryImpl) List() []ASM {
	var asms []ASM
	for _, p := range r.asms {
		asms = append(asms, p)
	}
	return asms
}

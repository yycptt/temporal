package chasm_test

import (
	"fmt"
	"reflect"
	"testing"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/workflow"
)

func TestHelloWorld(t *testing.T) {
	var comp chasm.Component
	comp = workflow.Workflow{}

	typ := reflect.TypeOf(comp)

	numFields := typ.NumField()
	for i := 0; i < numFields; i++ {
		f := typ.Field(i)
		tag := f.Tag.Get("chasm-component")
		if f.IsExported() {
			if f.Type.Kind() == reflect.Map && f.Type.Key().Kind() == reflect.String {
				fmt.Println(tag, f.Type.Elem())
			}
		}
	}

	// t.Fatal("not implemented")
}

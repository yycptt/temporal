package chasm

import (
	"fmt"
	"reflect"
	"testing"

	"go.temporal.io/server/api/persistence/v1"
	"google.golang.org/protobuf/proto"
)

type G[T any] struct {
	GValue any
}

func (g *G[T]) Get() T {
	return g.GValue.(T)
}

type MyStruct struct {
	MyG G[int]
}

func TestDummy(t *testing.T) {
	sType := reflect.TypeFor[MyStruct]()
	gIntType := sType.Field(0).Type

	newS := reflect.New(sType).Elem()
	newGInt := reflect.New(gIntType).Elem()
	newGInt.FieldByName("GValue").Set(reflect.ValueOf(1))
	newS.Field(0).Set(newGInt)

	newTypedS := newS.Interface().(MyStruct)
	fmt.Println(newTypedS.MyG.Get())

	t.Fail()
}

type (
	MyComponent struct {
		Child *ComponentField[Activity]
	}

	Activity interface {
		RecordStarted() error
		RunningState() ComponentState
	}

	activityImpl struct{}
)

func (a *activityImpl) RecordStarted() error {
	fmt.Println("RecordStarted")
	return nil
}
func (a *activityImpl) RunningState() ComponentState {
	return 0
}

func (c *MyComponent) InterceptActivity(
	// chasmContext Context,
	operationInfo OperationInfo[Activity],
	next func() error,
) error {
	fmt.Println("???")
	operationInfo.ChildComponent.RecordStarted()
	return nil
}

type OperationFn = func() error

func TestIntercept(t *testing.T) {
	fmt.Println(proto.MessageName(&persistence.ActivityInfo{}))

	c := &MyComponent{}

	cType := reflect.TypeOf(c)

	cMethod := cType.Method(0)
	for i := 0; i != cMethod.Type.NumIn(); i++ {
		fmt.Println(cMethod.Type.In(i))
	}

	var activity Activity = &activityImpl{}
	aType := reflect.TypeOf(activity)
	aMethod, _ := aType.MethodByName("RecordStarted")
	fmt.Println("======")
	fmt.Println(aMethod.Type)

	var op OperationFn
	opType := reflect.TypeOf(op)
	fmt.Println(opType)
	fmt.Println(aMethod.Type == opType)
	fmt.Println(aMethod.Type.AssignableTo(opType))
	op = activity.RecordStarted

	info := reflect.New(cMethod.Type.In(1)).Elem()
	// info.FieldByName("NextComponent").Set(reflect.ValueOf(reflect.ValueOf(activity)))
	info.FieldByName("ChildComponent").Set(reflect.ValueOf(activity))

	cMethod.Func.Call(
		[]reflect.Value{
			reflect.ValueOf(c),
			// reflect.ValueOf(ctx),
			info,
			reflect.ValueOf(func() error { return nil }),
		},
	)

	t.Fail()
}

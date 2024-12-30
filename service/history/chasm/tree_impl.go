package chasm

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/persistence/serialization"
	"google.golang.org/protobuf/proto"
)

type (
	Tree struct {
		timeSource clock.TimeSource
		backend    TreeBackend
		registry   *Registry

		root *nodeInfo

		// tasks added for unknown components
		danglingTasks map[Component][]componentTaskInfo
	}

	TreeMutation struct {
		UpdatedNodes map[string]*persistence.ChasmNode
		DeletedNodes map[string]struct{}
	}

	TreeSnapshot struct {
		Nodes map[string]*persistence.ChasmNode
	}

	TreeBackend interface {
		GetCurrentVersion() int64
		NextTransitionCount() int64
	}
)

type (
	nodeInfo struct {
		dbInfo   *persistence.ChasmNode
		instance any // deserialized component
		dirty    bool

		children map[string]*nodeInfo
	}

	componentTaskInfo struct {
		taskAttr TaskAttributes
		task     any
	}
)

func NewTree(
	registry *Registry,
	nodesInDB map[string]*persistence.ChasmNode,
	timeSource clock.TimeSource,
	backend TreeBackend,
) *Tree {
	tree := &Tree{
		timeSource:    timeSource,
		backend:       backend,
		registry:      registry,
		root:          &nodeInfo{},
		danglingTasks: make(map[Component][]componentTaskInfo),
	}

	for path, dbInfo := range nodesInDB {
		components := strings.Split(path, "/")
		tree.root.insert(components, dbInfo)
	}

	return tree
}

func (t *Tree) GetComponentByRef(
	chasmContext Context,
	ref ComponentRef,
) (Component, error) {

	nodeInfo, err := t.findNodeByRef(ref)
	if err != nil {
		return nil, err
	}

	rc, ok := t.registry.getComponentByName(ref.componentName)
	if !ok {
		return nil, serviceerror.NewInternal("component not registered")
	}
	if err := nodeInfo.prepare(chasmContext, rc.componentType); err != nil {
		return nil, err
	}

	component := nodeInfo.instance.(Component)
	if ref.validationFn != nil {
		ref.validationFn(chasmContext, component)
	}

	return component, nil
}

func (t *Tree) findNodeByRef(
	ref ComponentRef,
) (*nodeInfo, error) {

	info := t.root.findNode(ref.componentPath)
	if info == nil {
		return nil, serviceerror.NewNotFound("component not found")
	}

	attr, ok := info.dbInfo.Attributes.(*persistence.ChasmNode_ComponentAttributes)
	if !ok {
		return nil, serviceerror.NewNotFound("component not found")
	}
	componentAttr := attr.ComponentAttributes

	if CompareVersionedTransition(
		componentAttr.InitialVersionedTransition,
		ref.componentInitialVT,
	) != 0 {
		return nil, serviceerror.NewNotFound("component not found")
	}

	// if ref.componentName != componentAttr.ComponentName {
	// 	return nil, serviceerror.NewInternal("component name mismatch")
	// }

	return info, nil
}

func (i *nodeInfo) findNode(
	path componentPath,
) *nodeInfo {
	if len(path) == 0 {
		return i
	}

	if i.children == nil {
		return nil
	}

	childNode, ok := i.children[path[0]]
	if !ok {
		return nil
	}
	return childNode.findNode(path[1:])
}

func (i *nodeInfo) insert(
	path componentPath,
	dbInfo *persistence.ChasmNode,
) {
	if len(path) == 0 {
		i.dbInfo = dbInfo
		return
	}

	if i.children == nil {
		i.children = make(map[string]*nodeInfo)
	}
	childNode := &nodeInfo{}
	i.children[path[0]] = childNode
	childNode.insert(path[1:], dbInfo)
}

func (i *nodeInfo) prepare(
	chasmContext Context,
	instanceType reflect.Type,
) error {
	if i.instance == nil || reflect.TypeOf(i.instance) != instanceType {
		err := i.deserializeNode(instanceType)
		if err != nil {
			return err
		}
	}

	if _, isMutable := chasmContext.(MutableContext); isMutable {
		i.dirty = true
	}
	return nil
}

func (i *nodeInfo) isDirty() bool {
	if i == nil {
		return false
	}

	if i.dirty {
		return true
	}

	for _, child := range i.children {
		if child.isDirty() {
			return true
		}
	}
	return false
}

func (i *nodeInfo) closeTransaction(
	backend TreeBackend,
	mutation *TreeMutation,
	currentPath componentPath,
) error {
	if i == nil {
		return nil
	}

	if i.dirty {

		if err := i.serializeNode(backend, mutation, currentPath); err != nil {
			return err
		}
		i.dbInfo.LastUpdateVersionedTransition = &persistence.VersionedTransition{
			TransitionCount:          backend.NextTransitionCount(),
			NamespaceFailoverVersion: backend.GetCurrentVersion(),
		}
		mutation.UpdatedNodes[strings.Join(currentPath, "/")] = i.dbInfo
	}

	for childName, child := range i.children {
		if err := child.closeTransaction(
			backend,
			mutation,
			append(currentPath, childName),
		); err != nil {
			return err
		}
	}
	return nil
}

func (i *nodeInfo) serializeNode(
	backend TreeBackend,
	mutation *TreeMutation,
	currentPath componentPath,
) error {
	switch i.dbInfo.Attributes.(type) {
	case *persistence.ChasmNode_ComponentAttributes:
		return i.serializeComponentNode(backend, mutation, currentPath)
	case *persistence.ChasmNode_DataAttributes:
		return i.serializeDataNode()
	case *persistence.ChasmNode_CollectionAttributes:
		panic("not implemented")
	case *persistence.ChasmNode_PointerAttributes:
		panic("not implemented")
	default:
		return serviceerror.NewInternal("unknown node type")
	}
}

func (i *nodeInfo) serializeComponentNode(
	backend TreeBackend,
	mutation *TreeMutation,
	currentPath componentPath,
) error {
	instanceType := reflect.TypeOf(i.instance)
	instanceValue := reflect.ValueOf(i.instance)
	if instanceType.Kind() != reflect.Struct {
		return serviceerror.NewInternal("only struct is supported for component")
	}

	protoFieldFound := false
	remainingChildren := make(map[string]struct{})
	for idx := 0; idx < instanceType.NumField(); idx++ {
		field := instanceValue.Field(idx)
		// TODO: support chasm name tag on the field
		fieldName := instanceType.Field(idx).Name
		fieldType := field.Type()
		fieldTypeName := fieldType.String()
		if fieldType.AssignableTo(reflect.TypeFor[proto.Message]()) {
			if protoFieldFound {
				return serviceerror.NewInternal("only one proto field allowed in component")
			}
			protoFieldFound = true

			blob, err := serialization.ProtoEncodeBlob(field.Interface().(proto.Message), enums.ENCODING_TYPE_PROTO3)
			if err != nil {
				return err
			}

			i.dbInfo.GetComponentAttributes().ComponentState = blob
			// i.dbInfo.GetComponentAttributes().ComponentName = ???
			if i.dbInfo.GetComponentAttributes().InitialVersionedTransition == nil {
				i.dbInfo.GetComponentAttributes().InitialVersionedTransition = &persistence.VersionedTransition{
					TransitionCount:          backend.NextTransitionCount(),
					NamespaceFailoverVersion: backend.GetCurrentVersion(),
				}
			}
			continue
		}

		remainingChildren[fieldName] = struct{}{}

		if strings.Contains(fieldTypeName, "chasm.Field") {
			var internal fieldInternal
			if fieldType.Kind() == reflect.Ptr {
				internal = field.Elem().FieldByName("Internal").Interface().(fieldInternal)
			} else {
				internal = field.FieldByName("Internal").Interface().(fieldInternal)
			}
			if internal.backingNode != nil {
				// this is not a new node, don't need to do anything
				continue
			}

			// new node or replacing an existing node
			switch internal.fieldType {
			case fieldTypeData:
				i.children[fieldName] = &nodeInfo{
					dbInfo: &persistence.ChasmNode{
						Attributes: &persistence.ChasmNode_DataAttributes{},
					},
					instance: internal.fieldValue,
					dirty:    true,
					children: make(map[string]*nodeInfo),
				}
			case fieldTypeComponent:
				i.children[fieldName] = &nodeInfo{
					dbInfo: &persistence.ChasmNode{
						Attributes: &persistence.ChasmNode_ComponentAttributes{
							ComponentAttributes: &persistence.ChasmComponentAttributes{},
						},
					},
					instance: internal.fieldValue,
					dirty:    true,
					children: make(map[string]*nodeInfo),
				}
			case fieldTypeComponentPointer:
			}

			// TODO: set the field with non-nil backingNode
		}

		if strings.Contains(fieldTypeName, "chasm.Collection") {

			continue
		}
	}

	// remove deleted children
	// TODO: this needs to be done recursively
	// i.e remove the entire subtree
	for childName := range i.children {
		if _, ok := remainingChildren[childName]; !ok {
			mutation.DeletedNodes[strings.Join(append(currentPath, childName), "/")] = struct{}{}
			delete(i.children, childName)
		}
	}

	// validate existing tasks

	// check dangling tasks

	return nil
}

func (i *nodeInfo) serializeDataNode() error {
	protoMsg, ok := i.instance.(proto.Message)
	if !ok {
		return serviceerror.NewInternal("only support proto.Message as chasm data")
	}

	blob, err := serialization.ProtoEncodeBlob(protoMsg, enums.ENCODING_TYPE_PROTO3)
	if err != nil {
		return err
	}

	i.dbInfo.GetDataAttributes().Blob = blob
	return nil
}

func (i *nodeInfo) deserializeNode(
	instanceType reflect.Type,
) error {
	switch i.dbInfo.Attributes.(type) {
	case *persistence.ChasmNode_ComponentAttributes:
		return i.deserializeComponentNode(instanceType)
	case *persistence.ChasmNode_DataAttributes:
		return i.deserializeDataNode(instanceType)
	case *persistence.ChasmNode_CollectionAttributes:
		panic("not implemented")
	case *persistence.ChasmNode_PointerAttributes:
		panic("not implemented")
	default:
		return serviceerror.NewInternal("unknown node type")
	}
}

func (i *nodeInfo) deserializeComponentNode(
	instanceType reflect.Type,
) error {
	if instanceType.Kind() != reflect.Struct {
		return serviceerror.NewInternal("only struct is supported for component")
	}

	attr := i.dbInfo.GetComponentAttributes()

	instanceValue := reflect.New(instanceType).Elem()
	protoFieldFound := false
	for idx := 0; idx < instanceType.NumField(); idx++ {

		field := instanceValue.Field(idx)
		// TODO: support chasm name tag on the field
		fieldName := instanceType.Field(idx).Name
		fieldType := field.Type()
		fieldTypeName := fieldType.String()
		fmt.Println(fieldName, fieldTypeName)
		if fieldType.AssignableTo(reflect.TypeFor[proto.Message]()) {
			if protoFieldFound {
				return serviceerror.NewInternal("only one proto field allowed in component")
			}
			protoFieldFound = true

			value, err := unmarshalProto(attr.ComponentState, fieldType)
			if err != nil {
				return err
			}
			field.Set(value)
			continue
		}

		if strings.Contains(fieldTypeName, "chasm.Field") {
			var fieldValue reflect.Value
			isPtr := fieldType.Kind() == reflect.Ptr
			if isPtr {
				fieldValue = reflect.New(fieldType.Elem())
			} else {
				fieldValue = reflect.New(fieldType).Elem()
			}

			if childNode, found := i.children[fieldName]; found {
				internalValue := reflect.ValueOf(fieldInternal{
					backingNode: childNode,
				})
				if isPtr {
					fieldValue.Elem().FieldByName("Internal").Set(internalValue)
				} else {
					fieldValue.FieldByName("Internal").Set(internalValue)
				}
			}

			field.Set(fieldValue)

			continue
		}

		if strings.Contains(fieldTypeName, "chasm.Collection") {
			// TODO: support collection
			// init the map and populate
			continue
		}

		return serviceerror.NewInternal("unsupported field type in component" + fieldTypeName)
	}

	if !protoFieldFound {
		return serviceerror.NewInternal("no proto field found in component")
	}

	i.instance = instanceValue.Interface()

	return nil
}

func (i *nodeInfo) deserializeDataNode(
	instanceType reflect.Type,
) error {
	value, err := unmarshalProto(i.dbInfo.GetDataAttributes().Blob, instanceType)
	if err != nil {
		return err
	}

	i.instance = value.Interface()
	return nil
}

func unmarshalProto(
	dataBlob *common.DataBlob,
	instanceType reflect.Type,
) (reflect.Value, error) {
	if !instanceType.AssignableTo(reflect.TypeFor[proto.Message]()) {
		return reflect.Value{}, serviceerror.NewInternal("only support proto.Message as chasm data")
	}

	var value reflect.Value
	if instanceType.Kind() == reflect.Ptr {
		value = reflect.New(instanceType.Elem())
	} else {
		value = reflect.New(instanceType).Elem()
	}

	if err := serialization.ProtoDecodeBlob(
		dataBlob,
		value.Interface().(proto.Message),
	); err != nil {
		return reflect.Value{}, err
	}

	return value, nil
}

// this must be called before closing the transaction
func (t *Tree) IsDirty() bool {
	if len(t.danglingTasks) != 0 {
		return true
	}
	return t.root.isDirty()
}

// used by replication
// if snapshot, can just construct a new tree
// tree must be clean after applying the mutation
// i.e. only user logic can make the tree dirty,
// not the chasm implemention
func (t *Tree) ApplyMutation(
	mutation TreeMutation,
) {

	panic("not implemented")
}

func (t *Tree) CloseTransaction() (*TreeMutation, error) {
	mutation := &TreeMutation{}
	return mutation, t.root.closeTransaction(t.backend, mutation, []string{})
}

func (t *Tree) PendingNodes() map[string]*persistence.ChasmNode {
	panic("not implemented")
}

func (t *Tree) trimComponentTasks(
	componentInfo *nodeInfo,
) error {
	panic("not implemented")
}

// implements MutableContext interface
func (t *Tree) AddTask(
	component Component,
	taskAttr TaskAttributes,
	task any,
) error {
	t.danglingTasks[component] = append(t.danglingTasks[component], componentTaskInfo{
		taskAttr: taskAttr,
		task:     task,
	})
	return nil
}

// implements Context interface
func (t *Tree) Ref(
	c Component,
) (ComponentRef, bool) {
	panic("not implemented")
}

// implements Context interface
func (t *Tree) Now(
	c Component,
) time.Time {
	// TODO: support pause
	return t.timeSource.Now()
}

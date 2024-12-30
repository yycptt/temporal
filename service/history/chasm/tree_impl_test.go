package chasm

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence/serialization"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestDeserializeDataNode(t *testing.T) {
	activityInfo := &persistencespb.ActivityInfo{
		Version:       123,
		ScheduledTime: timestamppb.Now(),
	}
	activityInfoBlob, err := serialization.ProtoEncodeBlob(
		activityInfo,
		enums.ENCODING_TYPE_PROTO3,
	)
	assert.NoError(t, err)
	fmt.Println(activityInfoBlob.Data)

	info := &nodeInfo{
		dbInfo: &persistencespb.ChasmNode{
			Attributes: &persistencespb.ChasmNode_DataAttributes{
				DataAttributes: &persistencespb.ChasmDataAttributes{
					Blob: activityInfoBlob,
				},
			},
		},
	}

	err = info.deserializeDataNode(reflect.TypeFor[*persistencespb.ActivityInfo]())
	assert.NoError(t, err)

	fmt.Println(info.instance)

	t.Fail()
}

type Activity struct {
	Info *persistencespb.ActivityInfo

	Input *Field[*commonpb.Payload]
}

func TestDeserializeComponentNode(t *testing.T) {
	activityInfo := &persistencespb.ActivityInfo{
		Version:       123,
		ScheduledTime: timestamppb.Now(),
	}
	activityInfoBlob, err := serialization.ProtoEncodeBlob(
		activityInfo,
		enums.ENCODING_TYPE_PROTO3,
	)
	assert.NoError(t, err)
	fmt.Println(activityInfoBlob.Data)

	info := &nodeInfo{
		dbInfo: &persistencespb.ChasmNode{
			Attributes: &persistencespb.ChasmNode_ComponentAttributes{
				ComponentAttributes: &persistencespb.ChasmComponentAttributes{
					ComponentName:  "Activity",
					ComponentState: activityInfoBlob,
				},
			},
		},
		children: map[string]*nodeInfo{
			"Input": {
				dbInfo: &persistencespb.ChasmNode{},
			},
		},
	}

	err = info.deserializeComponentNode(reflect.TypeFor[Activity]())
	assert.NoError(t, err)

	a := info.instance.(Activity)

	fmt.Printf("%+v\n", a)
	fmt.Printf("%+v\n", a.Input)

	t.Fail()
}

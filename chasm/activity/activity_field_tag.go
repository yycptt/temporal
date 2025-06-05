package activity

import (
	"go.temporal.io/api/common/v1"
	"go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
)

var withFieldTagRegistrableComponent = chasm.NewRegistrableComponent[*ActivityWithFieldTag]("ActivityWithFieldTag")

type ActivityWithFieldTag struct {
	// Will need a custom proto plugin to help generating proto code with chasm tags.
	//
	// ActivityInfo has following fields with chasm tags:
	//     Status          string `chasm:"vis,indexed,id=1"`                        // KEYWORD by default for string fields
	//     ActivityType    string `chasm:"vis,indexed=text,id=2,name=activityType"` // override to use TEXT
	//     Attempt         int32  `chasm:"vis,indexed,id=3"`
	//     NonindexedValue string `chasm:"vis"` // non-indexed value, not searchable, like memo
	State *persistence.ActivityInfo

	// Separate fields doesn't really make too much sense. Separate fields in a Component are useful
	// for taking advantage of the parial update (and read) capability of the CHASM framework.
	// But that's done through chasm.Field values. Primitive types doesn't have such capability and CHASM framework
	// will essentially treat them as dirty whenever the component itself is dirty, which mean those fields can simply
	// be merged into the proto message for the component state.
	//
	// If we were to support them:
	// Tagged fields will be stored as part of the Visibility component if not part of the ComponentState,
	// and automatically populated by the framework.
	//
	// SA1 string `chasm:"vis,indexed,id=1"`
	// SA2 int64 `chasm:"vis,indexed,id=2"`

	// May want to support this as well.
	// Likely only useful for supporting custom SA and Memo.
	Data       chasm.Field[*common.Payload]              `chasm:"vis"` // only supports visibility tags on data node
	CustomMemo chasm.Collection[string, *common.Payload] `chasm:"vis"` // this should work as well, can be either indexed or non-indexed

	// optional, doesn't have to exist
	// An implicit one will be created as long as the chasm:"vis" tag is present.
	Visibility chasm.Field[*chasm.Visibility]

	chasm.UnimplementedComponent
}

func NewActivityWithFieldTag(
	mutableContext chasm.MutableContext,
	activityType string,
	customSA map[string]any,
) (*ActivityWithFieldTag, error) {
	visibility := chasm.NewVisibility()
	visibility.UpdateSearchAttributes(
		mutableContext,
		customSA,
	)

	return &ActivityWithFieldTag{
		State: &persistence.ActivityInfo{
			ActivityType:    &common.ActivityType{Name: activityType}, // pretend this is a string field :)
			Attempt:         1,
			Status:          "Scheduled",
			NonindexedValue: "newActivity debug info",
		},

		Visibility: chasm.NewComponentField(mutableContext, visibility),
	}, nil
}

func (a *ActivityWithFieldTag) Retry(
	mutableContext chasm.MutableContext,
) error {
	a.State.Attempt++
	a.State.Status = "BackingOff"
	a.State.NonindexedValue = "Retry activity debug info"

	// if err := mutableContext.AddTask( ... ); err != nil {
	// 	return err
	// }
	return nil
}

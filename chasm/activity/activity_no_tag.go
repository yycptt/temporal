package activity

import (
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
)

const (
	IndexedAttrActivityType = "ActivityType"
	IndexedAttrAttempt      = "Attempt"
	IndexedAttrStatus       = "Status"
)

const (
	AttrDebugInfo = "DebugInfo"
)

var noTagRegistrableComponent = chasm.NewRegistrableComponent[*ActivityNoTag](
	"ActivityNoTag",
	chasm.WithVisibilityAttributes(
		// Never change the order of the list or remove items from the list.
		[]chasm.VisibilityAttribute{
			{Name: IndexedAttrActivityType, Type: enums.INDEXED_VALUE_TYPE_KEYWORD},
			{Name: IndexedAttrAttempt, Type: enums.INDEXED_VALUE_TYPE_INT},
			{Name: IndexedAttrStatus, Type: enums.INDEXED_VALUE_TYPE_KEYWORD},
		},
	),
)

type ActivityNoTag struct {
	State *persistence.ActivityInfo

	Visibility chasm.Field[*chasm.Visibility]

	chasm.UnimplementedComponent
}

func NewActivityNoTag(
	mutableContext chasm.MutableContext,
	activityType string,
	customSA map[string]any,
) (*ActivityNoTag, error) {
	visibility := chasm.NewVisibility()
	if err := visibility.UpdateSearchAttributes(
		mutableContext,
		customSA,
	); err != nil {
		return nil, err
	}

	chasm.UpdateSearchAttribute(mutableContext, visibility, IndexedAttrActivityType, activityType)
	chasm.UpdateSearchAttribute(mutableContext, visibility, IndexedAttrAttempt, int64(1))
	chasm.UpdateSearchAttribute(mutableContext, visibility, IndexedAttrStatus, "Scheduled")
	chasm.UpdateMemo(mutableContext, visibility, AttrDebugInfo, "newActivity debug info")

	return &ActivityNoTag{
		State: &persistence.ActivityInfo{},

		Visibility: chasm.NewComponentField(mutableContext, visibility),
	}, nil
}

func (a *ActivityNoTag) Retry(
	mutableContext chasm.MutableContext,
) error {
	visibility, err := a.Visibility.Get(mutableContext)
	if err != nil {
		return err
	}

	// Alternatively, store attempt in ActiivityInfo as well
	attempt, err := chasm.SearchAttributeByName[int64](mutableContext, visibility, "Attempt")
	if err != nil {
		return err
	}

	chasm.UpdateSearchAttribute(mutableContext, visibility, IndexedAttrAttempt, attempt+1)
	chasm.UpdateSearchAttribute(mutableContext, visibility, IndexedAttrStatus, "BackingOff")
	chasm.UpdateMemo(mutableContext, visibility, AttrDebugInfo, "Retry activity debug info")

	// if err := mutableContext.AddTask( ... ); err != nil {
	// 	return err
	// }

	return nil
}

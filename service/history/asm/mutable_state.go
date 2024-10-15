package asm

import (
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
)

type (
	MutableState interface {
		workflow.MutableState

		ConsistencyToken() (ConsistenceToken, error)
		ValidateConsistenceToken(ConsistenceToken) error

		GetASMInstance(path string, initialVT *persistencespb.VersionedTransition) (Instance, error)
		AddASMInstance(path string, instance Instance) error
		ApplyASMTransition(path string, output TransitionOutput) error
		RemoveASM(path string) error
	}

	inMemoryASMInstanceInfo struct {
		instance Instance
		dbTasks  []Task
		memTasks []Task
		dirty    bool
		deleted  bool
	}

	MutableStateImpl struct {
		workflow.MutableState

		asmRegisty Registry

		pendingPersistedASMInfos map[string]*persistencespb.ASMInstanceInfo // path -> persisted encoded instance
		pendingInMemoryASMInfos  map[string]*inMemoryASMInstanceInfo        // path -> in memeory decoded instance
	}
)

func NewMutableState(
	shard shard.Context,
	asmRegisty Registry,
	eventsCache events.Cache,
	logger log.Logger,
	namespaceEntry *namespace.Namespace,
	asmID string,
	runID string,
	startTime time.Time,
) *MutableStateImpl {
	return &MutableStateImpl{
		MutableState: workflow.NewMutableState(
			shard,
			eventsCache,
			logger,
			namespaceEntry,
			asmID,
			runID,
			startTime,
		),
		asmRegisty:               asmRegisty,
		pendingPersistedASMInfos: make(map[string]*persistencespb.ASMInstanceInfo),
		pendingInMemoryASMInfos:  make(map[string]*inMemoryASMInstanceInfo),
	}
}

func NewMutableStateFromDB(
	shard shard.Context,
	asmRegisty Registry,
	eventsCache events.Cache,
	logger log.Logger,
	namespaceEntry *namespace.Namespace,
	dbRecord *persistencespb.WorkflowMutableState,
	dbRecordVersion int64,
) (*MutableStateImpl, error) {
	wfMutableState, err := workflow.NewMutableStateFromDB(
		shard,
		shard.GetEventsCache(),
		logger,
		namespaceEntry,
		dbRecord,
		dbRecordVersion,
	)
	if err != nil {
		return nil, err
	}

	return &MutableStateImpl{
		MutableState: wfMutableState,
		asmRegisty:   asmRegisty,

		pendingPersistedASMInfos: dbRecord.AsmInfos,
		pendingInMemoryASMInfos:  make(map[string]*inMemoryASMInstanceInfo),
	}, nil
}

func (m *MutableStateImpl) CurrentVersionedTransition() *persistencespb.VersionedTransition {
	transitionHistory := m.MutableState.GetExecutionInfo().TransitionHistory
	if len(transitionHistory) == 0 {
		return nil
	}
	return transitionHistory[len(transitionHistory)-1]
}

func (m *MutableStateImpl) ConsistencyToken() (ConsistenceToken, error) {
	versionedTransition := m.CurrentVersionedTransition()
	return versionedTransition.Marshal()
}

func (m *MutableStateImpl) ValidateConsistenceToken(
	token ConsistenceToken,
) error {
	var refVersionedTransition *persistencespb.VersionedTransition
	if err := refVersionedTransition.Unmarshal(token); err != nil {
		return err
	}

	return workflow.TransitionHistoryStalenessCheck(
		m.MutableState.GetExecutionInfo().TransitionHistory,
		refVersionedTransition,
	)
}

func (m *MutableStateImpl) GetASMInstance(
	path string,
	initialVT *persistencespb.VersionedTransition,
) (Instance, error) {
	var inMemoryInstance *inMemoryASMInstanceInfo
	var ok bool
	if inMemoryInstance, ok = m.pendingInMemoryASMInfos[path]; !ok {
		persistedInfo, ok := m.pendingPersistedASMInfos[path]
		if !ok {
			return nil, ErrASMInstanceNotFound
		}

		asm, ok := m.asmRegisty.Get(persistedInfo.GetAsmType())
		if !ok {
			return nil, ErrASMTypeNotRegistered
		}
		asmInstance, err := asm.Serializer().DeserializeState(
			newInstanceBackend(m.asmRegisty, m, path),
			persistedInfo.InstanceState,
		)
		if err != nil {
			return nil, err
		}
		inMemoryInstance := &inMemoryASMInstanceInfo{
			instance: asmInstance,
			dbTasks:  nil, // TODO: need to populate this
			memTasks: nil,
			dirty:    false,
			deleted:  false,
		}
		m.pendingInMemoryASMInfos[path] = inMemoryInstance
	}

	if inMemoryInstance.deleted {
		return nil, ErrASMInstanceNotFound
	}

	persistedInfo, ok := m.pendingPersistedASMInfos[path]
	if initialVT != nil && ok && workflow.CompareVersionedTransition(
		initialVT,
		persistedInfo.InitialVersionedTransition,
	) != 0 {
		return nil, ErrASMInstanceNotFound
	}

	return inMemoryInstance.instance, nil

}

func (m *MutableStateImpl) AddASMInstance(
	path string,
	instance Instance,
) error {
	if _, ok := m.pendingPersistedASMInfos[path]; ok {
		return serviceerror.NewAlreadyExist(path + "already exists")
	}
	if _, ok := m.pendingInMemoryASMInfos[path]; ok {
		return serviceerror.NewInternal(path + "already exists")
	}

	inMemoryInfo := &inMemoryASMInstanceInfo{
		instance: instance,
		dirty:    true,
	}
	m.pendingInMemoryASMInfos[path] = inMemoryInfo
	return nil
}

func (m *MutableStateImpl) RemoveASM(path string) error {
	if _, ok := m.pendingPersistedASMInfos[path]; !ok {
		delete(m.pendingInMemoryASMInfos, path)
		return nil
	}

	var inMemoryInfo *inMemoryASMInstanceInfo
	if _, ok := m.pendingInMemoryASMInfos[path]; !ok {
		inMemoryInfo = &inMemoryASMInstanceInfo{}
		m.pendingInMemoryASMInfos[path] = inMemoryInfo
	}

	inMemoryInfo.deleted = true
	inMemoryInfo.dirty = true

	return nil
}

func (m *MutableStateImpl) ApplyASMTransition(
	path string,
	transition TransitionOutput,
) error {

	inMemoryASMInstanceInfo, ok := m.pendingInMemoryASMInfos[path]
	if !ok {
		return serviceerror.NewInternal("in memory instance not found")
	}

	inMemoryASMInstanceInfo.dirty = true
	inMemoryASMInstanceInfo.memTasks = append(inMemoryASMInstanceInfo.memTasks, transition.Tasks...)

	// TODO: handle history events

	return nil
}

func (m *MutableStateImpl) closeTransactionSerializeDirtyASM() error {
	nextVersionedTransition := &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: m.MutableState.GetCurrentVersion(),
		TransitionCount:          m.MutableState.NextTransitionCount(),
	}

	for path, inMemoryASMInfo := range m.pendingInMemoryASMInfos {
		if !inMemoryASMInfo.dirty {
			continue
		}

		if inMemoryASMInfo.deleted {
			delete(m.pendingPersistedASMInfos, path)
			continue
		}

		asm, ok := m.asmRegisty.Get(inMemoryASMInfo.instance.Type())
		if !ok {
			return ErrASMTypeNotRegistered
		}

		serializer := asm.Serializer()
		serializedState, err := serializer.SerializeState(inMemoryASMInfo.instance)
		if err != nil {
			return err
		}
		persistedASMInfo, ok := m.pendingPersistedASMInfos[path]
		if !ok {
			// newly added asm
			persistedASMInfo = &persistencespb.ASMInstanceInfo{
				InitialVersionedTransition:    nextVersionedTransition,
				LastUpdateVersionedTransition: nextVersionedTransition,
				AsmType:                       inMemoryASMInfo.instance.Type(),
			}
			m.pendingPersistedASMInfos[path] = persistedASMInfo
		}
		persistedASMInfo.InstanceState = serializedState
		persistedASMInfo.LastUpdateVersionedTransition = nextVersionedTransition

		serializedTasks := make([]*commonpb.DataBlob, 0, len(inMemoryASMInfo.memTasks))
		persistenceTasks := make([]tasks.Task, 0, len(inMemoryASMInfo.memTasks))
		for _, task := range inMemoryASMInfo.memTasks {
			serializedTask, err := serializer.SerializeTask(task)
			if err != nil {
				return err
			}
			serializedTasks = append(serializedTasks, serializedTask)
			persistedASMInfo.Tasks = append(persistedASMInfo.Tasks, &persistencespb.ASMInstanceInfo_ASMTask{
				// TODO: this can't work, we are not able to create an actual task for those stored tasks
				// because TaskProperties (ScheduledTime and Destination) are not stored.
				// (replication only)
				Data:                serializedTask.Data,
				VersionedTransition: nextVersionedTransition,
			})

			destination := task.Properties().Destination
			scheduledTime := task.Properties().ScheduledTime
			if len(destination) != 0 && !scheduledTime.IsZero() {
				return ErrOutboundTimerNotSupported
			}
			if !scheduledTime.IsZero() {
				persistenceTasks = append(persistenceTasks, &tasks.StateMachineTimerTask{
					WorkflowKey:         m.GetWorkflowKey(),
					VisibilityTimestamp: scheduledTime,
					Version:             nextVersionedTransition.NamespaceFailoverVersion, // not used actually
					ASMTaskInfo: &persistencespb.ASMTaskInfo{
						DataBlob:                   serializedTask,
						SubAsmId:                   path,
						InitialVersionedTransition: persistedASMInfo.InitialVersionedTransition,
						VersionedTransition:        nextVersionedTransition,
						RootAsmType:                asm.Type(),
					},
				})
			} else if len(destination) != 0 {
				persistenceTasks = append(persistenceTasks, &tasks.StateMachineOutboundTask{
					StateMachineTask: tasks.StateMachineTask{
						WorkflowKey: m.GetWorkflowKey(),
						ASMTaskInfo: &persistencespb.ASMTaskInfo{
							DataBlob:                   serializedTask,
							SubAsmId:                   path,
							InitialVersionedTransition: persistedASMInfo.InitialVersionedTransition,
							VersionedTransition:        nextVersionedTransition,
							RootAsmType:                asm.Type(),
						},
					},
					Destination: destination,
				})
			} else {
				persistenceTasks = append(persistenceTasks, &tasks.StateMachineTask{
					WorkflowKey: m.GetWorkflowKey(),
					ASMTaskInfo: &persistencespb.ASMTaskInfo{
						DataBlob:                   serializedTask,
						SubAsmId:                   path,
						InitialVersionedTransition: persistedASMInfo.InitialVersionedTransition,
						VersionedTransition:        nextVersionedTransition,
						RootAsmType:                asm.Type(),
					},
				})
			}
		}
		m.AddTasks(persistenceTasks...)
	}

	return nil
}

func (m *MutableStateImpl) CloseTransactionAsMutation(
	transactionPolicy workflow.TransactionPolicy,
) (*persistence.WorkflowMutation, []*persistence.WorkflowEvents, error) {

	if err := m.closeTransactionSerializeDirtyASM(); err != nil {
		return nil, nil, err
	}

	mutation, events, err := m.MutableState.CloseTransactionAsMutation(transactionPolicy)
	if err != nil {
		return nil, nil, err
	}

	for path, inMemoryASMInfo := range m.pendingInMemoryASMInfos {
		if inMemoryASMInfo.deleted {
			mutation.DeleteASMInfos[path] = struct{}{}
			// TODO: update tombstones
			// executionInfo.SubStateMachineTombstoneBatches
			continue
		}

		if inMemoryASMInfo.dirty {
			mutation.UpsertASMInfos[path] = m.pendingPersistedASMInfos[path]
		}

		inMemoryASMInfo.dirty = false
		inMemoryASMInfo.dbTasks = append(inMemoryASMInfo.dbTasks, inMemoryASMInfo.memTasks...)
		inMemoryASMInfo.memTasks = nil
	}

	m.cleanupTransaction()

	return mutation, events, nil
}

func (m *MutableStateImpl) CloseTransactionAsSnapshot(
	transactionPolicy workflow.TransactionPolicy,
) (*persistence.WorkflowSnapshot, []*persistence.WorkflowEvents, error) {
	if err := m.closeTransactionSerializeDirtyASM(); err != nil {
		return nil, nil, err
	}

	snapshot, events, err := m.MutableState.CloseTransactionAsSnapshot(transactionPolicy)
	if err != nil {
		return nil, nil, err
	}

	snapshot.ASMInfos = m.pendingPersistedASMInfos

	// TODO: update tombstones (for replication)
	// executionInfo.SubStateMachineTombstoneBatches

	m.cleanupTransaction()

	return snapshot, events, nil
}

func (m *MutableStateImpl) cleanupTransaction() {
	for path, inMemoryASMInfo := range m.pendingInMemoryASMInfos {
		if inMemoryASMInfo.deleted {
			delete(m.pendingInMemoryASMInfos, path)
		}
		inMemoryASMInfo.dirty = false
		inMemoryASMInfo.dbTasks = append(inMemoryASMInfo.dbTasks, inMemoryASMInfo.memTasks...)
		inMemoryASMInfo.memTasks = nil
	}
}

// TODO chasm: block all methods on mutable state by default

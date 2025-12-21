package fsm

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/downfa11-org/go-broker/pkg/coordinator"
	"github.com/downfa11-org/go-broker/pkg/disk"
	"github.com/downfa11-org/go-broker/pkg/topic"
	"github.com/downfa11-org/go-broker/pkg/types"
	"github.com/downfa11-org/go-broker/util"
	"github.com/hashicorp/raft"
)

type ReplicationEntry struct {
	Topic     string
	Partition int
	Message   types.Message
	Term      uint64
}

type BrokerInfo struct {
	ID       string    `json:"id"`
	Addr     string    `json:"addr"`
	Status   string    `json:"status"`
	LastSeen time.Time `json:"last_seen"`
}

type BrokerFSMState struct {
	Version           int                           `json:"version"`
	Applied           uint64                        `json:"applied"`
	Logs              map[uint64]*ReplicationEntry  `json:"logs"`
	Brokers           map[string]*BrokerInfo        `json:"brokers"`
	PartitionMetadata map[string]*PartitionMetadata `json:"partitionMetadata"`
	ProducerState     map[string]uint64             `json:"producerState"`
}

type BrokerFSM struct {
	notifiers map[string]chan interface{}
	mu        sync.RWMutex

	logs              map[uint64]*ReplicationEntry
	brokers           map[string]*BrokerInfo
	partitionMetadata map[string]*PartitionMetadata
	producerState     map[string]uint64 // ProducerID -> LastSeqNum
	applied           uint64

	dm *disk.DiskManager
	tm *topic.TopicManager
	cd *coordinator.Coordinator
}

func NewBrokerFSM(dm *disk.DiskManager, tm *topic.TopicManager, cd *coordinator.Coordinator) *BrokerFSM {
	return &BrokerFSM{
		notifiers:         make(map[string]chan interface{}),
		logs:              make(map[uint64]*ReplicationEntry),
		brokers:           make(map[string]*BrokerInfo),
		partitionMetadata: make(map[string]*PartitionMetadata),
		producerState:     make(map[string]uint64),
		dm:                dm,
		tm:                tm,
		cd:                cd,
	}
}

func (f *BrokerFSM) GetBrokers() []BrokerInfo {
	f.mu.RLock()
	defer f.mu.RUnlock()

	var brokers []BrokerInfo
	for _, broker := range f.brokers {
		brokers = append(brokers, *broker)
	}
	return brokers
}

func (f *BrokerFSM) SetCoordinator(cd *coordinator.Coordinator) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cd = cd
}

func (f *BrokerFSM) Apply(log *raft.Log) interface{} {
	data := string(log.Data)
	util.Debug("Applying log entry at index %d", log.Index)

	var reqID string
	if strings.Contains(data, "{") {
		var meta struct {
			ReqID string `json:"req_id"`
		}

		_ = json.Unmarshal([]byte(extractJSON(data)), &meta)
		reqID = meta.ReqID
	}

	var res interface{}
	switch {
	case strings.HasPrefix(data, "REGISTER:"):
		res = f.applyRegisterCommand(strings.TrimPrefix(data, "REGISTER:"))
	case strings.HasPrefix(data, "DEREGISTER:"):
		res = f.applyDeregisterCommand(strings.TrimPrefix(data, "DEREGISTER:"))
	case strings.HasPrefix(data, "MESSAGE:"):
		res = f.applyMessageCommand(strings.TrimPrefix(data, "MESSAGE:"))
	case strings.HasPrefix(data, "BATCH:"):
		res = f.applyMessageCommand(strings.TrimPrefix(data, "BATCH:"))
	case strings.HasPrefix(data, "TOPIC:"):
		res = f.applyTopicCommand(strings.TrimPrefix(data, "TOPIC:"))
	case strings.HasPrefix(data, "PARTITION:"):
		res = f.applyPartitionCommand(data)
	case strings.HasPrefix(data, "GROUP_SYNC:"):
		res = f.applyGroupSyncCommand(strings.TrimPrefix(data, "GROUP_SYNC:"))
	case strings.HasPrefix(data, "OFFSET_SYNC:"):
		res = f.applyOffsetSyncCommand(strings.TrimPrefix(data, "OFFSET_SYNC:"))
	case strings.HasPrefix(data, "BATCH_OFFSET:"):
		res = f.applyBatchOffsetSyncCommand(strings.TrimPrefix(data, "BATCH_OFFSET:"))
	default:
		res = f.handleUnknownCommand(data)
	}

	if reqID != "" {
		f.notify(reqID, res)
	}

	f.mu.Lock()
	f.applied = log.Index
	f.mu.Unlock()

	return res
}

func (f *BrokerFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	util.Info("Starting FSM restore from snapshot")

	var state BrokerFSMState
	if err := json.NewDecoder(rc).Decode(&state); err != nil {
		util.Error("Failed to decode snapshot: %v", err)
		return fmt.Errorf("failed to restore snapshot: %w", err)
	}

	switch state.Version {
	case 0:
		util.Warn("FSM Restore: Legacy snapshot detected (Version 0).")
	case 1:
		util.Info("FSM Restore: Validating snapshot Version 1")
	default:
		return fmt.Errorf("unknown snapshot version: %d.", state.Version)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.logs = state.Logs
	f.brokers = state.Brokers
	f.partitionMetadata = state.PartitionMetadata
	f.applied = state.Applied

	f.producerState = state.ProducerState
	if f.producerState == nil {
		f.producerState = make(map[string]uint64)
	}

	util.Info("FSM restore completed: %d logs, %d brokers, %d partitions", len(state.Logs), len(state.Brokers), len(state.PartitionMetadata))
	return nil
}

func (f *BrokerFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	logsCopy := make(map[uint64]*ReplicationEntry, len(f.logs))
	for k, v := range f.logs {
		entryCopy := *v
		logsCopy[k] = &entryCopy
	}
	brokersCopy := make(map[string]*BrokerInfo, len(f.brokers))
	for k, v := range f.brokers {
		brokerCopy := *v
		brokersCopy[k] = &brokerCopy
	}
	metadataCopy := make(map[string]*PartitionMetadata, len(f.partitionMetadata))
	for k, v := range f.partitionMetadata {
		metaCopy := *v
		metadataCopy[k] = &metaCopy
	}
	producerStateCopy := make(map[string]uint64, len(f.producerState))
	for k, v := range f.producerState {
		producerStateCopy[k] = v
	}

	util.Debug("Creating FSM snapshot")
	return &BrokerFSMSnapshot{
		applied:           f.applied,
		logs:              logsCopy,
		brokers:           brokersCopy,
		partitionMetadata: metadataCopy,
		producerState:     producerStateCopy,
	}, nil
}

func (f *BrokerFSM) persistMessage(topicName string, partition int, msg *types.Message) error {
	dh, err := f.dm.GetHandler(topicName, partition)
	if err != nil {
		return fmt.Errorf("failed to get disk handler for topic %s: %w", topicName, err)
	}

	msg.Offset = dh.GetAbsoluteOffset()
	serialized, err := util.SerializeMessage(*msg)
	if err != nil {
		return fmt.Errorf("failed to serialize message: %w", err)
	}

	if err := dh.WriteDirect(topicName, partition, msg.Offset, string(serialized)); err != nil {
		return fmt.Errorf("WriteDirect failed: %w", err)
	}
	util.Debug("FSM persisted message: topic=%s, offset=%d", topicName, msg.Offset)
	return nil
}

func (f *BrokerFSM) persistBatch(topicName string, partition int, msgs []types.Message) error {
	dh, err := f.dm.GetHandler(topicName, partition)
	if err != nil {
		return fmt.Errorf("failed to get disk handler for topic %s: %w", topicName, err)
	}

	base := dh.GetAbsoluteOffset()
	for i := range msgs {
		msgs[i].Offset = base + uint64(i)
		serialized, err := util.SerializeMessage(msgs[i])
		if err != nil {
			return fmt.Errorf("failed to serialize message at index %d: %w", i, err)
		}

		if err := dh.WriteDirect(topicName, partition, msgs[i].Offset, string(serialized)); err != nil {
			return fmt.Errorf("WriteDirect failed: %w", err)
		}
	}

	util.Debug("FSM persisted batch: topic=%s, count=%d", topicName, len(msgs))
	return nil
}

func (f *BrokerFSM) GetPartitionMetadata(key string) *PartitionMetadata {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if meta := f.partitionMetadata[key]; meta != nil {
		copy := *meta
		return &copy
	}
	return nil
}

// todo.
func (f *BrokerFSM) getCurrentRaftLeaderID() string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	for id := range f.brokers {
		return id
	}
	return "unknown-leader"
}

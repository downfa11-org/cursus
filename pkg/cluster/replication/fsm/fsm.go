package fsm

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

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

type BrokerFSM struct {
	mu                sync.RWMutex
	logs              map[uint64]*ReplicationEntry
	brokers           map[string]*BrokerInfo
	partitionMetadata map[string]*PartitionMetadata
	applied           uint64

	dm *disk.DiskManager
	tm *topic.TopicManager
}

func NewBrokerFSM(dm *disk.DiskManager, tm *topic.TopicManager) *BrokerFSM {
	return &BrokerFSM{
		logs:              make(map[uint64]*ReplicationEntry),
		brokers:           make(map[string]*BrokerInfo),
		partitionMetadata: make(map[string]*PartitionMetadata),
		dm:                dm,
		tm:                tm,
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

func (f *BrokerFSM) Apply(log *raft.Log) interface{} {
	data := string(log.Data)
	util.Debug("Applying log entry at index %d", log.Index)

	switch {
	case strings.HasPrefix(data, "REGISTER:"):
		return f.applyRegisterCommand(data[9:])
	case strings.HasPrefix(data, "DEREGISTER:"):
		return f.applyDeregisterCommand(data[11:])
	case strings.HasPrefix(data, "MESSAGE:"):
		return f.applyMessageCommand(data[8:])
	case strings.HasPrefix(data, "BATCH:"):
		return f.applyMessageCommand(data[6:])
	case strings.HasPrefix(data, "TOPIC:"):
		return f.applyTopicCommand(data[6:])
	case strings.HasPrefix(data, "PARTITION:"):
		return f.applyPartitionCommand(data)
	default:
		return f.handleUnknownCommand(data)
	}
}

func (f *BrokerFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	util.Info("Starting FSM restore from snapshot")

	var state struct {
		Logs              map[uint64]*ReplicationEntry  `json:"logs"`
		Brokers           map[string]*BrokerInfo        `json:"brokers"`
		PartitionMetadata map[string]*PartitionMetadata `json:"partitionMetadata"`
	}

	if err := json.NewDecoder(rc).Decode(&state); err != nil {
		util.Error("Failed to decode snapshot: %v", err)
		return fmt.Errorf("failed to restore snapshot: %w", err)
	}

	f.mu.Lock()
	f.logs = state.Logs
	f.brokers = state.Brokers
	f.partitionMetadata = state.PartitionMetadata

	maxIndex := uint64(0)
	for index := range f.logs {
		if index > maxIndex {
			maxIndex = index
		}
	}
	f.applied = maxIndex
	f.mu.Unlock()

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

	util.Debug("Creating FSM snapshot")
	return &BrokerFSMSnapshot{
		logs:              logsCopy,
		brokers:           brokersCopy,
		partitionMetadata: metadataCopy,
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

	dh.WriteDirect(topicName, partition, msg.Offset, string(serialized))
	util.Debug("FSM persisted message: topic=%s, offset=%d", topicName, msg.Offset)
	return nil
}

func (f *BrokerFSM) persistBatch(topicName string, partition int, msgs []types.Message) error {
	dh, err := f.dm.GetHandler(topicName, partition)
	if err != nil {
		return fmt.Errorf("failed to get disk handler for topic %s: %w", topicName, err)
	}

	for i := range msgs {
		msgs[i].Offset = dh.GetAbsoluteOffset() + uint64(i)
		serialized, err := util.SerializeMessage(msgs[i])
		if err != nil {
			return fmt.Errorf("failed to serialize message at index %d: %w", i, err)
		}

		dh.WriteDirect(topicName, partition, msgs[i].Offset, string(serialized))
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

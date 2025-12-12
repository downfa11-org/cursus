package replication

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/downfa11-org/go-broker/pkg/disk"
	"github.com/downfa11-org/go-broker/util"
	"github.com/hashicorp/raft"
)

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

	diskHandler *disk.DiskHandler
}

func NewBrokerFSM(diskHandler *disk.DiskHandler) *BrokerFSM {
	return &BrokerFSM{
		logs:              make(map[uint64]*ReplicationEntry),
		brokers:           make(map[string]*BrokerInfo),
		partitionMetadata: make(map[string]*PartitionMetadata),
		diskHandler:       diskHandler,
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

	if strings.HasPrefix(data, "REGISTER:") {
		var broker BrokerInfo
		if err := json.Unmarshal([]byte(data[9:]), &broker); err != nil {
			util.Error("Failed to unmarshal broker registration: %v", err)
			return err
		}
		f.mu.Lock()
		f.brokers[broker.ID] = &broker
		f.mu.Unlock()

		util.Info("Registered broker %s at %s", broker.ID, broker.Addr)
		return nil
	} else if strings.HasPrefix(data, "DEREGISTER:") {
		brokerID := data[11:]
		f.mu.Lock()
		delete(f.brokers, brokerID)
		f.mu.Unlock()

		util.Info("Deregistered broker %s", brokerID)
		return nil
	} else if strings.HasPrefix(data, "MESSAGE:") {
		var entry ReplicationEntry
		jsonData := data[8:] // Skip "MESSAGE:" prefix
		if err := json.Unmarshal([]byte(jsonData), &entry); err != nil {
			util.Error("Failed to unmarshal replication entry: %v", err)
			return fmt.Errorf("failed to unmarshal entry: %w", err)
		}

		f.mu.Lock()
		f.logs[log.Index] = &entry
		f.applied = log.Index
		f.mu.Unlock()

		return f.persistMessage(&entry)
	} else if strings.HasPrefix(data, "PARTITION:") {
		parts := strings.SplitN(data, ":", 3)

		if len(parts) != 3 {
			return fmt.Errorf("invalid PARTITION command format: expected PARTITION:key:json")
		}

		key := parts[1]
		var metadata PartitionMetadata
		if err := json.Unmarshal([]byte(parts[2]), &metadata); err != nil {
			util.Error("Failed to unmarshal partition metadata: %v", err)
			return err
		}

		f.mu.Lock()
		f.partitionMetadata[key] = &metadata
		f.mu.Unlock()

		util.Debug("Updated partition metadata for %s", key)

		return nil
	}

	preview := data
	if len(preview) > 20 {
		preview = preview[:20]
	}
	util.Debug("Unknown log entry type: %s", preview)
	return nil
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

type BrokerFSMSnapshot struct {
	logs              map[uint64]*ReplicationEntry
	brokers           map[string]*BrokerInfo
	partitionMetadata map[string]*PartitionMetadata
}

func (s *BrokerFSMSnapshot) Persist(sink raft.SnapshotSink) error {
	state := struct {
		Logs              map[uint64]*ReplicationEntry  `json:"logs"`
		Brokers           map[string]*BrokerInfo        `json:"brokers"`
		PartitionMetadata map[string]*PartitionMetadata `json:"partitionMetadata"`
	}{
		Logs:              s.logs,
		Brokers:           s.brokers,
		PartitionMetadata: s.partitionMetadata,
	}

	util.Debug("Persisting snapshot data")
	err := json.NewEncoder(sink).Encode(state)
	if err != nil {
		cancelErr := sink.Cancel()
		if cancelErr != nil {
			util.Error("Failed to cancel snapshot after encoding error: %v", cancelErr)
		}
		return err
	}
	return sink.Close()
}

func (s *BrokerFSMSnapshot) Release() {}

func (f *BrokerFSM) persistMessage(entry *ReplicationEntry) error {
	if f.diskHandler == nil {
		util.Error("Disk handler not initialized for message persistence")
		return fmt.Errorf("disk handler not initialized")
	}

	f.diskHandler.AppendMessage(entry.Message.Payload)
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

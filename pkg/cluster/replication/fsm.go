package replication

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/downfa11-org/go-broker/pkg/disk"
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

	if strings.HasPrefix(data, "REGISTER:") {
		var broker BrokerInfo
		if err := json.Unmarshal([]byte(data[9:]), &broker); err != nil {
			return err
		}
		f.mu.Lock()
		f.brokers[broker.ID] = &broker
		f.mu.Unlock()
		return nil
	} else if strings.HasPrefix(data, "DEREGISTER:") {
		brokerID := data[11:]
		f.mu.Lock()
		delete(f.brokers, brokerID)
		f.mu.Unlock()
		return nil
	} else if strings.HasPrefix(data, "MESSAGE:") {
		var entry ReplicationEntry
		if err := json.Unmarshal(log.Data, &entry); err != nil {
			return fmt.Errorf("failed to unmarshal entry: %w", err)
		}

		f.mu.Lock()
		f.logs[log.Index] = &entry
		f.applied = log.Index
		f.mu.Unlock()

		return f.persistMessage(&entry)
	} else if strings.HasPrefix(data, "PARTITION:") {
		parts := strings.SplitN(data, ":", 3)
		if len(parts) == 3 {
			key := parts[1]
			var metadata PartitionMetadata
			if err := json.Unmarshal([]byte(parts[2]), &metadata); err != nil {
				return err
			}

			f.mu.Lock()
			f.partitionMetadata[key] = &metadata
			f.mu.Unlock()
		}
		return nil
	}

	return nil
}

func (f *BrokerFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var state struct {
		Logs              map[uint64]*ReplicationEntry  `json:"logs"`
		Brokers           map[string]*BrokerInfo        `json:"brokers"`
		PartitionMetadata map[string]*PartitionMetadata `json:"partitionMetadata"`
	}

	if err := json.NewDecoder(rc).Decode(&state); err != nil {
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

	return nil
}

func (f *BrokerFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return &BrokerFSMSnapshot{
		logs:              f.logs,
		brokers:           f.brokers,
		partitionMetadata: f.partitionMetadata,
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

	err := json.NewEncoder(sink).Encode(state)
	if err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *BrokerFSMSnapshot) Release() {}

func (f *BrokerFSM) persistMessage(entry *ReplicationEntry) error {
	if f.diskHandler == nil {
		return fmt.Errorf("disk handler not initialized")
	}

	f.diskHandler.AppendMessage(entry.Message.Payload)
	return nil
}

func (f *BrokerFSM) GetPartitionMetadata(key string) *PartitionMetadata {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.partitionMetadata[key]
}

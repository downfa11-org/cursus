package fsm

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/downfa11-org/go-broker/util"
)

func (f *BrokerFSM) parseMessageEntry(jsonData string) (*ReplicationEntry, error) {
	var entry ReplicationEntry
	if err := json.Unmarshal([]byte(jsonData), &entry); err != nil {
		util.Error("Failed to unmarshal replication entry: %v", err)
		return nil, fmt.Errorf("failed to unmarshal entry: %w", err)
	}
	return &entry, nil
}

func (f *BrokerFSM) parsePartitionCommand(data string) (string, *PartitionMetadata, error) {
	parts := strings.SplitN(data, ":", 3)

	if len(parts) != 3 {
		return "", nil, fmt.Errorf("invalid PARTITION command format: expected PARTITION:key:json")
	}

	key := parts[1]
	var metadata PartitionMetadata
	if err := json.Unmarshal([]byte(parts[2]), &metadata); err != nil {
		util.Error("Failed to unmarshal partition metadata: %v", err)
		return "", nil, err
	}

	return key, &metadata, nil
}

func (f *BrokerFSM) storeBroker(id string, broker *BrokerInfo) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.brokers[id] = broker
}

func (f *BrokerFSM) removeBroker(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.brokers, id)
}

func (f *BrokerFSM) storeLogEntry(index uint64, entry *ReplicationEntry) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.logs[index] = entry
	f.applied = index
}

func (f *BrokerFSM) storePartitionMetadata(key string, metadata *PartitionMetadata) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.partitionMetadata[key] = metadata
}

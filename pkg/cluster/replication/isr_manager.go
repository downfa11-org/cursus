package replication

import (
	"fmt"
	"sync"
	"time"

	"github.com/downfa11-org/go-broker/pkg/cluster/replication/fsm"
	"github.com/downfa11-org/go-broker/util"
)

const defaultHeartbeatTimeout = 10 * time.Second

type ISRManager struct {
	fsm              *fsm.BrokerFSM
	brokerID         string
	mu               sync.RWMutex
	lastSeen         map[string]time.Time
	heartbeatTimeout time.Duration
}

func NewISRManager(fsm *fsm.BrokerFSM, brokerID string, heartbeatTimeout time.Duration) *ISRManager {
	if heartbeatTimeout <= 0 {
		heartbeatTimeout = defaultHeartbeatTimeout
	}
	return &ISRManager{
		fsm:              fsm,
		brokerID:         brokerID,
		lastSeen:         make(map[string]time.Time),
		heartbeatTimeout: heartbeatTimeout,
	}
}

func (i *ISRManager) Start() {
	go func() {
		ticker := time.NewTicker(i.heartbeatTimeout / 2)
		defer ticker.Stop()

		for range ticker.C {
			i.refreshAllISRs()
			i.CleanStaleHeartbeats()
		}
	}()
}

func (i *ISRManager) refreshAllISRs() {
	partitionKeys := i.fsm.GetAllPartitionKeys()

	for _, key := range partitionKeys {
		var topic string
		var partition int

		_, err := fmt.Sscanf(key, "%s-%d", &topic, &partition) // topic-partitionID
		if err != nil {
			util.Debug("Skipping invalid partition key format: %s", key)
			continue
		}

		i.ComputeISR(topic, partition)
	}
}

// UpdateHeartbeat records the last heartbeat for a broker.
func (i *ISRManager) UpdateHeartbeat(brokerID string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.lastSeen[brokerID] = time.Now()
}

func (i *ISRManager) ComputeISR(topic string, partition int) []string {
	key := fmt.Sprintf("%s-%d", topic, partition)

	var isr []string
	i.mu.RLock()
	metadata := i.fsm.GetPartitionMetadata(key)
	if metadata != nil {
		for _, broker := range metadata.Replicas {
			if last, ok := i.lastSeen[broker]; ok && time.Since(last) < i.heartbeatTimeout {
				isr = append(isr, broker)
			}
		}
	}
	i.mu.RUnlock()

	if metadata == nil {
		util.Warn("Partition metadata not found for %s. Returning empty ISR.", key)
		return nil
	}

	i.fsm.UpdatePartitionISR(key, isr)
	return isr
}

// GetISR returns the latest ISR for a partition (FSM authoritative).
func (i *ISRManager) GetISR(topic string, partition int) []string {
	key := fmt.Sprintf("%s-%d", topic, partition)
	metadata := i.fsm.GetPartitionMetadata(key)
	if metadata == nil {
		util.Warn("Partition metadata not found for %s. Returning empty ISR.", key)
		return nil
	}
	return append([]string(nil), metadata.ISR...)
}

// HasQuorum checks if enough live replicas exist for the partition.
func (i *ISRManager) HasQuorum(topic string, partition int, minISR int) bool {
	isr := i.GetISR(topic, partition)

	currentISRCount := len(isr)
	isLeaderInISR := false
	for _, brokerID := range isr {
		if brokerID == i.brokerID {
			isLeaderInISR = true
			break
		}
	}

	if !isLeaderInISR {
		util.Error("Leader (%s) is not in its own ISR list for %s-%d", i.brokerID, topic, partition)
		return false
	}

	if currentISRCount >= minISR {
		util.Debug("Quorum met for %s-%d: current ISR count %d >= min ISR %d", topic, partition, currentISRCount, minISR)
		return true
	}

	util.Warn("Quorum NOT met for %s-%d: current ISR count %d < min ISR %d", topic, partition, currentISRCount, minISR)
	return false
}

// CleanStaleHeartbeats removes old heartbeat entries.
func (i *ISRManager) CleanStaleHeartbeats() {
	i.mu.Lock()
	defer i.mu.Unlock()

	now := time.Now()
	for brokerID, last := range i.lastSeen {
		if now.Sub(last) > i.heartbeatTimeout {
			delete(i.lastSeen, brokerID)
		}
	}
}

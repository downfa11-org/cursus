package replication

import (
	"fmt"
	"sync"
	"time"

	"github.com/downfa11-org/go-broker/pkg/cluster/replication/fsm"
	"github.com/downfa11-org/go-broker/util"
)

type ISRManager struct {
	fsm      *fsm.BrokerFSM
	brokerID string
	mu       sync.RWMutex
	lastSeen map[string]time.Time // brokerID -> last heartbeat
}

func NewISRManager(fsm *fsm.BrokerFSM, brokerID string) *ISRManager {
	return &ISRManager{
		fsm:      fsm,
		brokerID: brokerID,
		lastSeen: make(map[string]time.Time),
	}
}

// HasQuorum checks if the current number of in-sync replicas for a partition meets minISR.
func (i *ISRManager) HasQuorum(topic string, partition int, minISR int) bool {
	i.mu.RLock()
	defer i.mu.RUnlock()

	key := fmt.Sprintf("%s-%d", topic, partition)
	metadata := i.fsm.GetPartitionMetadata(key)

	if metadata == nil {
		util.Warn("Partition metadata not found for %s. Cannot check quorum.", key)
		return false
	}

	currentISRCount := len(metadata.ISR)
	isLeaderInISR := false
	for _, brokerID := range metadata.ISR {
		if brokerID == i.brokerID {
			isLeaderInISR = true
			break
		}
	}

	if !isLeaderInISR {
		util.Error("Leader (%s) is not in its own ISR list for %s.", i.brokerID, key)
		return false
	}

	if currentISRCount >= minISR {
		util.Debug("Quorum met for %s: current ISR count %d >= min ISR %d", key, currentISRCount, minISR)
		return true
	}

	util.Warn("Quorum NOT met for %s: current ISR count %d < min ISR %d", key, currentISRCount, minISR)
	return false
}

func (i *ISRManager) UpdateHeartbeat(brokerID string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.lastSeen[brokerID] = time.Now()
}

func (i *ISRManager) GetISR() []string {
	i.mu.RLock()
	defer i.mu.RUnlock()

	brokers := i.fsm.GetBrokers()
	var isr []string

	for _, broker := range brokers {
		if lastSeen, ok := i.lastSeen[broker.ID]; ok {
			if time.Since(lastSeen) < 10*time.Second {
				isr = append(isr, broker.ID)
			}
		}
	}

	return isr
}

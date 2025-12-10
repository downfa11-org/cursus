package replication

import (
	"fmt"
	"sync"
	"time"
)

type PreferredLeaderManager struct {
	mu                sync.RWMutex
	preferredLeaders  map[string]string // topic-partition -> preferred broker
	replicaLoad       map[string]int    // broker -> partition count
	rebalanceInterval time.Duration
}

func NewPreferredLeaderManager() *PreferredLeaderManager {
	return &PreferredLeaderManager{
		preferredLeaders:  make(map[string]string),
		replicaLoad:       make(map[string]int),
		rebalanceInterval: 5 * time.Minute,
	}
}

func (plm *PreferredLeaderManager) SetPreferredLeader(topic string, partition int, brokerAddr string) {
	key := fmt.Sprintf("%s-%d", topic, partition)

	plm.mu.Lock()
	defer plm.mu.Unlock()

	plm.preferredLeaders[key] = brokerAddr
}

func (plm *PreferredLeaderManager) GetPreferredLeader(topic string, partition int) (string, bool) {
	key := fmt.Sprintf("%s-%d", topic, partition)

	plm.mu.RLock()
	defer plm.mu.RUnlock()

	leader, exists := plm.preferredLeaders[key]
	return leader, exists
}

func (plm *PreferredLeaderManager) ShouldRebalance(currentLeader, preferredLeader string) bool {
	return currentLeader != preferredLeader && plm.canRebalance(currentLeader, preferredLeader)
}

func (plm *PreferredLeaderManager) canRebalance(current, preferred string) bool {
	plm.mu.RLock()
	defer plm.mu.RUnlock()

	currentLoad := plm.replicaLoad[current]
	preferredLoad := plm.replicaLoad[preferred]

	return preferredLoad <= currentLoad+2
}

func (plm *PreferredLeaderManager) UpdateReplicaLoad(brokerAddr string, delta int) {
	plm.mu.Lock()
	defer plm.mu.Unlock()

	plm.replicaLoad[brokerAddr] += delta
}

func (plm *PreferredLeaderManager) GetReplicaLoad(brokerAddr string) int {
	plm.mu.RLock()
	defer plm.mu.RUnlock()
	return plm.replicaLoad[brokerAddr]
}

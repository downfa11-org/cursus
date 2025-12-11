package replication

import (
	"fmt"
	"sync"

	"github.com/downfa11-org/go-broker/util"
)

type PreferredLeaderManager struct {
	mu               sync.RWMutex
	preferredLeaders map[string]string // topic-partition -> preferred broker
	replicaLoad      map[string]int    // broker -> partition count
}

func NewPreferredLeaderManager() *PreferredLeaderManager {
	return &PreferredLeaderManager{
		preferredLeaders: make(map[string]string),
		replicaLoad:      make(map[string]int),
	}
}

func (plm *PreferredLeaderManager) SetPreferredLeader(topic string, partition int, brokerAddr string) {
	key := fmt.Sprintf("%s-%d", topic, partition)

	plm.mu.Lock()
	defer plm.mu.Unlock()

	plm.preferredLeaders[key] = brokerAddr
	util.Debug("Set preferred leader for %s to %s", key, brokerAddr)
}

func (plm *PreferredLeaderManager) GetPreferredLeader(topic string, partition int) (string, bool) {
	key := fmt.Sprintf("%s-%d", topic, partition)

	plm.mu.RLock()
	defer plm.mu.RUnlock()

	leader, exists := plm.preferredLeaders[key]
	if exists {
		util.Debug("Found preferred leader for %s: %s", key, leader)
	} else {
		util.Debug("No preferred leader set for %s", key)
	}
	return leader, exists
}

func (plm *PreferredLeaderManager) ShouldRebalance(currentLeader, preferredLeader string) bool {
	should := currentLeader != preferredLeader && plm.canRebalance(currentLeader, preferredLeader)
	util.Debug("Rebalance check: current=%s, preferred=%s, should=%v", currentLeader, preferredLeader, should)
	return should
}

func (plm *PreferredLeaderManager) canRebalance(current, preferred string) bool {
	plm.mu.RLock()
	defer plm.mu.RUnlock()

	currentLoad := plm.replicaLoad[current]
	preferredLoad := plm.replicaLoad[preferred]

	can := preferredLoad <= currentLoad+2
	util.Debug("Load check: current=%s (load=%d), preferred=%s (load=%d), can=%v", current, currentLoad, preferred, preferredLoad, can)
	return can
}

func (plm *PreferredLeaderManager) UpdateReplicaLoad(brokerAddr string, delta int) {
	plm.mu.Lock()
	defer plm.mu.Unlock()

	oldLoad := plm.replicaLoad[brokerAddr]
	plm.replicaLoad[brokerAddr] += delta
	newLoad := plm.replicaLoad[brokerAddr]

	util.Debug("Updated replica load for %s: %d -> %d (delta: %d)", brokerAddr, oldLoad, newLoad, delta)
}

func (plm *PreferredLeaderManager) GetReplicaLoad(brokerAddr string) int {
	plm.mu.RLock()
	defer plm.mu.RUnlock()

	load := plm.replicaLoad[brokerAddr]
	util.Debug("Retrieved replica load for %s: %d", brokerAddr, load)
	return load
}

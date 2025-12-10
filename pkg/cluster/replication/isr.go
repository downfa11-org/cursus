package replication

import (
	"fmt"
	"sync"

	"github.com/downfa11-org/go-broker/pkg/metrics"
)

type ISRManager struct {
	rm         *RaftReplicationManager
	isrMap     map[string][]string // topic-partition -> []broker
	replicaLag map[string]int64    // topic-partition -> lag in bytes
	mu         sync.RWMutex
}

func NewISRManager(rm *RaftReplicationManager) *ISRManager {
	return &ISRManager{
		rm:         rm,
		isrMap:     make(map[string][]string),
		replicaLag: make(map[string]int64),
	}
}

func (im *ISRManager) UpdateISR(topic string, partition int, leader string, replicas []string) {
	key := fmt.Sprintf("%s-%d", topic, partition)

	im.mu.Lock()
	defer im.mu.Unlock()

	oldISR := im.isrMap[key]

	isr := []string{leader}
	for _, replica := range replicas {
		if replica != leader && im.replicaLag[key] < 1024*1024 {
			isr = append(isr, replica)
		}
	}

	if len(oldISR) != len(isr) {
		for _, removed := range oldISR {
			if !contains(isr, removed) {
				metrics.ISRChangesTotal.WithLabelValues(topic, fmt.Sprintf("%d", partition), "remove").Inc()
			}
		}
		for _, added := range isr {
			if !contains(oldISR, added) {
				metrics.ISRChangesTotal.WithLabelValues(topic, fmt.Sprintf("%d", partition), "add").Inc()
			}
		}
	}

	im.isrMap[key] = isr
	metrics.ISRSize.WithLabelValues(topic, fmt.Sprintf("%d", partition)).Set(float64(len(isr)))

	for _, replica := range replicas {
		if replica != leader {
			lag := im.replicaLag[key]
			metrics.ReplicationLagBytes.WithLabelValues(topic, fmt.Sprintf("%d", partition), replica).Set(float64(lag))
		}
	}
}

func (im *ISRManager) HasQuorum(topic string, partition int, required int) bool {
	key := fmt.Sprintf("%s-%d", topic, partition)

	im.mu.RLock()
	defer im.mu.RUnlock()

	isr := im.isrMap[key]
	return len(isr) >= required
}

func (im *ISRManager) GetISR(topic string, partition int) []string {
	key := fmt.Sprintf("%s-%d", topic, partition)

	im.mu.RLock()
	defer im.mu.RUnlock()

	return im.isrMap[key]
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

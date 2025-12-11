package replication

import (
	"fmt"
	"sync"

	"github.com/downfa11-org/go-broker/pkg/metrics"
	"github.com/downfa11-org/go-broker/util"
)

type ISRManager struct {
	isrMap     map[string][]string // topic-partition -> []broker
	replicaLag map[string]int64    // topic-partition-replica -> lag in bytes
	mu         sync.RWMutex
}

func NewISRManager() *ISRManager {
	return &ISRManager{
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
		replicaKey := fmt.Sprintf("%s-%d-%s", topic, partition, replica)
		if replica != leader && im.replicaLag[replicaKey] < 1024*1024 {
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
			replicaKey := fmt.Sprintf("%s-%d-%s", topic, partition, replica)
			lag := im.replicaLag[replicaKey]
			metrics.ReplicationLagBytes.WithLabelValues(topic, fmt.Sprintf("%d", partition), replica).Set(float64(lag))
		}
	}

	util.Debug("Updated ISR for %s: %v (lag threshold: 1MB)", key, isr)
}

func (im *ISRManager) HasQuorum(topic string, partition int, required int) bool {
	key := fmt.Sprintf("%s-%d", topic, partition)

	im.mu.RLock()
	defer im.mu.RUnlock()

	isr := im.isrMap[key]
	hasQuorum := len(isr) >= required

	util.Debug("Quorum check for %s: %d/%d (required: %d)", key, len(isr), len(isr), required)
	return hasQuorum
}

func (im *ISRManager) GetISR(topic string, partition int) []string {
	key := fmt.Sprintf("%s-%d", topic, partition)

	im.mu.RLock()
	defer im.mu.RUnlock()

	isr := im.isrMap[key]
	util.Debug("Retrieved ISR for %s: %v", key, isr)
	return isr
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

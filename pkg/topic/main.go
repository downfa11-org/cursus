package topic

import (
	"fmt"
	"sync"
	"time"

	"github.com/downfa11-org/go-broker/pkg/config"
	"github.com/downfa11-org/go-broker/pkg/disk"
	"github.com/downfa11-org/go-broker/pkg/metrics"
	"github.com/downfa11-org/go-broker/pkg/types"
	"github.com/downfa11-org/go-broker/util"
)

// TopicManager manages all topics and deduplication
type TopicManager struct {
	mu         sync.Mutex
	topics     map[string]*Topic
	dedupMap   map[uint64]time.Time
	cleanupInt time.Duration
	stopCh     chan struct{}
	dm         *disk.DiskManager
}

// NewTopicManager creates a new TopicManager with cleanup loop
func NewTopicManager(cfg *config.Config, dm *disk.DiskManager) *TopicManager {
	cleanupSec := cfg.CleanupInterval
	if cleanupSec <= 0 {
		cleanupSec = 60
	}

	tm := &TopicManager{
		topics:     make(map[string]*Topic),
		dedupMap:   make(map[uint64]time.Time),
		cleanupInt: time.Duration(cleanupSec) * time.Second,
		stopCh:     make(chan struct{}),
		dm:         dm,
	}
	go tm.cleanupLoop()
	return tm
}

// CreateTopic creates a new topic with the given partition count
func (tm *TopicManager) CreateTopic(name string, partitionCount int) *Topic {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if existing, ok := tm.topics[name]; ok {
		current := len(existing.Partitions)

		if partitionCount < current {
			fmt.Printf("⚠️ cannot decrease partitions for topic '%s' (%d → %d)\n",
				name, current, partitionCount)
			return existing
		}

		if partitionCount > current {
			existing.AddPartitions(partitionCount-current, tm.dm)
			fmt.Printf("🔄 topic '%s' partitions increased: %d → %d\n",
				name, current, len(existing.Partitions))
			return existing
		}

		// same count, just reuse
		fmt.Printf("ℹ️ topic '%s' already exists with %d partitions\n", name, current)
		return existing
	}

	// brand new topic
	t := NewTopic(name, partitionCount, tm.dm)
	tm.topics[name] = t
	fmt.Printf("✅ topic '%s' created with %d partitions\n", name, partitionCount)
	return t
}

// AddPartitions adds extra partitions to the topic
func (t *Topic) AddPartitions(extra int, dm *disk.DiskManager) {
	for i := 0; i < extra; i++ {
		idx := len(t.Partitions)
		dh, _ := dm.GetHandler(t.Name, idx)
		newP := NewPartition(idx, t.Name, dh)
		t.Partitions = append(t.Partitions, newP)
	}

	for _, group := range t.consumerGroups {
		for i := len(t.Partitions) - extra; i < len(t.Partitions); i++ {
			target := i % len(group.Consumers)
			p := t.Partitions[i]
			c := group.Consumers[target]
			go func(p *Partition, c *Consumer) {
				for msg := range p.ch {
					c.MsgCh <- msg
				}
			}(p, c)
		}
	}
}

// GetTopic returns the topic if exists
func (tm *TopicManager) GetTopic(name string) *Topic {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	return tm.topics[name]
}

// Publish sends a message to a topic if exists
func (tm *TopicManager) Publish(topicName string, msg types.Message) {
	msg.ID = util.GenerateID(msg.Payload)
	now := time.Now()

	tm.mu.Lock()
	if _, exists := tm.dedupMap[msg.ID]; exists {
		tm.mu.Unlock()
		return
	}
	tm.dedupMap[msg.ID] = now
	t := tm.topics[topicName]
	tm.mu.Unlock()

	if t == nil {
		// Skip silently if topic doesn't exist
		return
	}

	start := time.Now()
	t.Publish(msg)
	elapsed := time.Since(start).Seconds()

	metrics.MessagesProcessed.Inc()
	metrics.LatencyHist.Observe(elapsed)
}

// RegisterConsumerGroup registers a group to an existing topic
func (tm *TopicManager) RegisterConsumerGroup(topicName, groupName string, consumerCount int) *ConsumerGroup {
	t := tm.GetTopic(topicName)
	if t == nil {
		return nil
	}
	return t.RegisterConsumerGroup(groupName, consumerCount)
}

// Consume returns a channel for a consumer in a group
func (tm *TopicManager) Consume(topicName, groupName string, consumerIdx int) <-chan types.Message {
	t := tm.GetTopic(topicName)
	if t == nil {
		return nil
	}
	return t.Consume(groupName, consumerIdx)
}

// cleanupLoop periodically cleans old dedup entries
func (tm *TopicManager) cleanupLoop() {
	ticker := time.NewTicker(tm.cleanupInt)
	for {
		select {
		case <-ticker.C:
			tm.CleanupDedup()
		case <-tm.stopCh:
			ticker.Stop()
			return
		}
	}
}

// CleanupDedup removes expired deduplication entries
func (tm *TopicManager) CleanupDedup() {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	expireBefore := time.Now().Add(-30 * time.Minute)
	cleaned := 0
	for id, t := range tm.dedupMap {
		if t.Before(expireBefore) {
			delete(tm.dedupMap, id)
			cleaned++
		}
	}
	if cleaned > 0 {
		metrics.CleanupCount.Add(float64(cleaned))
	}
}

// DeleteTopic removes a topic if exists
func (tm *TopicManager) DeleteTopic(name string) bool {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if _, ok := tm.topics[name]; ok {
		delete(tm.topics, name)
		return true
	}
	return false
}

// ListTopics returns all topic names
func (tm *TopicManager) ListTopics() []string {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	names := make([]string, 0, len(tm.topics))
	for name := range tm.topics {
		names = append(names, name)
	}
	return names
}

// Stop stops the cleanup loop
func (tm *TopicManager) Stop() {
	close(tm.stopCh)
}

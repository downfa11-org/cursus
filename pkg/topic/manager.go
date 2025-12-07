package topic

import (
	"fmt"
	"sync"
	"time"

	"github.com/downfa11-org/go-broker/pkg/config"
	"github.com/downfa11-org/go-broker/pkg/coordinator"
	"github.com/downfa11-org/go-broker/pkg/disk"
	"github.com/downfa11-org/go-broker/pkg/metrics"
	"github.com/downfa11-org/go-broker/pkg/stream"
	"github.com/downfa11-org/go-broker/pkg/types"
	"github.com/downfa11-org/go-broker/util"
)

type TopicManager struct {
	topics        map[string]*Topic
	dedupMap      sync.Map
	cleanupInt    time.Duration
	stopCh        chan struct{}
	hp            HandlerProvider
	mu            sync.RWMutex
	cfg           *config.Config
	StreamManager *stream.StreamManager
	coordinator   *coordinator.Coordinator
}

// HandlerProvider defines an interface to provide disk handlers.
type HandlerProvider interface {
	GetHandler(topic string, partitionID int) (*disk.DiskHandler, error)
}

func (tm *TopicManager) SetCoordinator(cd *coordinator.Coordinator) {
	tm.coordinator = cd
}

func NewTopicManager(cfg *config.Config, hp HandlerProvider, sm *stream.StreamManager) *TopicManager {
	cleanupSec := cfg.CleanupInterval
	if cleanupSec <= 0 {
		cleanupSec = 60
	}

	tm := &TopicManager{
		topics:        make(map[string]*Topic),
		cleanupInt:    time.Duration(cleanupSec) * time.Second,
		stopCh:        make(chan struct{}),
		hp:            hp,
		cfg:           cfg,
		StreamManager: sm,
	}
	go tm.cleanupLoop()
	return tm
}

func (tm *TopicManager) CreateTopic(name string, partitionCount int) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if existing, ok := tm.topics[name]; ok {
		current := len(existing.Partitions)
		switch {
		case partitionCount < current:
			util.Error("⚠️ cannot decrease partitions for topic '%s' (%d → %d)\n", name, current, partitionCount)
			return
		case partitionCount > current:
			existing.AddPartitions(partitionCount-current, tm.hp)
			util.Info("🔄 topic '%s' partitions increased: %d → %d\n", name, current, len(existing.Partitions))
			return
		default:
			util.Info("ℹ️ topic '%s' already exists with %d partitions\n", name, current)
			return
		}
	}

	t, err := NewTopic(name, partitionCount, tm.hp, tm.cfg, tm.StreamManager)
	if err != nil {
		util.Error("❌ failed to create topic '%s': %v\n", name, err)
		return
	}
	tm.topics[name] = t
	t.RegisterConsumerGroup("default-group", 1)
	util.Info("✅ topic '%s' created with %d partitions\n", name, partitionCount)
}

func (tm *TopicManager) GetTopic(name string) *Topic {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.topics[name]
}

// Async (acks=0)
func (tm *TopicManager) Publish(topicName string, msg types.Message) error {
	return tm.publishInternal(topicName, msg, false)
}

// Sync (acks=1)
func (tm *TopicManager) PublishWithAck(topicName string, msg types.Message) error {
	return tm.publishInternal(topicName, msg, true)
}

// Batch Sync (acks=1)
func (tm *TopicManager) PublishBatchSync(topicName string, messages []types.Message) error {
	if len(messages) == 0 {
		return nil
	}

	t := tm.GetTopic(topicName)
	if t == nil {
		return fmt.Errorf("topic '%s' does not exist", topicName)
	}

	partitioned := make(map[int][]types.Message)
	var partitions []*Partition

	t.mu.Lock()
	partitions = make([]*Partition, len(t.Partitions))
	copy(partitions, t.Partitions)
	for _, msg := range messages {
		var idx int
		if msg.Key != "" {
			keyID := util.GenerateID(msg.Key)
			idx = int(keyID % uint64(len(partitions)))
		} else {
			idx = int(t.counter % uint64(len(partitions)))
			t.counter++
		}
		partitioned[idx] = append(partitioned[idx], msg)
	}
	t.mu.Unlock()

	var wg sync.WaitGroup
	errCh := make(chan error, len(partitioned))

	for idx, msgs := range partitioned {
		wg.Add(1)
		go func(p *Partition, msgs []types.Message) {
			defer wg.Done()
			if err := p.EnqueueBatchSync(msgs); err != nil {
				errCh <- fmt.Errorf("partition %d: %w", p.id, err)
			}
		}(partitions[idx], msgs)
	}

	wg.Wait()
	close(errCh)

	if len(errCh) > 0 {
		return <-errCh
	}
	return nil
}

func (tm *TopicManager) publishInternal(topicName string, msg types.Message, requireAck bool) error {
	util.Debug("Starting publish. Topic: %s, RequireAck: %v, ProducerID: %s, SeqNum: %d",
		topicName, requireAck, msg.ProducerID, msg.SeqNum)

	start := time.Now()

	var dedupKey string
	// Idempotence (try to exactly-once)
	if msg.ProducerID != "" && msg.SeqNum > 0 {
		dedupKey = fmt.Sprintf("%s-%s-%d", topicName, msg.ProducerID, msg.SeqNum)
	} else {
		// at-least once
		dedupKey = fmt.Sprintf("%s-%s", topicName, msg.Payload)
	}

	now := time.Now()
	if _, loaded := tm.dedupMap.LoadOrStore(dedupKey, now); loaded {
		util.Info("Duplicate message detected: ProducerID=%s, SeqNum=%d", msg.ProducerID, msg.SeqNum)
		return nil
	}

	t := tm.GetTopic(topicName)
	if t == nil {
		return fmt.Errorf("topic '%s' does not exist", topicName)
	}

	if requireAck {
		if err := t.PublishSync(msg); err != nil {
			// Allow safe retry with same ProducerID+SeqNum
			tm.dedupMap.Delete(dedupKey)
			return fmt.Errorf("sync publish failed: %w", err)
		}
	} else {
		t.Publish(msg)
	}

	elapsed := time.Since(start).Seconds()
	metrics.MessagesProcessed.Inc()
	metrics.LatencyHist.Observe(elapsed)
	return nil
}

func (tm *TopicManager) RegisterConsumerGroup(topicName, groupName string, consumerCount int) (*types.ConsumerGroup, error) {
	t := tm.GetTopic(topicName)
	if t == nil {
		return nil, fmt.Errorf("topic '%s' does not exist", topicName)
	}

	group := t.RegisterConsumerGroup(groupName, consumerCount)

	if tm.coordinator != nil {
		if err := tm.coordinator.RegisterGroup(topicName, groupName, len(t.Partitions)); err != nil {
			if deregErr := t.DeregisterConsumerGroup(groupName); deregErr != nil {
				util.Error("Failed to rollback consumer group '%s' on topic '%s': %v", groupName, topicName, deregErr)
			}
			return nil, fmt.Errorf("failed to register group '%s' with coordinator: %w", groupName, err)
		}

		assignments := tm.coordinator.GetAssignments(groupName)
		t.applyAssignments(groupName, assignments)
	}

	return group, nil
}

func (tm *TopicManager) Flush() {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	for _, t := range tm.topics {
		for _, p := range t.Partitions {
			if p.dh != nil {
				if f, ok := p.dh.(interface{ Flush() }); ok {
					f.Flush()
				}
			}
		}
	}
}

func (tm *TopicManager) Stop() {
	close(tm.stopCh)
}

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

func (tm *TopicManager) CleanupDedup() {
	expireBefore := time.Now().Add(-30 * time.Minute)
	tm.dedupMap.Range(func(key, value any) bool {
		if ts, ok := value.(time.Time); ok && ts.Before(expireBefore) {
			tm.dedupMap.Delete(key)
			metrics.CleanupCount.Inc()
		}
		return true
	})
}

func (tm *TopicManager) DeleteTopic(name string) bool {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if _, ok := tm.topics[name]; ok {
		delete(tm.topics, name)
		return true
	}
	return false
}

func (tm *TopicManager) ListTopics() []string {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	names := make([]string, 0, len(tm.topics))
	for name := range tm.topics {
		names = append(names, name)
	}
	return names
}

func (tm *TopicManager) EnsureDefaultGroups() {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	for name, t := range tm.topics {
		t.RegisterConsumerGroup("default-group", 1)
		util.Info("Registered default-group for topic '%s'", name)
	}
}

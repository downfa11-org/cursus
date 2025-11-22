package topic

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/downfa11-org/go-broker/pkg/config"
	"github.com/downfa11-org/go-broker/pkg/coordinator"
	"github.com/downfa11-org/go-broker/pkg/disk"
	"github.com/downfa11-org/go-broker/pkg/metrics"
	"github.com/downfa11-org/go-broker/pkg/types"
	"github.com/downfa11-org/go-broker/util"
)

type TopicManager struct {
	topics      map[string]*Topic
	dedupMap    sync.Map
	cleanupInt  time.Duration
	stopCh      chan struct{}
	hp          HandlerProvider
	mu          sync.RWMutex
	cfg         *config.Config
	coordinator *coordinator.Coordinator
}

// HandlerProvider defines an interface to provide disk handlers.
type HandlerProvider interface {
	GetHandler(topic string, partitionID int) (*disk.DiskHandler, error)
}

func NewTopicManager(cfg *config.Config, hp HandlerProvider, cd *coordinator.Coordinator) *TopicManager {
	cleanupSec := cfg.CleanupInterval
	if cleanupSec <= 0 {
		cleanupSec = 60
	}

	tm := &TopicManager{
		topics:      make(map[string]*Topic),
		cleanupInt:  time.Duration(cleanupSec) * time.Second,
		stopCh:      make(chan struct{}),
		hp:          hp,
		cfg:         cfg,
		coordinator: cd,
	}
	go tm.cleanupLoop()
	return tm
}

func (tm *TopicManager) CreateTopic(name string, partitionCount int) *Topic {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if existing, ok := tm.topics[name]; ok {
		current := len(existing.Partitions)
		switch {
		case partitionCount < current:
			fmt.Printf("⚠️ cannot decrease partitions for topic '%s' (%d → %d)\n", name, current, partitionCount)
			return existing
		case partitionCount > current:
			existing.AddPartitions(partitionCount-current, tm.hp)
			fmt.Printf("🔄 topic '%s' partitions increased: %d → %d\n", name, current, len(existing.Partitions))
			return existing
		default:
			fmt.Printf("ℹ️ topic '%s' already exists with %d partitions\n", name, current)
			return existing
		}
	}

	t, err := NewTopic(name, partitionCount, tm.hp, tm.coordinator)
	if err != nil {
		fmt.Printf("❌ failed to create topic '%s': %v\n", name, err)
		return nil
	}
	tm.topics[name] = t
	fmt.Printf("✅ topic '%s' created with %d partitions\n", name, partitionCount)
	return t
}

func (tm *TopicManager) GetTopic(name string) *Topic {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.topics[name]
}

func (tm *TopicManager) Publish(topicName string, msg types.Message) error {
	idSource := fmt.Sprintf("%s-%d-%d", msg.Payload, time.Now().UnixNano(), rand.Int63())
	msg.ID = util.GenerateID(idSource)
	now := time.Now()
	if _, loaded := tm.dedupMap.LoadOrStore(msg.ID, now); loaded {
		log.Printf("[DEBUG] Message %d already exists in dedupMap, skipping", msg.ID) // test
		return nil
	}

	t := tm.GetTopic(topicName)
	if t == nil {
		if tm.cfg.AutoCreateTopics {
			t = tm.CreateTopic(topicName, 4) // auto-create topic (default: 4 partition)
			if t == nil {
				log.Printf("[DEBUG] Topic '%s' auto-created, but topic is nil", topicName) // test
				return fmt.Errorf("failed to auto-create topic '%s'", topicName)
			}
		} else {
			log.Printf("[DEBUG] Topic '%s' not found", topicName) // test
			return fmt.Errorf("topic '%s' does not exist", topicName)
		}
	}
	start := time.Now()
	t.Publish(msg)
	elapsed := time.Since(start).Seconds()

	metrics.MessagesProcessed.Inc()
	metrics.LatencyHist.Observe(elapsed)
	return nil
}

func (tm *TopicManager) PublishWithAck(topicName string, msg types.Message) error {
	msg.ID = util.GenerateID(msg.Payload)
	now := time.Now()
	if _, loaded := tm.dedupMap.LoadOrStore(msg.ID, now); loaded {
		return fmt.Errorf("duplicate message: %v", msg.ID)
	}

	t := tm.GetTopic(topicName)
	if t == nil {
		return fmt.Errorf("topic not found: %s", topicName)
	}

	t.mu.Lock()
	partitionCount := len(t.Partitions)
	var idx int
	if msg.Key != "" {
		keyID := util.GenerateID(msg.Key)
		idx = int(keyID % uint64(partitionCount))
	} else {
		idx = int(t.counter % uint64(partitionCount))
		t.counter++
	}
	partition := t.Partitions[idx]
	t.mu.Unlock()

	if !partition.closed {
		select {
		case partition.ch <- msg:
		default:
			return fmt.Errorf("partition channel full")
		}
	}

	if partition.dh != nil {
		if appender, ok := partition.dh.(DiskAppender); ok {
			if err := appender.AppendMessageSync(msg.Payload); err != nil {
				return fmt.Errorf("disk write failed: %w", err)
			}
		} else {
			return fmt.Errorf("disk handler does not implement DiskAppender")
		}
	}

	start := time.Now()
	elapsed := time.Since(start).Seconds()
	metrics.MessagesProcessed.Inc()
	metrics.LatencyHist.Observe(elapsed)

	return nil
}

func (tm *TopicManager) RegisterConsumerGroup(topicName, groupName string, consumerCount int) *ConsumerGroup {
	t := tm.GetTopic(topicName)
	if t == nil {
		return nil
	}
	return t.RegisterConsumerGroup(groupName, consumerCount)
}

func (tm *TopicManager) Consume(topicName, groupName string, consumerIdx int) <-chan types.Message {
	t := tm.GetTopic(topicName)
	if t == nil {
		return nil
	}
	return t.Consume(groupName, consumerIdx)
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

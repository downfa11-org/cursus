package topic

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/downfa11-org/go-broker/pkg/config"
	"github.com/downfa11-org/go-broker/pkg/stream"
	"github.com/downfa11-org/go-broker/pkg/types"
	"github.com/downfa11-org/go-broker/util"
)

const DefaultBufSize = 10000
const DefaultConsumerBufSize = 1000

// Topic represents a logical message stream divided into partitions and consumer groups.
type Topic struct {
	Name           string
	Partitions     []*Partition
	counter        uint64
	consumerGroups map[string]*types.ConsumerGroup
	mu             sync.RWMutex
	cfg            *config.Config
	streamManager  *stream.StreamManager
}

// Partition handles messages for one shard of a topic.
type Partition struct {
	id            int
	topic         string
	mu            sync.RWMutex
	dh            interface{}
	closed        bool
	streamManager *stream.StreamManager
	newMessageCh  chan struct{}
}

type DiskAppender interface {
	AppendMessage(topic string, partition int, offset uint64, payload string)
	AppendMessageSync(topic string, partition int, offset uint64, payload string) error
}

// NewTopic initializes a topic with partitions.
func NewTopic(name string, partitionCount int, hp HandlerProvider, cfg *config.Config, sm *stream.StreamManager) (*Topic, error) {
	partitions := make([]*Partition, partitionCount)
	for i := 0; i < partitionCount; i++ {
		dh, err := hp.GetHandler(name, i)
		if err != nil {
			return nil, fmt.Errorf("open handler for %s[%d]: %w", name, i, err)
		}
		partitions[i] = NewPartition(i, name, dh, sm)
	}
	return &Topic{
		Name:           name,
		Partitions:     partitions,
		consumerGroups: make(map[string]*types.ConsumerGroup),
		cfg:            cfg,
		streamManager:  sm,
	}, nil
}

// NewPartition creates a partition instance.
func NewPartition(id int, topic string, dh interface{}, sm *stream.StreamManager) *Partition {
	p := &Partition{
		id:            id,
		topic:         topic,
		dh:            dh,
		streamManager: sm,
		newMessageCh:  make(chan struct{}, 1),
	}

	return p
}

func (t *Topic) GetPartitionForMessage(msg types.Message) int {
	partitionsLen := uint64(len(t.Partitions))
	if partitionsLen == 0 {
		return -1
	}

	if msg.Key != "" {
		keyID := util.GenerateID(msg.Key)
		return int(keyID % partitionsLen)
	}

	oldCounter := atomic.AddUint64(&t.counter, 1) - 1
	return int(oldCounter % partitionsLen)
}

// AddPartitions extends the topic with new partitions.
func (t *Topic) AddPartitions(extra int, hp HandlerProvider) {
	for i := 0; i < extra; i++ {
		idx := len(t.Partitions)
		dh, err := hp.GetHandler(t.Name, idx)
		if err != nil {
			util.Error("❌ failed to attach partition %d for topic '%s': %v\n", idx, t.Name, err)
			return
		}
		newP := NewPartition(idx, t.Name, dh, t.streamManager)
		t.Partitions = append(t.Partitions, newP)
	}
}

// RegisterConsumerGroup registers a consumer group to the topic.
func (t *Topic) RegisterConsumerGroup(groupName string, consumerCount int) *types.ConsumerGroup {
	t.mu.Lock()
	defer t.mu.Unlock()

	if g, ok := t.consumerGroups[groupName]; ok {
		return g
	}

	group := &types.ConsumerGroup{
		Name:             groupName,
		Consumers:        make([]*types.Consumer, consumerCount),
		CommittedOffsets: make(map[int]uint64),
	}

	for i := 0; i < consumerCount; i++ {
		group.Consumers[i] = &types.Consumer{
			ID: i,
		}
	}

	t.consumerGroups[groupName] = group
	return group
}

// DeregisterConsumerGroup removes a consumer group from the topic.
func (t *Topic) DeregisterConsumerGroup(groupName string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.consumerGroups[groupName]; !ok {
		return fmt.Errorf("consumer group '%s' does not exist", groupName)
	}

	delete(t.consumerGroups, groupName)
	util.Info("Consumer group '%s' deregistered from topic '%s'", groupName, t.Name)
	return nil
}

// Publish sends a message to one partition.
func (t *Topic) Publish(msg types.Message) {
	var idx int
	partitionsLen := uint64(len(t.Partitions))

	if msg.Key != "" {
		keyID := util.GenerateID(msg.Key)
		idx = int(keyID % partitionsLen)
		util.Debug("Key-based routing to partition %d", idx)
	} else {
		oldCounter := atomic.AddUint64(&t.counter, 1) - 1
		idx = int(oldCounter % partitionsLen)
		util.Debug("Round-robin routing to partition %d (counter: %d)", idx, oldCounter)
	}

	p := t.Partitions[idx]
	p.Enqueue(msg)
}

func (t *Topic) PublishSync(msg types.Message) error {
	var idx int
	partitionsLen := uint64(len(t.Partitions))
	util.Debug("Starting sync publish. Topic: %s, Key: %s", t.Name, msg.Key)

	if msg.Key != "" {
		keyID := util.GenerateID(msg.Key)
		idx = int(keyID % partitionsLen)
		util.Debug("Key-based routing to partition %d", idx)
	} else {
		oldCounter := atomic.AddUint64(&t.counter, 1) - 1
		idx = int(oldCounter % partitionsLen)
		util.Debug("Round-robin routing to partition %d", idx)
	}

	util.Debug("Calling Partition[%d].EnqueueSync", idx)
	p := t.Partitions[idx]
	return p.EnqueueSync(msg)
}

func (t *Topic) PublishBatchSync(msgs []types.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	partitioned := make(map[int][]types.Message)

	partitionsLen := uint64(len(t.Partitions))
	for _, msg := range msgs {
		var idx int
		if msg.Key != "" {
			keyID := util.GenerateID(msg.Key)
			idx = int(keyID % partitionsLen)
		} else {
			oldCounter := atomic.AddUint64(&t.counter, 1) - 1
			idx = int(oldCounter % partitionsLen)
		}
		partitioned[idx] = append(partitioned[idx], msg)
	}

	for idx, pm := range partitioned {
		p := t.Partitions[idx]

		if err := p.EnqueueBatchSync(pm); err != nil {
			return fmt.Errorf("partition %d: failed to publish batch: %w", idx, err)
		}
	}
	return nil
}

// applyAssignments connects partitions to consumers according to coordinator results.
func (t *Topic) applyAssignments(groupName string, assignments map[string][]int) {
	group := t.consumerGroups[groupName]
	if group == nil {
		return
	}

	util.Debug("Applied assignments for group '%s': %v", groupName, assignments)
}

func (t *Topic) GetCommittedOffset(groupName string, partition int) (uint64, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	group, ok := t.consumerGroups[groupName]
	if !ok {
		return 0, false
	}

	offset, ok := group.CommittedOffsets[partition]
	return offset, ok
}

func (t *Topic) CommitOffset(groupName string, partition int, offset uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if partition < 0 || partition >= len(t.Partitions) {
		return fmt.Errorf("partition %d out of range [0, %d)", partition, len(t.Partitions))
	}

	group, ok := t.consumerGroups[groupName]
	if !ok {
		return fmt.Errorf("consumer group '%s' not found", groupName)
	}

	if group.CommittedOffsets == nil {
		group.CommittedOffsets = make(map[int]uint64)
	}
	group.CommittedOffsets[partition] = offset
	return nil
}

func (t *Topic) NewMessageSignal(partition int) <-chan struct{} {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if partition < 0 || partition >= len(t.Partitions) {
		util.Warn("NewMessageSignal called with invalid partition %d for topic '%s'", partition, t.Name)
		return nil
	}
	return t.Partitions[partition].newMessageCh
}

package topic

import (
	"fmt"
	"sync"

	"github.com/downfa11-org/go-broker/pkg/disk"
	"github.com/downfa11-org/go-broker/pkg/types"
	"github.com/downfa11-org/go-broker/util"
)

// Topic represents a logical message stream divided into partitions and groups.
type Topic struct {
	Name           string
	Partitions     []*Partition
	counter        uint64
	consumerGroups map[string]*ConsumerGroup
	mu             sync.RWMutex
}

// Partition handles messages for one shard of a topic.
// Each partition maintains its own fan-out to multiple consumer groups.
type Partition struct {
	id     int
	topic  string
	ch     chan types.Message            // producer input
	subs   map[string]chan types.Message // groupName -> group-specific channel
	mu     sync.RWMutex
	dh     *disk.DiskHandler
	closed bool
}

// Consumer represents a single consumer instance in a group.
type Consumer struct {
	ID    int
	MsgCh chan types.Message
}

// ConsumerGroup is a set of consumers subscribed to the same topic.
type ConsumerGroup struct {
	Name      string
	Consumers []*Consumer
}

// NewPartition initializes one partition and starts its fan-out goroutine.
func NewPartition(id int, topic string, dh *disk.DiskHandler) *Partition {
	p := &Partition{
		id:    id,
		topic: topic,
		ch:    make(chan types.Message, 10000),
		subs:  make(map[string]chan types.Message),
		dh:    dh,
	}

	go func() {
		for msg := range p.ch {
			fileKey := fmt.Sprintf("%s_%d", p.topic, p.id)
			if _, err := p.dh.AppendMessage(msg.String()); err != nil {
				fmt.Printf("⚠️ disk write failed for %s: %v\n", fileKey, err)
			}

			// fan-out to all subscribed consumer groups
			p.mu.RLock()
			for _, sub := range p.subs {
				select {
				case sub <- msg:
				default:
					// prevent blocking if subscriber slow
				}
			}
			p.mu.RUnlock()
		}
	}()

	return p
}

// Enqueue accepts producer messages into partition input channel.
func (p *Partition) Enqueue(msg types.Message) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return
	}
	p.ch <- msg
}

// RegisterGroup ensures a dedicated channel exists for a consumer group.
func (p *Partition) RegisterGroup(groupName string) chan types.Message {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	if ch, ok := p.subs[groupName]; ok {
		return ch
	}
	ch := make(chan types.Message, 10000)
	p.subs[groupName] = ch
	return ch
}

// Close safely terminates partition operation.
func (p *Partition) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	close(p.ch)
	for _, sub := range p.subs {
		close(sub)
	}
}

// NewTopic creates a new topic with N partitions.
func NewTopic(name string, partitionCount int, dm *disk.DiskManager) *Topic {
	partitions := make([]*Partition, partitionCount)
	for i := 0; i < partitionCount; i++ {
		dh, _ := dm.GetHandler(name, i)
		partitions[i] = NewPartition(i, name, dh)
	}
	return &Topic{
		Name:           name,
		Partitions:     partitions,
		consumerGroups: make(map[string]*ConsumerGroup),
	}
}

// RegisterConsumerGroup binds all partitions of a topic to a group.
func (t *Topic) RegisterConsumerGroup(groupName string, consumerCount int) *ConsumerGroup {
	t.mu.Lock()
	defer t.mu.Unlock()

	if g, ok := t.consumerGroups[groupName]; ok {
		return g
	}

	group := &ConsumerGroup{
		Name:      groupName,
		Consumers: make([]*Consumer, consumerCount),
	}

	for i := 0; i < consumerCount; i++ {
		group.Consumers[i] = &Consumer{
			ID:    i,
			MsgCh: make(chan types.Message, 1000),
		}
	}

	for pid, p := range t.Partitions {
		groupCh := p.RegisterGroup(groupName)
		if groupCh == nil {
			continue
		}
		target := pid % consumerCount
		go func(ch <-chan types.Message, consumer *Consumer) {
			for msg := range ch {
				consumer.MsgCh <- msg
			}
		}(groupCh, group.Consumers[target])
	}

	t.consumerGroups[groupName] = group
	fmt.Printf("👥 registered consumer group '%s' with %d consumers on topic '%s'\n",
		groupName, consumerCount, t.Name)
	return group
}

// Publish selects a partition (by key or round-robin) and enqueues the message.
func (t *Topic) Publish(msg types.Message) {
	var idx int
	t.mu.Lock()
	if msg.Key != "" {
		keyID := util.GenerateID(msg.Key)
		idx = int(keyID % uint64(len(t.Partitions)))
	} else {
		idx = int(t.counter % uint64(len(t.Partitions)))
		t.counter++
	}
	t.mu.Unlock()

	// partition enqueue
	p := t.Partitions[idx]
	p.Enqueue(msg)
}

// Consume returns a consumer channel for reading messages.
func (t *Topic) Consume(groupName string, consumerIdx int) <-chan types.Message {
	t.mu.RLock()
	group, ok := t.consumerGroups[groupName]
	t.mu.RUnlock()
	if !ok || consumerIdx >= len(group.Consumers) {
		return nil
	}
	return group.Consumers[consumerIdx].MsgCh
}

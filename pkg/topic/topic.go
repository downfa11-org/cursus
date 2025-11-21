package topic

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/downfa11-org/go-broker/pkg/types"

	"github.com/downfa11-org/go-broker/util"
)

// Topic represents a logical message stream divided into partitions and consumer groups.
type Topic struct {
	Name           string
	Partitions     []*Partition
	counter        uint64
	consumerGroups map[string]*ConsumerGroup
	coordinator    Coordinator
	mu             sync.RWMutex
}

// Partition handles messages for one shard of a topic.
type Partition struct {
	id     int
	topic  string
	ch     chan types.Message
	subs   map[string]chan types.Message
	mu     sync.RWMutex
	dh     interface{}
	closed bool
}

type ConsumerState int

const (
	ConsumerStateActive ConsumerState = iota
	ConsumerStateDead
	ConsumerStateRebalancing
)

// Consumer represents a single consumer instance in a group.
type Consumer struct {
	ID                 int
	MsgCh              chan types.Message
	LastHeartbeat      time.Time
	AssignedPartitions []int
	State              ConsumerState
	stopCh             chan struct{}
}

// ConsumerGroup is a set of consumers subscribed to the same topic.
type ConsumerGroup struct {
	Name      string
	Consumers []*Consumer
}

type Coordinator interface {
	RegisterGroup(topicName, groupName string, partitionCount int) error
	AddConsumer(groupName, consumerID string) ([]int, error) // return allocated partitions
	RemoveConsumer(groupName, consumerID string) error
	GetAssignments(groupName string) map[string][]int
}

type DiskAppender interface {
	AppendMessage(msg string)
	AppendMessageSync(msg string) error
}

// NewTopic creates a new topic with partitions.
func NewTopic(name string, partitionCount int, hp HandlerProvider) (*Topic, error) {
	partitions := make([]*Partition, partitionCount)
	for i := 0; i < partitionCount; i++ {
		dh, err := hp.GetHandler(name, i)
		if err != nil {
			return nil, fmt.Errorf("open handler for %s[%d]: %w", name, i, err)
		}
		partitions[i] = NewPartition(i, name, dh)
	}
	return &Topic{
		Name:           name,
		Partitions:     partitions,
		consumerGroups: make(map[string]*ConsumerGroup),
	}, nil
}

// NewPartition creates a new partition.
func NewPartition(id int, topic string, dh interface{}) *Partition {
	p := &Partition{
		id:    id,
		topic: topic,
		ch:    make(chan types.Message, 10000),
		subs:  make(map[string]chan types.Message),
		dh:    dh,
	}

	go p.run()
	return p
}

func (p *Partition) run() {
	for msg := range p.ch {
		p.mu.RLock()
		for _, subCh := range p.subs {
			subCh <- msg
		}
		p.mu.RUnlock()
	}

	p.mu.Lock()
	for _, subCh := range p.subs {
		close(subCh)
	}
	p.subs = nil
	p.mu.Unlock()
}

// AddPartitions adds extra partitions to the topic.
func (t *Topic) AddPartitions(extra int, hp HandlerProvider) {
	for i := 0; i < extra; i++ {
		idx := len(t.Partitions)
		dh, err := hp.GetHandler(t.Name, idx)
		if err != nil {
			fmt.Printf("❌ failed to attach partition %d for topic '%s': %v\n", idx, t.Name, err)
			return
		}
		newP := NewPartition(idx, t.Name, dh)
		t.Partitions = append(t.Partitions, newP)
	}
}

// RegisterConsumerGroup binds all partitions of a topic to a consumer group.
func (t *Topic) RegisterConsumerGroup(groupName string, consumerCount int) *ConsumerGroup {
	t.mu.Lock()
	defer t.mu.Unlock()

	if g, ok := t.consumerGroups[groupName]; ok {
		newConsumerID := len(g.Consumers)
		newConsumer := &Consumer{
			ID:     newConsumerID,
			MsgCh:  make(chan types.Message, 1000),
			stopCh: make(chan struct{}),
		}
		g.Consumers = append(g.Consumers, newConsumer)

		if t.coordinator != nil {
			if _, err := t.coordinator.AddConsumer(groupName, fmt.Sprintf("%d", newConsumerID)); err != nil {
				fmt.Printf("❌ failed to add consumer %d: %v\n", newConsumerID, err)
			} else {
				assignments := t.coordinator.GetAssignments(groupName)
				t.applyAssignments(groupName, assignments)
			}
		}
		return g
	}

	group := &ConsumerGroup{
		Name:      groupName,
		Consumers: make([]*Consumer, consumerCount),
	}

	for i := 0; i < consumerCount; i++ {
		group.Consumers[i] = &Consumer{
			ID:     i,
			MsgCh:  make(chan types.Message, 1000),
			stopCh: make(chan struct{}),
		}
	}

	if t.coordinator != nil {
		if err := t.coordinator.RegisterGroup(t.Name, groupName, len(t.Partitions)); err != nil {
			fmt.Printf("❌ failed to register group with coordinator: %v\n", err)
			return nil
		}

		for i := 0; i < consumerCount; i++ {
			_, err := t.coordinator.AddConsumer(groupName, fmt.Sprintf("%d", i))
			if err != nil {
				fmt.Printf("❌ failed to add consumer %d: %v\n", i, err)
			}
		}

		assignments := t.coordinator.GetAssignments(groupName)
		t.applyAssignments(groupName, assignments)
	} else {
		// fallback
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
	}

	t.consumerGroups[groupName] = group
	fmt.Printf("👥 registered consumer group '%s' with %d consumers on topic '%s'\n",
		groupName, consumerCount, t.Name)
	return group
}

// Publish selects a partition and enqueues the message.
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

	p := t.Partitions[idx]
	p.Enqueue(msg)
}

// Consume returns a consumer channel.
func (t *Topic) Consume(groupName string, consumerIdx int) <-chan types.Message {
	t.mu.RLock()
	group, ok := t.consumerGroups[groupName]
	t.mu.RUnlock()
	if !ok || consumerIdx >= len(group.Consumers) {
		return nil
	}
	return group.Consumers[consumerIdx].MsgCh
}

// Partition methods
func (p *Partition) Enqueue(msg types.Message) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return
	}

	p.ch <- msg
	if appender, ok := p.dh.(DiskAppender); ok {
		appender.AppendMessage(msg.Payload)
	} else {
		fmt.Printf("⚠️ Partition %d: DiskHandler does not implement AppendMessage\n", p.id)
	}
}

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

func (t *Topic) applyAssignments(groupName string, assignments map[string][]int) {
	group := t.consumerGroups[groupName]
	if group == nil {
		return
	}

	for _, consumer := range group.Consumers {
		if consumer.stopCh != nil {
			close(consumer.stopCh)
		}
		consumer.stopCh = make(chan struct{})
	}

	for consumerIDStr, partitionIDs := range assignments {
		consumerID, _ := strconv.Atoi(consumerIDStr)
		if consumerID >= len(group.Consumers) {
			continue
		}
		consumer := group.Consumers[consumerID]

		for _, pid := range partitionIDs {
			if pid >= len(t.Partitions) {
				continue
			}
			p := t.Partitions[pid]
			groupCh := p.RegisterGroup(groupName)
			if groupCh == nil {
				continue
			}

			stopCh := consumer.stopCh
			go func(ch <-chan types.Message, c *Consumer, stop <-chan struct{}) {
				for {
					select {
					case <-stop:
						return
					case msg, ok := <-ch:
						if !ok {
							return
						}
						c.MsgCh <- msg
					}
				}
			}(groupCh, consumer, stopCh)
		}
	}
}

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

package coordinator

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/downfa11-org/go-broker/pkg/config"
)

type Coordinator struct {
	groups map[string]*GroupMetadata
	mu     sync.RWMutex
	cfg    *config.Config
}

type GroupMetadata struct {
	TopicName     string
	Members       map[string]*MemberMetadata
	Generation    int
	Partitions    []int
	LastRebalance time.Time
}

type MemberMetadata struct {
	ID            string
	LastHeartbeat time.Time
	Assignments   []int
}

func NewCoordinator(cfg *config.Config) *Coordinator {
	return &Coordinator{
		groups: make(map[string]*GroupMetadata),
		cfg:    cfg,
	}
}

func (c *Coordinator) Start() {
	go c.monitorHeartbeats()
}

func (c *Coordinator) RegisterGroup(topicName, groupName string, partitionCount int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	partitions := make([]int, partitionCount)
	for i := 0; i < partitionCount; i++ {
		partitions[i] = i
	}

	c.groups[groupName] = &GroupMetadata{
		TopicName:  topicName,
		Members:    make(map[string]*MemberMetadata),
		Partitions: partitions,
	}
	return nil
}

func (c *Coordinator) GetAssignments(groupName string) map[string][]int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	group := c.groups[groupName]
	if group == nil {
		return nil
	}

	result := make(map[string][]int)
	for id, member := range group.Members {
		result[id] = member.Assignments
	}
	return result
}

func (c *Coordinator) AddConsumer(groupName, consumerID string) ([]int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	group := c.groups[groupName]
	if group == nil {
		log.Printf("[JOIN_ERROR] ❌ Consumer '%s' failed to join: group '%s' not found", consumerID, groupName)
		return nil, fmt.Errorf("group not found")
	}

	log.Printf("[JOIN_START] 🚀 Consumer '%s' joining group '%s' (current members: %d)",
		consumerID, groupName, len(group.Members))

	group.Members[consumerID] = &MemberMetadata{
		ID:            consumerID,
		LastHeartbeat: time.Now(),
		Assignments:   nil,
	}

	log.Printf("[JOIN_SUCCESS] ✅ Consumer '%s' joined group '%s'", consumerID, groupName)
	return nil, nil
}

func (c *Coordinator) RemoveConsumer(groupName, consumerID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	group := c.groups[groupName]
	if group == nil {
		log.Printf("[LEAVE_ERROR] ❌ Consumer '%s' failed to leave: group '%s' not found", consumerID, groupName)
		return fmt.Errorf("group not found")
	}

	log.Printf("[LEAVE_START] 👋 Consumer '%s' leaving group '%s' (current members: %d)",
		consumerID, groupName, len(group.Members))

	delete(group.Members, consumerID)

	log.Printf("[LEAVE_SUCCESS] ✅ Consumer '%s' left group '%s'. Remaining members: %d",
		consumerID, groupName, len(group.Members))

	return nil
}

func (c *Coordinator) Rebalance(groupName string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rebalanceRange(groupName)
}

func (c *Coordinator) RecordHeartbeat(groupName, consumerID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	group := c.groups[groupName]
	if group == nil {
		log.Printf("[HEARTBEAT_ERROR] ❌ Heartbeat from '%s' failed: group '%s' not found", consumerID, groupName)
		return fmt.Errorf("group not found")
	}

	member := group.Members[consumerID]
	if member == nil {
		log.Printf("[HEARTBEAT_ERROR] ❌ Heartbeat from '%s' failed: consumer not found in group '%s'", consumerID, groupName)
		return fmt.Errorf("consumer not found")
	}

	oldHeartbeat := member.LastHeartbeat
	member.LastHeartbeat = time.Now()

	log.Printf("[HEARTBEAT_RECEIVED] 💓 Consumer '%s' in group '%s' sent heartbeat (previous: %v ago)",
		consumerID, groupName, time.Since(oldHeartbeat))
	return nil
}

package coordinator

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/downfa11-org/go-broker/pkg/config"
)

// Coordinator manages consumer groups, membership, heartbeats, and partition assignment.
type Coordinator struct {
	groups map[string]*GroupMetadata // All consumer groups
	mu     sync.RWMutex              // Global lock for coordinator state
	cfg    *config.Config            // Configuration reference
	stopCh chan struct{}
}

// GroupMetadata holds metadata for a single consumer group.
type GroupMetadata struct {
	TopicName     string                     // Topic this group consumes
	Members       map[string]*MemberMetadata // Active members
	Generation    int                        // Current generation (unused but reserved)
	Partitions    []int                      // All partitions of the topic
	LastRebalance time.Time                  // Timestamp of last rebalance
}

// MemberMetadata holds state for a single consumer instance.
type MemberMetadata struct {
	ID            string    // Unique consumer ID
	LastHeartbeat time.Time // Last heartbeat timestamp
	Assignments   []int     // Partition assignments for this member
}

// NewCoordinator creates a new Coordinator instance.
func NewCoordinator(cfg *config.Config) *Coordinator {
	return &Coordinator{
		groups: make(map[string]*GroupMetadata),
		cfg:    cfg,
		stopCh: make(chan struct{}),
	}
}

// Start launches background monitoring processes (e.g., heartbeat monitor).
func (c *Coordinator) Start() {
	go c.monitorHeartbeats()
}

// Stop launches background monitoring processes (graceful shutdown)
func (c *Coordinator) Stop() {
	close(c.stopCh)
}

// RegisterGroup creates a new consumer group for a topic.
func (c *Coordinator) RegisterGroup(topicName, groupName string, partitionCount int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.groups[groupName]; exists {
		return fmt.Errorf("group '%s' already exists", groupName)
	}

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

// GetAssignments returns the current partition assignments for each group member.
func (c *Coordinator) GetAssignments(groupName string) map[string][]int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	group := c.groups[groupName]
	if group == nil {
		return map[string][]int{}
	}

	result := make(map[string][]int)
	for id, member := range group.Members {
		cp := append([]int(nil), member.Assignments...)
		result[id] = cp
	}
	return result
}

// AddConsumer registers a new consumer in the group and triggers a rebalance.
func (c *Coordinator) AddConsumer(groupName, consumerID string) ([]int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	group := c.groups[groupName]
	if group == nil {
		log.Printf("[JOIN_ERROR] ❌ Consumer '%s' failed to join: group '%s' not found",
			consumerID, groupName)
		return nil, fmt.Errorf("group not found")
	}

	log.Printf("[JOIN_START] 🚀 Consumer '%s' joining group '%s' (current members: %d)",
		consumerID, groupName, len(group.Members))

	group.Members[consumerID] = &MemberMetadata{
		ID:            consumerID,
		LastHeartbeat: time.Now(),
	}

	c.rebalanceRange(groupName)
	assignments := group.Members[consumerID].Assignments

	log.Printf("[JOIN_SUCCESS] ✅ Consumer '%s' joined group '%s'", consumerID, groupName)
	return assignments, nil
}

// RemoveConsumer unregisters a consumer and triggers a rebalance.
func (c *Coordinator) RemoveConsumer(groupName, consumerID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	group := c.groups[groupName]
	if group == nil {
		log.Printf("[LEAVE_ERROR] ❌ Consumer '%s' failed to leave: group '%s' not found",
			consumerID, groupName)
		return fmt.Errorf("group not found")
	}

	log.Printf("[LEAVE_START] 👋 Consumer '%s' leaving group '%s' (current members: %d)",
		consumerID, groupName, len(group.Members))

	delete(group.Members, consumerID)
	c.rebalanceRange(groupName)

	log.Printf("[LEAVE_SUCCESS] ✅ Consumer '%s' left group '%s'. Remaining members: %d",
		consumerID, groupName, len(group.Members))

	return nil
}

// Rebalance forces a rebalance for a consumer group.
func (c *Coordinator) Rebalance(groupName string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rebalanceRange(groupName)
}

// RecordHeartbeat updates the consumer's last heartbeat timestamp.
func (c *Coordinator) RecordHeartbeat(groupName, consumerID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	group := c.groups[groupName]
	if group == nil {
		log.Printf("[HEARTBEAT_ERROR] ❌ Heartbeat from '%s' failed: group '%s' not found",
			consumerID, groupName)
		return fmt.Errorf("group not found")
	}

	member := group.Members[consumerID]
	if member == nil {
		log.Printf(
			"[HEARTBEAT_ERROR] ❌ Heartbeat from '%s' failed: consumer not found in group '%s'",
			consumerID, groupName,
		)
		return fmt.Errorf("consumer not found")
	}

	old := member.LastHeartbeat
	member.LastHeartbeat = time.Now()

	log.Printf("[HEARTBEAT_RECEIVED] 💓 Consumer '%s' in group '%s' sent heartbeat (previous: %v ago)",
		consumerID, groupName, time.Since(old))

	return nil
}

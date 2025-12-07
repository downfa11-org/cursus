package coordinator

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/downfa11-org/go-broker/pkg/config"
	"github.com/downfa11-org/go-broker/pkg/types"
	"github.com/downfa11-org/go-broker/util"
)

// Coordinator manages consumer groups, membership, heartbeats, and partition assignment.
type Coordinator struct {
	groups map[string]*GroupMetadata // All consumer groups
	mu     sync.RWMutex              // Global lock for coordinator state
	cfg    *config.Config            // Configuration reference
	stopCh chan struct{}

	offsetPublisher           OffsetPublisher
	offsetTopic               string
	offsetTopicPartitionCount int

	offsets map[string]map[string]map[int]uint64 // group -> topic -> partition -> offset
}

type OffsetPublisher interface {
	Publish(topic string, msg *types.Message) error
	CreateTopic(topic string, partitionCount int)
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

// GroupStatus represents the status of a consumer group
type GroupStatus struct {
	GroupName      string       `json:"group_name"`
	TopicName      string       `json:"topic_name"`
	State          string       `json:"state"` // "Stable", "Rebalancing", "Dead"
	MemberCount    int          `json:"member_count"`
	PartitionCount int          `json:"partition_count"`
	Members        []MemberInfo `json:"members"`
	LastRebalance  time.Time    `json:"last_rebalance"`
}

type MemberInfo struct {
	MemberID      string    `json:"member_id"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	Assignments   []int     `json:"assignments"`
}

type OffsetCommitMessage struct {
	Group     string    `json:"group"`
	Topic     string    `json:"topic"`
	Partition int       `json:"partition"`
	Offset    uint64    `json:"offset"`
	Timestamp time.Time `json:"timestamp"`
}

// NewCoordinator creates a new Coordinator instance.
func NewCoordinator(cfg *config.Config, publisher OffsetPublisher) *Coordinator {
	if publisher == nil {
		util.Fatal("Coordinator requires a non-nil OffsetPublisher")
	}

	c := &Coordinator{
		groups:                    make(map[string]*GroupMetadata),
		cfg:                       cfg,
		stopCh:                    make(chan struct{}),
		offsetPublisher:           publisher,
		offsetTopic:               "__consumer_offsets",
		offsetTopicPartitionCount: 4, // init. dynamic
		offsets:                   make(map[string]map[string]map[int]uint64),
	}

	// Create offset topic based on initial partition count.
	publisher.CreateTopic(c.offsetTopic, c.offsetTopicPartitionCount)
	return c
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
		return nil
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

	// NOTE: updateOffsetPartitionCount assumes caller holds the lock to avoid deadlock.
	c.updateOffsetPartitionCount()
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
		util.Error("❌ Consumer '%s' failed to join: group '%s' not found",
			consumerID, groupName)
		return nil, fmt.Errorf("group not found")
	}

	util.Info("🚀 Consumer '%s' joining group '%s' (current members: %d)",
		consumerID, groupName, len(group.Members))

	group.Members[consumerID] = &MemberMetadata{
		ID:            consumerID,
		LastHeartbeat: time.Now(),
	}

	c.rebalanceRange(groupName)
	assignments := group.Members[consumerID].Assignments

	util.Info("✅ Consumer '%s' joined group '%s'", consumerID, groupName)
	return assignments, nil
}

// RemoveConsumer unregisters a consumer and triggers a rebalance.
func (c *Coordinator) RemoveConsumer(groupName, consumerID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	group := c.groups[groupName]
	if group == nil {
		util.Error("❌ Consumer '%s' failed to leave: group '%s' not found",
			consumerID, groupName)
		return fmt.Errorf("group not found")
	}

	util.Info("👋 Consumer '%s' leaving group '%s' (current members: %d)",
		consumerID, groupName, len(group.Members))

	delete(group.Members, consumerID)
	c.rebalanceRange(groupName)
	c.updateOffsetPartitionCount()
	util.Info("✅ Consumer '%s' left group '%s'. Remaining members: %d", consumerID, groupName, len(group.Members))
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
		util.Error("❌ Heartbeat from '%s' failed: group '%s' not found",
			consumerID, groupName)
		return fmt.Errorf("group not found")
	}

	member := group.Members[consumerID]
	if member == nil {
		util.Error("❌ Heartbeat from '%s' failed: consumer not found in group '%s'", consumerID, groupName)
		return fmt.Errorf("consumer not found")
	}

	old := member.LastHeartbeat
	member.LastHeartbeat = time.Now()

	util.Debug("💓 Consumer '%s' in group '%s' sent heartbeat (previous: %v ago)",
		consumerID, groupName, time.Since(old))

	return nil
}

func (c *Coordinator) ListGroups() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	groups := make([]string, 0, len(c.groups))
	for name := range c.groups {
		groups = append(groups, name)
	}
	return groups
}

// GetGroupStatus returns the current status of a consumer group
func (c *Coordinator) GetGroupStatus(groupName string) (*GroupStatus, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	group := c.groups[groupName]
	if group == nil {
		return nil, fmt.Errorf("group '%s' not found", groupName)
	}

	// Determine group state
	state := "Stable"
	if len(group.Members) == 0 {
		state = "Dead"
	}

	// Build member info list
	members := make([]MemberInfo, 0, len(group.Members))
	for _, member := range group.Members {
		members = append(members, MemberInfo{
			MemberID:      member.ID,
			LastHeartbeat: member.LastHeartbeat,
			Assignments:   append([]int(nil), member.Assignments...),
		})
	}

	return &GroupStatus{
		GroupName:      groupName,
		TopicName:      group.TopicName,
		State:          state,
		MemberCount:    len(group.Members),
		PartitionCount: len(group.Partitions),
		Members:        members,
		LastRebalance:  group.LastRebalance,
	}, nil
}

func calculateOffsetPartitionCount(groupCount int) int {
	return min(max(groupCount/10, 4), 50)
}

func (c *Coordinator) storeOffsetInMemory(group, topic string, partition int, offset uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.offsets[group]; !ok {
		c.offsets[group] = make(map[string]map[int]uint64)
	}
	if _, ok := c.offsets[group][topic]; !ok {
		c.offsets[group][topic] = make(map[int]uint64)
	}
	c.offsets[group][topic][partition] = offset
}

func (c *Coordinator) CommitOffset(group, topic string, partition int, offset uint64) error {
	util.Debug("Committing offset: group='%s', topic='%s', partition=%d, offset=%d",
		group, topic, partition, offset)

	offsetMsg := OffsetCommitMessage{
		Group:     group,
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		Timestamp: time.Now(),
	}

	payload, err := json.Marshal(offsetMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal offset commit: %w", err)
	}

	if err := c.offsetPublisher.Publish(c.offsetTopic, &types.Message{
		Payload: string(payload),
		Key:     fmt.Sprintf("%s-%s-%d", group, topic, partition),
	}); err != nil {
		return err
	}

	c.storeOffsetInMemory(group, topic, partition, offset)
	return nil
}

func (c *Coordinator) GetOffset(group, topic string, partition int) (uint64, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if topics, ok := c.offsets[group]; ok {
		if partitions, ok := topics[topic]; ok {
			if offset, ok := partitions[partition]; ok {
				return offset, nil
			}
		}
	}
	return 0, fmt.Errorf("no offset found for group '%s', topic '%s', partition %d", group, topic, partition)
}

// updateOffsetPartitionCount updates the number of partitions for the internal offset topic.
//
// IMPORTANT: this function DOES NOT acquire c.mu. Callers MUST hold the Coordinator lock
// (either c.mu.Lock() or c.mu.RLock() as appropriate) before calling this to avoid races.
// This change removes deadlock caused by nested locking when callers already hold the lock.
func (c *Coordinator) updateOffsetPartitionCount() {
	// NOTE: intentionally no locking here. Caller must hold lock.
	groupCount := len(c.groups)
	newCount := calculateOffsetPartitionCount(groupCount)

	if newCount != c.offsetTopicPartitionCount {
		c.offsetTopicPartitionCount = newCount
		c.offsetPublisher.CreateTopic(c.offsetTopic, newCount)
		util.Info("Updated offset topic partitions to %d for %d groups", newCount, groupCount)
	}
}

func (c *Coordinator) ValidateAndCommit(groupName, topic string, partition int, offset uint64, generation int, memberID string) error {
	c.mu.Lock()

	group := c.groups[groupName] // *GroupMetadata
	if group == nil {
		c.mu.Unlock()
		return fmt.Errorf("group not found")
	}

	member := group.Members[memberID]
	if member == nil {
		c.mu.Unlock()
		return fmt.Errorf("member not found")
	}

	if group.Generation != generation {
		c.mu.Unlock()
		return fmt.Errorf("generation mismatch")
	}

	if !contains(member.Assignments, partition) {
		c.mu.Unlock()
		return fmt.Errorf("not partition owner")
	}

	c.mu.Unlock()
	return c.CommitOffset(groupName, topic, partition, offset)
}

func (c *Coordinator) ValidateOwnershipAtomic(groupName, memberID string, generation int, partition int) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	group := c.GetGroup(groupName)
	if group == nil {
		util.Debug("failed to validate ownership for partition %d: Group '%s' not found.", partition, groupName)
		return false
	}

	member := group.Members[memberID]
	if member == nil {
		util.Debug("failed to validate ownership for partition %d: Member '%s' not found in group '%s'.", partition, memberID, groupName)
		return false
	}

	if group.Generation != generation {
		util.Debug("failed to validate ownership  for partition %d: Generation mismatch. Group Gen: %d, Request Gen: %d.", partition, group.Generation, generation)
		return false
	}

	isAssigned := false
	for _, assigned := range member.Assignments {
		if assigned == partition {
			isAssigned = true
			break
		}
	}

	if !isAssigned {
		util.Debug("failed to validate ownership for partition %d: Partition not assigned to member '%s'. Assignments: %v", partition, memberID, member.Assignments)
		return false
	}

	util.Debug("success to validate ownership for partition %d (Member: %s, Gen: %d).", partition, memberID, generation)
	return true
}

func (c *Coordinator) GetGroup(groupName string) *GroupMetadata {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.groups[groupName]
}

func (c *Coordinator) GetGeneration(groupName string) int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if group := c.groups[groupName]; group != nil {
		return group.Generation
	}
	return 0
}

func contains(slice []int, item int) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

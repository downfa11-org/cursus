package coordinator

import (
	"fmt"
	"sort"
	"time"

	"github.com/downfa11-org/go-broker/util"
)

// RegisterGroup creates a new consumer group for a topic.
func (c *Coordinator) RegisterGroup(topicName, groupName string, partitionCount int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if partitionCount <= 0 {
		return fmt.Errorf("invalid partition count: %d (must be > 0)", partitionCount)
	}

	if existing, exists := c.groups[groupName]; exists {
		if len(existing.Partitions) != partitionCount {
			return fmt.Errorf("group '%s' already exists with different partition count (existing: %d, requested: %d)", groupName, len(existing.Partitions), partitionCount)
		}
		util.Debug("Group '%s' already registered with same configuration", groupName)
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

	c.updateOffsetPartitionCount()
	util.Info("🆕 Group '%s' registered for topic '%s' with %d partitions", groupName, topicName, partitionCount)
	return nil
}

// AddConsumer registers a new consumer in the group and triggers a rebalance.
func (c *Coordinator) AddConsumer(groupName, consumerID string) ([]int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	group := c.groups[groupName]
	if group == nil {
		util.Error("❌ Consumer '%s' failed to join: group '%s' not found", consumerID, groupName)
		return nil, fmt.Errorf("group not found")
	}

	if existing, exists := group.Members[consumerID]; exists {
		util.Warn("⚠️ Consumer '%s' re-joining group '%s'. Overwriting previous session (was assigned: %v)", consumerID, groupName, existing.Assignments)
	}

	timeout := time.Duration(c.cfg.ConsumerSessionTimeoutMS) * time.Millisecond
	now := time.Now()
	for memberID, member := range group.Members {
		if now.Sub(member.LastHeartbeat) > timeout {
			delete(group.Members, memberID)
			util.Info("Removed inactive member %s before adding new consumer", memberID)
		}
	}

	util.Info("🚀 Consumer '%s' joining group '%s' (current members: %d)", consumerID, groupName, len(group.Members))

	group.Members[consumerID] = &MemberMetadata{
		ID:            consumerID,
		LastHeartbeat: time.Now(),
	}

	group.Generation++

	c.rebalanceRange(groupName)
	assignments := group.Members[consumerID].Assignments

	if len(assignments) == 0 {
		util.Warn("ℹ️ Consumer '%s' joined group '%s' but received no assignments (Total Partitions: %d, Active Members: %d)",
			consumerID, groupName, len(group.Partitions), len(group.Members))
	} else {
		util.Info("✅ Consumer '%s' joined group '%s' (Generation: %d, Assignments: %v)",
			consumerID, groupName, group.Generation, assignments)
	}

	return assignments, nil
}

// RemoveConsumer unregisters a consumer and triggers a rebalance.
func (c *Coordinator) RemoveConsumer(groupName, consumerID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	group := c.groups[groupName]
	if group == nil {
		util.Error("❌ Consumer '%s' failed to leave: group '%s' not found", consumerID, groupName)
		return fmt.Errorf("group not found")
	}

	util.Info("👋 Consumer '%s' leaving group '%s' (current members: %d)", consumerID, groupName, len(group.Members))

	delete(group.Members, consumerID)

	group.Generation++
	util.Info("⬆️ Group '%s' generation incremented to %d after member left", groupName, group.Generation)

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

// rebalanceRange redistributes partitions among consumers using range-based assignment.
func (c *Coordinator) rebalanceRange(groupName string) {
	group := c.groups[groupName]
	if group == nil {
		util.Error("❌ Cannot rebalance: group '%s' not found", groupName)
		return
	}

	util.Info("🔄 Starting rebalance for group '%s'", groupName)
	util.Info("Group '%s' has %d partitions and %d active members", groupName, len(group.Partitions), len(group.Members))

	members := make([]string, 0, len(group.Members))
	for id := range group.Members {
		members = append(members, id)
	}
	sort.Strings(members)

	if len(members) == 0 {
		util.Warn("⚠️ No active members in group '%s', skipping rebalance", groupName)
		return
	}

	partitionsPerConsumer := len(group.Partitions) / len(members)
	remainder := len(group.Partitions) % len(members)

	util.Debug("Using range strategy: %d partitions per consumer (base), %d consumers get +1 partition", partitionsPerConsumer, remainder)

	partitionIdx := 0
	for i, memberID := range members {
		count := partitionsPerConsumer
		if i < remainder {
			count++
		}

		var newAssignments []int
		if partitionIdx < len(group.Partitions) {
			end := partitionIdx + count
			if end > len(group.Partitions) {
				end = len(group.Partitions)
			}
			newAssignments = group.Partitions[partitionIdx:end]
		}

		oldAssignments := group.Members[memberID].Assignments
		group.Members[memberID].Assignments = newAssignments
		partitionIdx += len(newAssignments)

		util.Info("📋 Consumer '%s': assigned partitions %v (previously: %v)", memberID, newAssignments, oldAssignments)
	}

	util.Info("✅ Rebalance completed for group '%s'. Total members: %d, Total partitions: %d", groupName, len(members), len(group.Partitions))
}

package coordinator

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/downfa11-org/go-broker/pkg/types"
	"github.com/downfa11-org/go-broker/util"
)

func calculateOffsetPartitionCount(groupCount int) int {
	return min(max(groupCount/10, 4), 50)
}

func (c *Coordinator) storeOffsetInMemory(group, topic string, partition int, offset uint64) {
	if _, ok := c.offsets[group]; !ok {
		c.offsets[group] = make(map[string]map[int]uint64)
	}
	if _, ok := c.offsets[group][topic]; !ok {
		c.offsets[group][topic] = make(map[int]uint64)
	}
	c.offsets[group][topic][partition] = offset
}

func (c *Coordinator) CommitOffset(group, topic string, partition int, offset uint64) error {
	util.Debug("Committing offset: group='%s', topic='%s', partition=%d, offset=%d", group, topic, partition, offset)

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

	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.offsetPublisher.Publish(c.offsetTopic, &types.Message{
		Payload: string(payload),
		Key:     fmt.Sprintf("%s-%s-%d", group, topic, partition),
	}); err != nil {
		return err
	}

	c.storeOffsetInMemory(group, topic, partition, offset)
	return nil
}

func (c *Coordinator) CommitOffsetsBulk(group, topic string, offsets []OffsetItem) error {
	if offsets == nil {
		util.Debug("Skipping bulk offset commit: no offsets provided for group='%s', topic='%s'", group, topic)
		return nil
	}

	util.Debug("Committing bulk offsets: group='%s', topic='%s', count=%d", group, topic, len(offsets))

	bulkMsg := BulkOffsetMsg{
		Group:     group,
		Topic:     topic,
		Offsets:   offsets,
		Timestamp: time.Now(),
	}

	payload, err := json.Marshal(bulkMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal bulk offset commit: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.offsetPublisher.Publish(c.offsetTopic,
		&types.Message{
			Payload: string(payload),
			Key:     fmt.Sprintf("%s-%s-bulk", group, topic),
		}); err != nil {
		return err
	}

	if _, ok := c.offsets[group]; !ok {
		c.offsets[group] = make(map[string]map[int]uint64)
	}
	if _, ok := c.offsets[group][topic]; !ok {
		c.offsets[group][topic] = make(map[int]uint64)
	}

	for _, item := range offsets {
		c.offsets[group][topic][item.Partition] = item.Offset
	}

	return nil
}

func (c *Coordinator) ApplyOffsetUpdateFromFSM(group, topic string, offsets []OffsetItem) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if group == "" || topic == "" {
		return fmt.Errorf("invalid group or topic name")
	}

	if _, ok := c.offsets[group]; !ok {
		c.offsets[group] = make(map[string]map[int]uint64)
	}
	if _, ok := c.offsets[group][topic]; !ok {
		c.offsets[group][topic] = make(map[int]uint64)
	}

	for _, item := range offsets {
		c.offsets[group][topic][item.Partition] = item.Offset
	}

	return nil
}

func (c *Coordinator) GetOffset(group, topic string, partition int) (uint64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if topics, ok := c.offsets[group]; ok {
		if partitions, ok := topics[topic]; ok {
			if offset, ok := partitions[partition]; ok {
				return offset, true
			}
		}
	}
	return 0, false
}

// updateOffsetPartitionCount updates the number of partitions for the internal offset topic.
func (c *Coordinator) updateOffsetPartitionCount() {
	c.mu.Lock()
	groupCount := len(c.groups)
	currentPartitionCount := c.offsetTopicPartitionCount
	c.mu.Unlock()

	newCount := calculateOffsetPartitionCount(groupCount)

	if newCount != currentPartitionCount {
		c.mu.Lock()
		c.offsetTopicPartitionCount = newCount
		c.mu.Unlock()

		c.offsetPublisher.CreateTopic(c.offsetTopic, newCount)
		util.Info("Updated offset topic partitions to %d for %d groups", newCount, groupCount)
	}
}

func (c *Coordinator) ValidateAndCommit(groupName, topic string, partition int, offset uint64, generation int, memberID string) error {
	group := c.groups[groupName] // *GroupMetadata
	if group == nil {
		return fmt.Errorf("group not found")
	}

	member := group.Members[memberID]
	if member == nil {
		return fmt.Errorf("member not found")
	}

	if group.Generation != generation {
		return fmt.Errorf("generation mismatch")
	}

	if !contains(member.Assignments, partition) {
		return fmt.Errorf("not partition owner")
	}

	c.storeOffsetInMemory(groupName, topic, partition, offset)

	offsetMsg := OffsetCommitMessage{
		Group:     groupName,
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		Timestamp: time.Now(),
	}

	payload, err := json.Marshal(offsetMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal offset commit: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.offsetPublisher.Publish(c.offsetTopic,
		&types.Message{
			Payload: string(payload),
			Key:     fmt.Sprintf("%s-%s-%d", groupName, topic, partition),
		}); err != nil {
		return err
	}

	return nil
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

	return true
}

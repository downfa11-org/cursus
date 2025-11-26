package coordinator

import (
	"sort"

	"github.com/downfa11-org/go-broker/util"
)

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

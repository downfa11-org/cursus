package coordinator

import (
	"log"
	"sort"
)

func (c *Coordinator) rebalanceRange(groupName string) {
	group := c.groups[groupName]
	if group == nil {
		log.Printf("[REBALANCE_ERROR] ❌ Cannot rebalance: group '%s' not found", groupName)
		return
	}

	log.Printf("[REBALANCE_START] 🔄 Starting rebalance for group '%s'", groupName)
	log.Printf("[REBALANCE_INFO] Group '%s' has %d partitions and %d active members", groupName, len(group.Partitions), len(group.Members))

	members := make([]string, 0, len(group.Members))
	for id := range group.Members {
		members = append(members, id)
	}
	sort.Strings(members)

	if len(members) == 0 {
		log.Printf("[REBALANCE_WARN] ⚠️ No active members in group '%s', skipping rebalance", groupName)
		return
	}

	partitionsPerConsumer := len(group.Partitions) / len(members)
	remainder := len(group.Partitions) % len(members)

	log.Printf("[REBALANCE_STRATEGY] Using range strategy: %d partitions per consumer (base), %d consumers get +1 partition",
		partitionsPerConsumer, remainder)

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

		log.Printf("[REBALANCE_ASSIGN] 📋 Consumer '%s': assigned partitions %v (previously: %v)",
			memberID, newAssignments, oldAssignments)
	}

	log.Printf("[REBALANCE_COMPLETE] ✅ Rebalance completed for group '%s'. Total members: %d, Total partitions: %d",
		groupName, len(members), len(group.Partitions))
}

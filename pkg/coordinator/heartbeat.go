package coordinator

import (
	"fmt"
	"time"

	"github.com/downfa11-org/go-broker/util"
)

// monitorHeartbeats checks consumer heartbeat intervals and triggers rebalancing when timeouts occur.
func (c *Coordinator) monitorHeartbeats() {
	checkInterval := time.Duration(c.cfg.ConsumerHeartbeatCheckMS) * time.Millisecond
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.mu.Lock()
			for groupName, group := range c.groups {
				for memberID, member := range group.Members {
					timeout := time.Duration(c.cfg.ConsumerSessionTimeoutMS) * time.Millisecond
					timeSinceLastHeartbeat := time.Since(member.LastHeartbeat)

					if timeSinceLastHeartbeat > timeout {
						util.Error("⚠️ Consumer '%s' in group '%s' timed out (last heartbeat: %v ago, timeout: %v)",
							memberID, groupName, timeSinceLastHeartbeat, timeout)
						util.Debug("🔄 Triggering rebalance for group '%s' due to consumer '%s' timeout",
							groupName, memberID)

						delete(group.Members, memberID)

						group.Generation++
						util.Debug("⬆️ Group '%s' generation incremented to %d due to consumer '%s' timeout", groupName, group.Generation, memberID)

						c.triggerRebalance(groupName)

						util.Debug("❌ Consumer '%s' removed from group '%s'. Remaining members: %d",
							memberID, groupName, len(group.Members))
					} else {
						util.Debug("✅ Consumer '%s' in group '%s' is healthy (last heartbeat: %v ago)",
							memberID, groupName, timeSinceLastHeartbeat)
					}
				}
			}
			c.mu.Unlock()
		case <-c.stopCh:
			return
		}
	}
}

// triggerRebalance invokes the range-based rebalance strategy.
func (c *Coordinator) triggerRebalance(groupName string) {
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

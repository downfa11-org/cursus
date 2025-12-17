package controller

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/downfa11-org/go-broker/util"
)

// handleHelp processes HELP command
func (ch *CommandHandler) handleHelp() string {
	return `Available commands:  
CREATE topic=<name> [partitions=<N>] - create topic (default=4)  
DELETE topic=<name> - delete topic  
LIST - list all topics  
PUBLISH topic=<name> acks=<0|1> message=<text> producerId=<id> [seqNum=<N> epoch=<N>] - publish message
CONSUME topic=<name> partition=<N> offset=<N> group=<name> [autoOffsetReset=<earliest|latest>] - consume messages  
JOIN_GROUP topic=<name> group=<name> member=<id> - join consumer group  
SYNC_GROUP topic=<name> group=<name> member=<id> generation=<N> - sync group assignments  
LEAVE_GROUP group=<name> member=<id> - leave consumer group  
HEARTBEAT topic=<name> group=<name> member=<id> - send heartbeat  
COMMIT_OFFSET topic=<name> partition=<N> group=<name> offset=<N> - commit offset  
FETCH_OFFSET topic=<name> partition=<N> group=<name> - fetch committed offset  
REGISTER_GROUP topic=<name> group=<name> - register consumer group  
GROUP_STATUS group=<name> - get group status  
HELP - show this help  
EXIT - exit`
}

// handleCreate processes CREATE command
func (ch *CommandHandler) handleCreate(cmd string) string {
	args := parseKeyValueArgs(cmd[7:])
	topicName, ok := args["topic"]
	if !ok || topicName == "" {
		return "ERROR: missing topic parameter. Expected: CREATE topic=<name> [partitions=<N>]"
	}

	partitions := 4 // default
	if partStr, ok := args["partitions"]; ok {
		n, err := strconv.Atoi(partStr)
		if err != nil || n <= 0 {
			return "ERROR: partitions must be a positive integer"
		}
		partitions = n
	}

	tm := ch.TopicManager

	if ch.Config.EnabledDistribution && ch.Cluster.RaftManager != nil {
		if resp, forwarded, _ := ch.isLeaderAndForward(cmd); forwarded {
			return resp
		}

		topicData := map[string]interface{}{
			"name":       topicName,
			"partitions": partitions,
		}
		data, _ := json.Marshal(topicData)

		err := ch.Cluster.RaftManager.ApplyCommand("TOPIC", data)
		if err != nil {
			return fmt.Sprintf("ERROR: failed to replicate topic: %v", err)
		}

		const maxWaitRetries = 10
		const waitDelay = 200 * time.Millisecond

		for i := 0; i < maxWaitRetries; i++ {
			if tm.GetTopic(topicName) != nil {
				util.Debug("Topic '%s' successfully applied to FSM after %d attempts.", topicName, i+1)
				break
			}
			if i == maxWaitRetries-1 {
				return fmt.Sprintf("ERROR: topic '%s' creation timed out (FSM application failed)", topicName)
			}
			time.Sleep(waitDelay)
		}
	} else {
		tm.CreateTopic(topicName, partitions)
	}

	t := tm.GetTopic(topicName)
	if ch.Coordinator != nil {
		err := ch.Coordinator.RegisterGroup(topicName, "default-group", partitions)
		if err != nil {
			util.Warn("Failed to register default group with coordinator: %v", err)
		}
	}
	return fmt.Sprintf("✅ Topic '%s' now has %d partitions", topicName, len(t.Partitions))
}

// handleDelete processes DELETE command
func (ch *CommandHandler) handleDelete(cmd string) string {
	args := parseKeyValueArgs(cmd[7:])
	topicName, ok := args["topic"]
	if !ok || topicName == "" {
		return "ERROR: missing topic parameter. Expected: DELETE topic=<name>"
	}

	if ch.Config.EnabledDistribution && ch.Cluster.RaftManager != nil {
		if resp, forwarded, _ := ch.isLeaderAndForward(cmd); forwarded {
			return resp
		}

		err := ch.Cluster.RaftManager.ApplyCommand("TOPIC_DELETE", []byte(topicName))
		if err != nil {
			return fmt.Sprintf("ERROR: %v", err)
		}
		return fmt.Sprintf("🗑️ Topic '%s' deleted across cluster", topicName)
	}

	if ch.TopicManager.DeleteTopic(topicName) {
		return fmt.Sprintf("🗑️ Topic '%s' deleted", topicName)
	}
	return fmt.Sprintf("ERROR: topic '%s' not found", topicName)
}

// handleList processes LIST command
func (ch *CommandHandler) handleList() string {
	tm := ch.TopicManager
	names := tm.ListTopics()
	if len(names) == 0 {
		return "(no topics)"
	}
	return strings.Join(names, ", ")
}

// handleRegisterGroup processes REGISTER_GROUP command
func (ch *CommandHandler) handleRegisterGroup(cmd string) string {
	args := parseKeyValueArgs(cmd[15:])
	topicName, ok := args["topic"]
	if !ok || topicName == "" {
		return "ERROR: REGISTER_GROUP requires topic parameter"
	}
	groupName, ok := args["group"]
	if !ok || groupName == "" {
		return "ERROR: REGISTER_GROUP requires group parameter"
	}

	t := ch.TopicManager.GetTopic(topicName)
	if t == nil {
		util.Warn("ch registerGroup: topic '%s' does not exist", topicName)
		return fmt.Sprintf("ERROR: topic '%s' does not exist", topicName)
	}

	if ch.Coordinator != nil {
		if err := ch.Coordinator.RegisterGroup(topicName, groupName, len(t.Partitions)); err != nil {
			return fmt.Sprintf("ERROR: %v", err)
		}
		return fmt.Sprintf("✅ Group '%s' registered for topic '%s'", groupName, topicName)
	}
	return "ERROR: coordinator not available"
}

// handleJoinGroup processes JOIN_GROUP command
func (ch *CommandHandler) handleJoinGroup(cmd string, ctx *ClientContext) string {
	args := parseKeyValueArgs(cmd[11:])

	topicName, ok := args["topic"]
	if !ok || topicName == "" {
		return "ERROR: JOIN_GROUP requires topic parameter"
	}
	groupName, ok := args["group"]
	if !ok || groupName == "" {
		return "ERROR: JOIN_GROUP requires group parameter"
	}
	consumerID, ok := args["member"]
	if !ok || consumerID == "" {
		return "ERROR: JOIN_GROUP requires member parameter"
	}

	n, err := rand.Int(rand.Reader, big.NewInt(10000))
	var randSuffix string
	if err != nil {
		util.Warn("Failed to generate random consumer suffix, falling back to time-based value: %v", err)
		randSuffix = fmt.Sprintf("%04d", time.Now().UnixNano()%10000)
	} else {
		randSuffix = fmt.Sprintf("%04d", n.Int64())
	}
	consumerID = fmt.Sprintf("%s-%s", consumerID, randSuffix)

	if ch.Config.EnabledDistribution && ch.Cluster.RaftManager != nil {
		if resp, forwarded, _ := ch.isLeaderAndForward(cmd); forwarded {
			return resp
		}

		joinData := map[string]interface{}{
			"type":   "JOIN",
			"group":  groupName,
			"member": consumerID,
			"topic":  topicName,
		}
		data, _ := json.Marshal(joinData)

		err = ch.Cluster.RaftManager.ApplyCommand("GROUP_SYNC", data)
		if err != nil {
			return fmt.Sprintf("ERROR: failed to replicate group operation: %v", err)
		}
	} else {
		if ch.Coordinator != nil {
			_, err := ch.Coordinator.AddConsumer(groupName, consumerID)
			if err != nil {
				util.Error("failed to join %s: %v", groupName, err)
			}
		} else {
			return "ERROR: coordinator not available"
		}
	}

	ctx.MemberID = consumerID
	ctx.Generation = ch.Coordinator.GetGeneration(groupName)
	assignments := ch.Coordinator.GetAssignments(groupName)[consumerID]
	util.Debug("✅ Joined group '%s' member '%s' generation '%d' with partitions: %v", groupName, ctx.MemberID, ctx.Generation, assignments)
	return fmt.Sprintf("OK generation=%d member=%s assignments=%v", ctx.Generation, ctx.MemberID, assignments)
}

// handleSyncGroup processes SYNC_GROUP command
func (ch *CommandHandler) handleSyncGroup(cmd string) string {
	args := parseKeyValueArgs(cmd[11:])

	topicName, ok := args["topic"]
	if !ok || topicName == "" {
		return "ERROR: SYNC_GROUP requires topic parameter"
	}
	groupName, ok := args["group"]
	if !ok || groupName == "" {
		return "ERROR: SYNC_GROUP requires group parameter"
	}
	memberID, ok := args["member"]
	if !ok || memberID == "" {
		return "ERROR: SYNC_GROUP requires member parameter"
	}

	if ch.Coordinator == nil {
		return "ERROR: coordinator not available"
	}

	if ch.Config.EnabledDistribution && ch.Cluster.RaftManager != nil {
		if resp, forwarded, _ := ch.isLeaderAndForward(cmd); forwarded {
			return resp
		}
	}

	assignments := ch.Coordinator.GetAssignments(groupName)
	if _, exists := assignments[memberID]; !exists {
		return fmt.Sprintf("ERROR: member %s not found in group", memberID)
	}

	memberAssignments := assignments[memberID]
	return fmt.Sprintf("OK assignments=%v", memberAssignments)
}

// handleLeaveGroup processes LEAVE_GROUP command
func (ch *CommandHandler) handleLeaveGroup(cmd string) string {
	args := parseKeyValueArgs(cmd[12:])

	topicName, ok := args["topic"]
	if !ok || topicName == "" {
		return "ERROR: LEAVE_GROUP requires topic parameter"
	}
	groupName, ok := args["group"]
	if !ok || groupName == "" {
		return "ERROR: LEAVE_GROUP requires group parameter"
	}
	consumerID, ok := args["member"]
	if !ok || consumerID == "" {
		return "ERROR: LEAVE_GROUP requires member parameter"
	}

	if groupName == "" || consumerID == "" {
		return "ERROR: LEAVE_GROUP requires group and member parameters"
	}

	if ch.Config.EnabledDistribution && ch.Cluster.RaftManager != nil {
		if resp, forwarded, _ := ch.isLeaderAndForward(cmd); forwarded {
			return resp
		}

		leaveData := map[string]interface{}{
			"type":   "LEAVE",
			"group":  groupName,
			"member": consumerID,
		}
		data, _ := json.Marshal(leaveData)

		err := ch.Cluster.RaftManager.ApplyCommand("GROUP_SYNC", data)
		if err != nil {
			return fmt.Sprintf("ERROR: failed to replicate leave operation: %v", err)
		}
	} else {
		if ch.Coordinator != nil {
			if err := ch.Coordinator.RemoveConsumer(groupName, consumerID); err != nil {
				return fmt.Sprintf("ERROR: %v", err)
			}
		}
	}
	return fmt.Sprintf("✅ Left group '%s'", groupName)
}

// handleFetchOffset processes FETCH_OFFSET command
func (ch *CommandHandler) handleFetchOffset(cmd string) string {
	args := parseKeyValueArgs(cmd[13:])

	topicName, ok := args["topic"]
	if !ok || topicName == "" {
		return "ERROR: FETCH_OFFSET requires topic parameter"
	}
	partitionStr, ok := args["partition"]
	if !ok || partitionStr == "" {
		return "ERROR: FETCH_OFFSET requires partition parameter"
	}
	partition, err := strconv.Atoi(partitionStr)
	if err != nil {
		return "ERROR: invalid partition"
	}
	groupName, ok := args["group"]
	if !ok || groupName == "" {
		return "ERROR: FETCH_OFFSET requires group parameter"
	}

	if ch.Config.EnabledDistribution && ch.Cluster.RaftManager != nil {
		if resp, forwarded, _ := ch.isLeaderAndForward(cmd); forwarded {
			return resp
		}
		if !ch.isAuthorizedForPartition(topicName, partition) {
			return fmt.Sprintf("ERROR: NOT_AUTHORIZED_FOR_PARTITION %s:%d", topicName, partition)
		}
	}

	if ch.Coordinator != nil {
		offset, err := ch.Coordinator.GetOffset(groupName, topicName, partition)
		if err != nil {
			return fmt.Sprintf("ERROR: %v", err)
		} else {
			return fmt.Sprintf("%d", offset)
		}
	} else {
		return "ERROR: offset manager not available"
	}
}

// handleGroupStatus processes GROUP_STATUS command
func (ch *CommandHandler) handleGroupStatus(cmd string) string {
	args := parseKeyValueArgs(cmd[13:])
	groupName, ok := args["group"]
	if !ok || groupName == "" {
		return "ERROR: GROUP_STATUS requires group parameter"
	}

	if ch.Coordinator == nil {
		return "ERROR: coordinator not available"
	}

	if ch.Config.EnabledDistribution && ch.Cluster.RaftManager != nil {
		if resp, forwarded, _ := ch.isLeaderAndForward(cmd); forwarded {
			return resp
		}
	}

	status, err := ch.Coordinator.GetGroupStatus(groupName)
	if err != nil {
		return fmt.Sprintf("ERROR: %v", err)
	}

	statusJSON, err := json.Marshal(status)
	if err != nil {
		return fmt.Sprintf("ERROR: failed to marshal status: %v", err)
	}
	return string(statusJSON)
}

// handleHeartbeat processes HEARTBEAT command
func (ch *CommandHandler) handleHeartbeat(cmd string) string {
	args := parseKeyValueArgs(cmd[10:])

	topicName, ok := args["topic"]
	if !ok || topicName == "" {
		return "ERROR: HEARTBEAT requires topic parameter"
	}
	groupName, ok := args["group"]
	if !ok || groupName == "" {
		return "ERROR: HEARTBEAT requires group parameter"
	}
	consumerID, ok := args["member"]
	if !ok || consumerID == "" {
		return "ERROR: HEARTBEAT requires member parameter"
	}

	if ch.Config.EnabledDistribution && ch.Cluster.RaftManager != nil {
		if resp, forwarded, _ := ch.isLeaderAndForward(cmd); forwarded {
			return resp
		}
	}

	if ch.Coordinator != nil {
		err := ch.Coordinator.RecordHeartbeat(groupName, consumerID)
		if err != nil {
			return fmt.Sprintf("ERROR: %v", err)
		} else {
			return "OK"
		}
	} else {
		return "ERROR: coordinator not available"
	}
}

// handleCommitOffset processes COMMIT_OFFSET command
func (ch *CommandHandler) handleCommitOffset(cmd string) string {
	args := parseKeyValueArgs(cmd[14:])

	topicName, ok := args["topic"]
	if !ok || topicName == "" {
		return "ERROR: COMMIT_OFFSET requires topic parameter"
	}
	partitionStr, ok := args["partition"]
	if !ok || partitionStr == "" {
		return "ERROR: COMMIT_OFFSET requires partition parameter"
	}
	partition, err := strconv.Atoi(partitionStr)
	if err != nil {
		return "ERROR: invalid partition"
	}
	groupID, ok := args["group"]
	if !ok || groupID == "" {
		return "ERROR: COMMIT_OFFSET requires groupID parameter"
	}
	offsetStr, ok := args["offset"]
	if !ok || offsetStr == "" {
		return "ERROR: COMMIT_OFFSET requires offset parameter"
	}
	offset, err := strconv.ParseUint(offsetStr, 10, 64)
	if err != nil {
		return "ERROR: invalid offset"
	}

	if ch.Config.EnabledDistribution && ch.Cluster.RaftManager != nil {
		if resp, forwarded, _ := ch.isLeaderAndForward(cmd); forwarded {
			return resp
		}

		if !ch.isAuthorizedForPartition(topicName, partition) {
			return fmt.Sprintf("ERROR: NOT_AUTHORIZED_FOR_PARTITION %s:%d", topicName, partition)
		}

		commitData := map[string]interface{}{
			"type":      "COMMIT",
			"group":     args["group"],
			"topic":     args["topic"],
			"partition": partition,
			"offset":    offset,
		}
		data, _ := json.Marshal(commitData)

		err = ch.Cluster.RaftManager.ApplyCommand("OFFSET_SYNC", data)
		if err != nil {
			return fmt.Sprintf("ERROR: offset replication failed: %v", err)
		}
		return "OK"
	}

	if ch.Coordinator != nil {
		err := ch.Coordinator.CommitOffset(groupID, topicName, partition, offset)
		if err != nil {
			return fmt.Sprintf("ERROR: %v", err)
		} else {
			return "OK"
		}
	}
	return "ERROR: offset manager not available"
}

// resolveOffset determines the starting offset for a consumer
func (ch *CommandHandler) resolveOffset(topicName string, partition int, requestedOffset uint64, groupName string, autoOffsetReset string) (uint64, error) {
	dh, err := ch.DiskManager.GetHandler(topicName, partition)
	if err != nil {
		return 0, fmt.Errorf("failed to get disk handler: %w", err)
	}

	actualOffset := requestedOffset

	if requestedOffset == 0 {
		if ch.Coordinator != nil {
			savedOffset, err := ch.Coordinator.GetOffset(groupName, topicName, partition)
			if err == nil {
				actualOffset = savedOffset
				util.Debug("Saved offset %d for group '%s'", actualOffset, groupName)
				return actualOffset, nil
			} else {
				util.Debug("💾 No saved offset found for group '%s', using reset policy", groupName)
			}
		}

		if strings.ToLower(autoOffsetReset) == "latest" {
			latest, err := dh.GetLatestOffset()
			if err != nil {
				util.Warn("Failed to get latest offset, defaulting to 0: %v", err)
				actualOffset = 0
			} else {
				actualOffset = latest
			}
			util.Debug("Using latest offset %d for group '%s'", actualOffset, groupName)
		} else {
			actualOffset = 0
			util.Debug("Using earliest offset 0 for group '%s'", groupName)
		}
	} else {
		util.Debug("Using explicitly requested offset %d for group '%s'", requestedOffset, groupName)
	}

	return actualOffset, nil
}

func (ch *CommandHandler) ValidateOwnership(groupName, memberID string, generation int, partition int) bool {
	if ch.Coordinator == nil {
		util.Debug("failed to validate ownership: Coordinator is nil.")
		return false
	}

	return ch.Coordinator.ValidateOwnershipAtomic(groupName, memberID, generation, partition)
}

package controller

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/downfa11-org/go-broker/pkg/types"
	"github.com/downfa11-org/go-broker/util"
)

// handleHelp processes HELP command
func (ch *CommandHandler) handleHelp() string {
	return `Available commands:  
CREATE topic=<name> [partitions=<N>] - create topic (default=4)  
DELETE topic=<name> - delete topic  
LIST - list all topics  
PUBLISH topic=<name> acks=<0|1> message=<text> [producerId=<id> seqNum=<N> epoch=<N>] - publish message  
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
	tm.CreateTopic(topicName, partitions)
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

	tm := ch.TopicManager
	if tm.DeleteTopic(topicName) {
		return fmt.Sprintf("🗑️ Topic '%s' deleted", topicName)
	} else {
		return fmt.Sprintf("ERROR: topic '%s' not found", topicName)
	}
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

// handlePublish processes PUBLISH command
func (ch *CommandHandler) handlePublish(cmd string) string {
	args := parseKeyValueArgs(cmd[8:])
	var err error

	topicName, ok := args["topic"]
	if !ok || topicName == "" {
		return "ERROR: missing topic parameter"
	}

	message, ok := args["message"]
	if !ok || message == "" {
		return "ERROR: missing message parameter"
	}

	producerID, ok := args["producerId"]
	if !ok || producerID == "" {
		return "ERROR: missing producerID parameter"
	}

	var seqNum uint64
	if seqNumStr, ok := args["seqNum"]; ok {
		seqNum, err = strconv.ParseUint(seqNumStr, 10, 64)
		if err != nil {
			return fmt.Sprintf("ERROR: invalid seqNum: %v", err)
		}
	}

	var epoch int64
	if epochStr, ok := args["epoch"]; ok {
		epoch, err = strconv.ParseInt(epochStr, 10, 64)
		if err != nil {
			return fmt.Sprintf("ERROR: invalid epoch: %v", err)
		}
	}

	tm := ch.TopicManager
	t := tm.GetTopic(topicName)
	if t == nil {
		return fmt.Sprintf("ERROR: topic '%s' does not exist", topicName)
	}

	msg := &types.Message{
		Payload:    message,
		ProducerID: producerID,
		SeqNum:     seqNum,
		Epoch:      epoch,
	}

	if args["acks"] == "1" {
		err = tm.PublishWithAck(topicName, msg) // sync
	} else {
		err = tm.Publish(topicName, msg) // async
	}

	if err != nil {
		return fmt.Sprintf("ERROR: %v", err)
	}

	ackResp := types.AckResponse{
		Status:        "OK",
		LastOffset:    msg.Offset,
		ProducerEpoch: epoch,
		ProducerID:    producerID,
		SeqStart:      seqNum,
		SeqEnd:        seqNum,
	}
	respBytes, _ := json.Marshal(ackResp)
	return string(respBytes)
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
		return fmt.Sprintf("ERROR: topic '%s' does not exist", topicName)
	}

	if ch.Coordinator != nil {
		if err := ch.Coordinator.RegisterGroup(topicName, groupName, len(t.Partitions)); err != nil {
			return fmt.Sprintf("ERROR: %v", err)
		} else {
			return fmt.Sprintf("✅ Group '%s' registered for topic '%s'", groupName, topicName)
		}
	} else {
		return "ERROR: coordinator not available"
	}
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

	if ch.Coordinator == nil {
		return "ERROR: coordinator not available"
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

	assignments, err := ch.Coordinator.AddConsumer(groupName, consumerID)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			t := ch.TopicManager.GetTopic(topicName)
			if t == nil {
				return fmt.Sprintf("ERROR: topic '%s' does not exist", topicName)
			}

			if regErr := ch.Coordinator.RegisterGroup(topicName, groupName, len(t.Partitions)); regErr != nil {
				util.Debug("register group error, %v", regErr)
				return fmt.Sprintf("ERROR: failed to create group: %v", regErr)
			}
			assignments, err = ch.Coordinator.AddConsumer(groupName, consumerID)
			if err != nil {
				return fmt.Sprintf("ERROR: failed to join after group creation: %v", err)
			}
		} else {
			return fmt.Sprintf("ERROR: %v", err)
		}
	}

	ctx.MemberID = consumerID
	ctx.Generation = ch.Coordinator.GetGeneration(groupName)

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

	if ch.Coordinator != nil {
		err := ch.Coordinator.RemoveConsumer(groupName, consumerID)
		if err != nil {
			return fmt.Sprintf("ERROR: %v", err)
		} else {
			return fmt.Sprintf("✅ Left group '%s'", groupName)
		}
	} else {
		return "ERROR: coordinator not available"
	}
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

	if ch.Coordinator != nil {
		err := ch.Coordinator.CommitOffset(groupID, topicName, partition, offset)
		if err != nil {
			return fmt.Sprintf("ERROR: %v", err)
		} else {
			return "OK"
		}
	} else {
		return "ERROR: offset manager not available"
	}
}

// resolveOffset determines the starting offset for a consumer
func (ch *CommandHandler) resolveOffset(
	topicName string,
	partition int,
	requestedOffset uint64,
	groupName string,
	autoOffsetReset string,
) (uint64, error) {
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
			}
			actualOffset = latest
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

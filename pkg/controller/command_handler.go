package controller

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math/big"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/downfa11-org/go-broker/pkg/types"
	"github.com/downfa11-org/go-broker/util"
	"github.com/hashicorp/raft"
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

	if ch.Config.EnabledDistribution && ch.ReplicationManager != nil {
		if ch.ReplicationManager.GetRaft().State() != raft.Leader {
			if ch.Router == nil {
				return "ERROR: not the leader, and router is nil"
			}

			const maxRetries = 5
			const retryDelay = 500 * time.Millisecond
			var leader string
			var err error

			encodedCmd := string(util.EncodeMessage(topicName, cmd))
			for i := 0; i < maxRetries; i++ {

				resp, forwardErr := ch.Router.ForwardToLeader(encodedCmd)
				if forwardErr == nil {
					return resp
				}

				leader, _ = ch.Router.GetCachedLeader()
				util.Debug("Failed to forward CREATE to leader (%s). Retrying (Attempt %d/%d). Error: %v", leader, i+1, maxRetries, forwardErr)

				if i < maxRetries-1 {
					time.Sleep(retryDelay)
				}
				err = forwardErr
			}
			return ch.errorResponse(fmt.Sprintf("failed to forward CREATE to leader: %v", err))
		}

		topicData := map[string]interface{}{
			"name":       topicName,
			"partitions": partitions,
		}
		data, _ := json.Marshal(topicData)

		future := ch.ReplicationManager.GetRaft().Apply(
			[]byte(fmt.Sprintf("TOPIC:%s", string(data))),
			5*time.Second,
		)

		if err := future.Error(); err != nil {
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

	tm := ch.TopicManager
	if tm.DeleteTopic(topicName) {
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

	acks, ok := args["acks"]
	if !ok || acks == "" {
		acks = "1"
	}

	acksLower := strings.ToLower(acks)
	if acksLower != "0" && acksLower != "1" && acksLower != "-1" && acksLower != "all" {
		return fmt.Sprintf("ERROR: invalid acks value: %s. Expected -1 (all), 0, or 1", acks)
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

	var ackResp types.AckResponse
	if ch.Config.EnabledDistribution && ch.Router != nil {
		if leader, err := ch.Router.GetCachedLeader(); err == nil && leader != "" {
			if leader != ch.Router.GetLocalAddr() {
				const maxRetries = 3
				const retryDelay = 200 * time.Millisecond
				var lastErr error

				encodedCmd := string(util.EncodeMessage(topicName, cmd))
				for i := 0; i < maxRetries; i++ {
					resp, forwardErr := ch.Router.ForwardToLeader(encodedCmd)
					if forwardErr == nil {
						return resp
					}

					util.Debug("Failed to forward PUBLISH to leader (%s). Retrying (Attempt %d/%d). Error: %v", leader, i+1, maxRetries, forwardErr)

					if i < maxRetries-1 {
						time.Sleep(retryDelay)
					}
					lastErr = forwardErr
				}
				return ch.errorResponse(fmt.Sprintf("failed to forward PUBLISH to leader after %d attempts: %v", maxRetries, lastErr))
			} else {
				util.Debug("Processing PUBLISH locally as leader: %s", leader)
			}
		}
	}

	if ch.Router != nil {
		util.Info("router nil.")
	}
	tm := ch.TopicManager
	t := tm.GetTopic(topicName)
	if t == nil {
		const maxRetries = 5
		const retryDelay = 100 * time.Millisecond

		util.Warn("Topic '%s' not found. Checking if creation is pending...", topicName)

		found := false
		for i := 0; i < maxRetries; i++ {
			t = tm.GetTopic(topicName)
			if t != nil {
				found = true
				break
			}
			time.Sleep(retryDelay)
		}

		if !found {
			util.Warn("ch publish: topic '%s' does not exist after retries", topicName)
			return fmt.Sprintf("ERROR: topic '%s' does not exist", topicName)
		}
	}

	msg := &types.Message{
		Payload:    message,
		ProducerID: producerID,
		SeqNum:     seqNum,
		Epoch:      epoch,
	}

	if ch.Config.EnabledDistribution && ch.ReplicationManager != nil {
		partition := ch.TopicManager.GetTopic(topicName).GetPartitionForMessage(*msg)
		messageData := map[string]interface{}{
			"topic":      topicName,
			"partition":  partition,
			"payload":    message,
			"producerId": producerID,
			"seqNum":     seqNum,
			"epoch":      epoch,
			"acks":       acks,
		}

		jsonData, _ := json.Marshal(messageData)
		timeout := 5 * time.Second
		if acks == "0" {
			timeout = 0
		}

		future := ch.ReplicationManager.GetRaft().Apply([]byte(fmt.Sprintf("MESSAGE:%s", jsonData)), timeout)
		if acks != "0" {
			if err := future.Error(); err != nil {
				return ch.errorResponse(fmt.Sprintf("failed to replicate message with acks=%s: %v", acks, err))
			}

			if ackResponse, ok := future.Response().(types.AckResponse); ok {
				ackResp = ackResponse
			} else {
				util.Warn("Raft FSM did not return a proper AckResponse for single message; using local message info.")
				ackResp = types.AckResponse{
					Status:        "ERROR",
					ErrorMsg:      "Raft FSM failed to return acknowledgement",
					LastOffset:    msg.Offset,
					ProducerEpoch: epoch,
					ProducerID:    producerID,
					SeqStart:      seqNum,
					SeqEnd:        seqNum,
				}

			}
			goto Respond
		}
	} else {
		switch acks {
		case "1", "-1", "all":
			err = ch.TopicManager.PublishWithAck(topicName, msg) // sync
		case "0":
			err = ch.TopicManager.Publish(topicName, msg) // async
		}

		if (acks == "-1" || acksLower == "all") && !ch.Config.EnabledDistribution {
			return ch.errorResponse("acks=-1 requires cluster")
		}

		if err != nil {
			return ch.errorResponse(fmt.Sprintf("publish failed: %v", err))
		}
	}

	if err != nil {
		return ch.errorResponse(fmt.Sprintf("%v", err))
	}

	if acks == "0" {
		return "OK"
	}

	ackResp = types.AckResponse{
		Status:        "OK",
		LastOffset:    msg.Offset,
		ProducerEpoch: epoch,
		ProducerID:    producerID,
		SeqStart:      seqNum,
		SeqEnd:        seqNum,
	}

Respond:
	if ch.Config.EnabledDistribution && ch.Router != nil {
		if leader, err := ch.Router.GetCachedLeader(); err == nil && leader != "" {
			if leader != ch.Router.GetLocalAddr() {
				ackResp.Leader = leader
			}
		}
	}

	respBytes, err := json.Marshal(ackResp)
	if err != nil {
		return fmt.Sprintf("ERROR: failed to marshal response: %v", err)
	}
	return string(respBytes)
}

// HandleBatchMessage processes PUBLISH of multiple messages.
func (ch *CommandHandler) HandleBatchMessage(data []byte, conn net.Conn) (string, error) {
	batch, err := util.DecodeBatchMessages(data)
	if err != nil {
		util.Error("Batch message decoding failed: %v", err)
		return fmt.Sprintf("ERROR: %v", err), nil
	}

	if len(batch.Messages) == 0 {
		return "ERROR: empty batch", nil
	}

	acks := batch.Acks
	if acks == "" {
		acks = "1"
	}

	acksLower := strings.ToLower(acks)
	if acksLower != "0" && acksLower != "1" && acksLower != "-1" && acksLower != "all" {
		return fmt.Sprintf("ERROR: invalid acks value in batch: %s. Expected -1 (all), 0, or 1", acks), nil
	}

	var respAck types.AckResponse
	var lastMsg *types.Message
	if ch.Config.EnabledDistribution && ch.Router != nil {
		if leader, err := ch.Router.GetCachedLeader(); err == nil && leader != "" {
			if leader != ch.Router.GetLocalAddr() {
				const maxRetries = 3
				const retryDelay = 200 * time.Millisecond
				var lastErr error

				for i := 0; i < maxRetries; i++ {
					resp, forwardErr := ch.Router.ForwardDataToLeader(data)
					if forwardErr == nil {
						return resp, nil
					}

					util.Debug("Failed to forward BATCH to leader (%s). Retrying (Attempt %d/%d). Error: %v", leader, i+1, maxRetries, forwardErr)

					if i < maxRetries-1 {
						time.Sleep(retryDelay)
					}
					lastErr = forwardErr
				}

				return ch.errorResponse(fmt.Sprintf("failed to forward BATCH to leader after %d attempts: %v", maxRetries, lastErr)), nil
			} else {
				util.Debug("Processing BATCH locally as leader: %s", leader)
			}
		}
	}

	if ch.Config.EnabledDistribution && ch.ReplicationManager != nil {
		batchData, _ := json.Marshal(batch)
		timeout := 5 * time.Second

		if acks == "0" {
			timeout = 0
		}

		future := ch.ReplicationManager.GetRaft().Apply([]byte(fmt.Sprintf("BATCH:%s", batchData)), timeout)
		if acks != "0" {
			if err := future.Error(); err != nil {
				return ch.errorResponse(fmt.Sprintf("failed to replicate batch message with acks=%s: %v", acks, err)), nil
			}

			tempMsg := batch.Messages[len(batch.Messages)-1]
			lastMsg = &tempMsg

			if ack, ok := future.Response().(types.AckResponse); ok {
				respAck = ack
				util.Debug("Raft FSM returned AckResponse.")
			} else {
				respAck = types.AckResponse{
					Status:        "ERROR",
					ErrorMsg:      "Raft FSM failed to return acknowledgement",
					ProducerID:    lastMsg.ProducerID,
					ProducerEpoch: lastMsg.Epoch,
					SeqStart:      batch.Messages[0].SeqNum,
					SeqEnd:        lastMsg.SeqNum,
				}
				util.Warn("Raft FSM did not return a proper AckResponse for BATCH; using local message info.")
			}
			goto Respond
		} else {
			return "OK", nil
		}
	}

	switch acks {
	case "1", "-1", "all":
		err = ch.TopicManager.PublishBatchSync(batch.Topic, batch.Messages)
	case "0":
		err = ch.TopicManager.PublishBatchAsync(batch.Topic, batch.Messages)
	}

	if (acks == "-1" || acksLower == "all") && !ch.Config.EnabledDistribution {
		return "ERROR: acks=-1 requires distributed clustering to be enabled", nil
	}

	if err != nil {
		return fmt.Sprintf("ERROR: publish batch failed: %v", err), nil
	}

	if acks == "0" {
		return "OK", nil
	}

	lastMsg = &batch.Messages[len(batch.Messages)-1]

	respAck = types.AckResponse{
		Status:        "OK",
		LastOffset:    lastMsg.Offset,
		SeqStart:      batch.Messages[0].SeqNum,
		SeqEnd:        lastMsg.SeqNum,
		ProducerID:    lastMsg.ProducerID,
		ProducerEpoch: lastMsg.Epoch,
	}

Respond:
	if ch.Config.EnabledDistribution && ch.Router != nil {
		if leader, err := ch.Router.GetCachedLeader(); err == nil && leader != "" {
			if leader != ch.Router.GetLocalAddr() {
				respAck.Leader = leader
			}
		}
	}

	ackBytes, err := json.Marshal(respAck)
	if err != nil {
		util.Error("Failed to marshal AckResponse: %v", err)
		return "ERROR: internal marshal error", nil
	}
	return string(ackBytes), nil
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
				util.Warn("ch joinGroup: topic '%s' does not exist", topicName)
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

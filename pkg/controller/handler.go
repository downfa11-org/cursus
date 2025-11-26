package controller

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/downfa11-org/go-broker/pkg/config"
	"github.com/downfa11-org/go-broker/pkg/coordinator"
	"github.com/downfa11-org/go-broker/pkg/disk"
	"github.com/downfa11-org/go-broker/pkg/offset"
	"github.com/downfa11-org/go-broker/pkg/topic"
	"github.com/downfa11-org/go-broker/pkg/types"
	"github.com/downfa11-org/go-broker/util"
)

const DefaultMaxPollRecords = 8192
const STREAM_DATA_SIGNAL = "STREAM_DATA"
const AutoOffset = ^uint64(0)

type CommandHandler struct {
	TopicManager  *topic.TopicManager
	DiskManager   *disk.DiskManager
	Config        *config.Config
	OffsetManager *offset.OffsetManager
	Coordinator   *coordinator.Coordinator
}

type ConsumeArgs struct {
	Topic     string
	Partition int
	Offset    uint64
}

func NewCommandHandler(tm *topic.TopicManager, dm *disk.DiskManager, cfg *config.Config, om *offset.OffsetManager, cd *coordinator.Coordinator) *CommandHandler {
	return &CommandHandler{
		TopicManager:  tm,
		DiskManager:   dm,
		Config:        cfg,
		OffsetManager: om,
		Coordinator:   cd,
	}
}

func (ch *CommandHandler) logCommandResult(cmd, response string) {
	status := "SUCCESS"
	if strings.HasPrefix(response, "ERROR:") {
		status = "FAILURE"
	}

	cleanResponse := strings.ReplaceAll(response, "\n", " ")
	util.Info("%s - [%s] to Response [%s]", status, cmd, cleanResponse)
}

// HandleConsumeCommand is responsible for parsing the CONSUME command and streaming messages.
func (ch *CommandHandler) HandleConsumeCommand(conn net.Conn, rawCmd string, ctx *ClientContext) (int, error) {
	// CONSUME topic=<name> partition=<N> offset=<N> group=<name> [autoOffsetReset=<earliest|latest>]
	args := parseKeyValueArgs(rawCmd[8:])

	topicName, ok := args["topic"]
	if !ok || topicName == "" {
		return 0, fmt.Errorf("missing topic parameter")
	}

	partitionStr, ok := args["partition"]
	if !ok {
		return 0, fmt.Errorf("missing partition parameter")
	}
	partition, err := strconv.Atoi(partitionStr)
	if err != nil {
		return 0, fmt.Errorf("invalid partition ID: %w", err)
	}

	offsetStr, ok := args["offset"]
	if !ok {
		return 0, fmt.Errorf("missing offset parameter")
	}
	requestedOffset, err := strconv.ParseUint(offsetStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid offset: %w", err)
	}

	groupName := args["group"]
	if groupName == "" || groupName == "-" {
		groupName = "default-group"
		util.Info("Using default consumer group for topic '%s' partition %d", topicName, partition)
	}
	ctx.ConsumerGroup = groupName

	autoOffsetReset := args["autoOffsetReset"]
	if autoOffsetReset == "" {
		autoOffsetReset = "earliest"
	}
	autoOffsetReset = strings.ToLower(autoOffsetReset)

	util.Debug("Parsed command: topic=%s, partition=%d, offset=%d, group=%s, autoOffsetReset=%s",
		topicName, partition, requestedOffset, groupName, autoOffsetReset)

	t := ch.TopicManager.GetTopic(topicName)
	if t == nil {
		return 0, fmt.Errorf("topic '%s' does not exist", topicName)
	}

	dh, err := ch.DiskManager.GetHandler(topicName, partition)
	if err != nil {
		util.Error("Failed to get disk handler: %v", err)
		return 0, fmt.Errorf("failed to get disk handler: %w", err)
	}

	startTime := time.Now()
	actualOffset := requestedOffset
	util.Debug("Requested offset: %d for group '%s'", requestedOffset, ctx.ConsumerGroup)

	if requestedOffset == 0 {
		util.Debug("Requested offset is 0 (earliest), checking saved offset for group '%s'", groupName)

		if committedOffset, ok := t.GetCommittedOffset(groupName, partition); ok {
			actualOffset = committedOffset
			util.Debug("Using topic committed offset %d for group '%s'", actualOffset, groupName)
		} else if ch.OffsetManager != nil {
			savedOffset, err := ch.OffsetManager.GetOffset(groupName, topicName, partition)
			if err == nil {
				actualOffset = savedOffset
				util.Debug("Using OffsetManager saved offset %d for group '%s'", actualOffset, groupName)
			} else {
				util.Debug("No saved offset found, applying autoOffsetReset=%s for group '%s'", autoOffsetReset, groupName)
				if autoOffsetReset == "latest" {
					actualOffset, _ = dh.GetLatestOffset()
					util.Debug("Using latest offset %d for group '%s'", actualOffset, groupName)
				} else {
					actualOffset = 0
					util.Debug("Using earliest offset 0 for group '%s'", groupName)
				}
			}
		} else {
			util.Debug("OffsetManager is nil, applying autoOffsetReset=%s for group '%s'", autoOffsetReset, groupName)
			if autoOffsetReset == "latest" {
				actualOffset, _ = dh.GetLatestOffset()
				util.Debug("Using latest offset %d for group '%s'", actualOffset, groupName)
			} else {
				actualOffset = 0
				util.Debug("Using earliest offset 0 for group '%s'", groupName)
			}
		}
	} else {
		util.Debug("Using explicitly requested offset %d for group '%s'", requestedOffset, groupName)
	}

	maxMessages := DefaultMaxPollRecords
	util.Debug("Max messages to fetch: %d", maxMessages)

	messages, err := dh.ReadMessages(actualOffset, maxMessages)
	if err != nil {
		util.Error("Failed to read messages: %v", err)
		return 0, fmt.Errorf("failed to read messages from disk: %w", err)
	}

	util.Debug("Read %d messages from disk", len(messages))

	streamedCount := 0
	lastOffset := actualOffset
	for _, msg := range messages {
		msgBytes := []byte(msg.Payload)
		if err := util.WriteWithLength(conn, msgBytes); err != nil {
			return streamedCount, fmt.Errorf("failed to stream message: %w", err)
		}
		streamedCount++
		lastOffset++
	}

	if streamedCount > 0 {
		if err := t.CommitOffset(ctx.ConsumerGroup, partition, lastOffset); err != nil {
			util.Warn("Failed to commit offset to topic for group '%s': %v", ctx.ConsumerGroup, err)
		} else {
			util.Debug("[OFFSET] Successfully committed offset %d to topic for group '%s', topic '%s', partition %d",
				lastOffset, ctx.ConsumerGroup, topicName, partition)
		}

		if ch.OffsetManager != nil {
			if err := ch.OffsetManager.CommitOffset(ctx.ConsumerGroup, topicName, partition, lastOffset); err != nil {
				util.Warn("Failed to commit offset to OffsetManager for group '%s': %v", ctx.ConsumerGroup, err)
			} else {
				util.Debug("[OFFSET] Successfully committed offset %d to OffsetManager for group '%s', topic '%s', partition %d",
					lastOffset, ctx.ConsumerGroup, topicName, partition)
			}
		}
	}

	duration := time.Since(startTime)
	util.Debug("[METRICS] Streamed %d messages from topic '%s' partition %d in %v",
		streamedCount, topicName, partition, duration)

	return streamedCount, nil
}

// HandleCommand processes non-streaming commands and returns a signal for streaming commands.
func (ch *CommandHandler) HandleCommand(rawCmd string, ctx *ClientContext) string {
	cmd := strings.TrimSpace(rawCmd)
	if cmd == "" {
		resp := "ERROR: empty command"
		ch.logCommandResult(rawCmd, resp)
		return resp
	}

	if strings.HasPrefix(strings.ToUpper(cmd), "CONSUME ") {
		args := parseKeyValueArgs(cmd[8:])
		if args["topic"] == "" || args["partition"] == "" || args["offset"] == "" {
			resp := "ERROR: invalid CONSUME syntax. Expected: CONSUME topic=<name> partition=<N> offset=<N> group=<name> [autoOffsetReset=<earliest|latest>]"
			ch.logCommandResult(rawCmd, resp)
			return resp
		}
		return STREAM_DATA_SIGNAL
	}

	tm := ch.TopicManager
	var resp string

	switch {
	case strings.EqualFold(cmd, "HELP"):
		resp = `Available commands:      
  SETGROUP group=<name>                                    - set consumer group name      
  CREATE topic=<name> [partitions=<N>]                     - create topic (default=4)      
  DELETE topic=<name>                                      - delete topic      
  LIST                                                     - list all topics      
  SUBSCRIBE topic=<name> [group=<name>]                    - subscribe to topic      
  PUBLISH topic=<name> acks=<0|1> message=<text> [producerId=<id> seqNum=<N> epoch=<N>] - publish message      
  CONSUME topic=<name> partition=<N> offset=<N> group=<name> [autoOffsetReset=<earliest|latest>] - consume messages      
  JOIN_GROUP group=<name> consumer=<id>                    - join consumer group      
  LEAVE_GROUP group=<name> consumer=<id>                   - leave consumer group      
  HEARTBEAT group=<name> consumer=<id>                     - send heartbeat      
  COMMIT_OFFSET topic=<name> partition=<N> offset=<N>     - commit offset      
  FETCH_OFFSET topic=<name> partition=<N> group=<name>    - fetch committed offset      
  REGISTER_GROUP topic=<name> group=<name>                - register consumer group      
  GROUP_STATUS group=<name>                                - get group status      
  HELP                                                     - show this help      
  EXIT                                                     - exit`

	case strings.HasPrefix(strings.ToUpper(cmd), "CREATE "):
		args := parseKeyValueArgs(cmd[7:])

		topicName, ok := args["topic"]
		if !ok || topicName == "" {
			resp = "ERROR: missing topic parameter. Expected: CREATE topic=<name> [partitions=<N>]"
			break
		}

		partitions := 4 // default
		if partStr, ok := args["partitions"]; ok {
			n, err := strconv.Atoi(partStr)
			if err != nil || n <= 0 {
				resp = "ERROR: partitions must be a positive integer"
				break
			}
			partitions = n
		}

		t := tm.CreateTopic(topicName, partitions)

		if ch.Coordinator != nil {
			err := ch.Coordinator.RegisterGroup(topicName, "default-group", partitions)
			if err != nil {
				util.Warn("Failed to register default group with coordinator: %v", err)
			}
		}

		resp = fmt.Sprintf("✅ Topic '%s' now has %d partitions", topicName, len(t.Partitions))

	case strings.HasPrefix(strings.ToUpper(cmd), "DELETE "):
		args := parseKeyValueArgs(cmd[7:])

		topicName, ok := args["topic"]
		if !ok || topicName == "" {
			resp = "ERROR: missing topic parameter. Expected: DELETE topic=<name>"
			break
		}

		if tm.DeleteTopic(topicName) {
			resp = fmt.Sprintf("🗑️ Topic '%s' deleted", topicName)
		} else {
			resp = fmt.Sprintf("ERROR: topic '%s' not found", topicName)
		}

	case strings.EqualFold(cmd, "LIST"):
		names := tm.ListTopics()
		if len(names) == 0 {
			resp = "(no topics)"
			break
		}
		resp = strings.Join(names, ", ")

	case strings.HasPrefix(strings.ToUpper(cmd), "SUBSCRIBE "):
		args := parseKeyValueArgs(cmd[10:])

		topicName, ok := args["topic"]
		if !ok || topicName == "" {
			resp = "ERROR: missing topic parameter. Expected: SUBSCRIBE topic=<name> [group=<name>]"
			break
		}

		t := tm.GetTopic(topicName)
		if t == nil {
			resp = fmt.Sprintf("ERROR: topic '%s' does not exist", topicName)
			break
		}

		groupName := args["group"]
		if groupName == "" {
			groupName = ctx.ConsumerGroup
		}

		ctx.CurrentTopics[topicName] = struct{}{}
		t.RegisterConsumerGroup(groupName, 1)
		resp = fmt.Sprintf("✅ Subscribed to '%s' with group '%s'", topicName, groupName)

	case strings.HasPrefix(strings.ToUpper(cmd), "PUBLISH "):
		args := parseKeyValueArgs(cmd[8:])

		topicName, ok := args["topic"]
		if !ok || topicName == "" {
			resp = "ERROR: missing topic parameter"
			break
		}

		message, ok := args["message"]
		if !ok || message == "" {
			resp = "ERROR: missing message parameter"
			break
		}

		t := tm.GetTopic(topicName)
		if t == nil {
			resp = fmt.Sprintf("ERROR: topic '%s' does not exist", topicName)
			break
		}

		msg := types.Message{
			ID:      util.GenerateID(message),
			Payload: message,
			Key:     message,
		}

		// Idempotence
		if producerID, ok := args["producerId"]; ok {
			msg.ProducerID = producerID
			if seqNumStr, ok := args["seqNum"]; ok {
				seqNum, _ := strconv.ParseUint(seqNumStr, 10, 64)
				msg.SeqNum = seqNum
			}
			if epochStr, ok := args["epoch"]; ok {
				epoch, _ := strconv.ParseInt(epochStr, 10, 64)
				msg.Epoch = epoch
			}
		}

		acks := args["acks"]
		var err error
		if acks == "1" {
			err = tm.PublishWithAck(topicName, msg) // sync
		} else {
			err = tm.Publish(topicName, msg) // async
		}

		if err != nil {
			resp = fmt.Sprintf("ERROR: %v", err)
			break
		}

		if acks == "1" {
			resp = fmt.Sprintf("✅ Published to '%s' (acks=1)", topicName)
		} else {
			resp = fmt.Sprintf("📤 Published to '%s'", topicName)
		}

	case strings.HasPrefix(strings.ToUpper(cmd), "SETGROUP "):
		args := parseKeyValueArgs(cmd[9:])

		groupName, ok := args["group"]
		if !ok || groupName == "" {
			resp = "ERROR: SETGROUP requires group parameter"
			break
		}

		ctx.SetConsumerGroup(groupName)
		resp = fmt.Sprintf("✅ Consumer group set to '%s'", groupName)

	case strings.HasPrefix(strings.ToUpper(cmd), "REGISTER_GROUP "):
		args := parseKeyValueArgs(cmd[15:])

		topicName, ok := args["topic"]
		if !ok || topicName == "" {
			resp = "ERROR: REGISTER_GROUP requires topic parameter"
			break
		}

		groupName, ok := args["group"]
		if !ok || groupName == "" {
			resp = "ERROR: REGISTER_GROUP requires group parameter"
			break
		}

		t := tm.GetTopic(topicName)
		if t == nil {
			resp = fmt.Sprintf("ERROR: topic '%s' does not exist", topicName)
			break
		}

		if ch.Coordinator != nil {
			if err := ch.Coordinator.RegisterGroup(topicName, groupName, len(t.Partitions)); err != nil {
				resp = fmt.Sprintf("ERROR: %v", err)
			} else {
				resp = fmt.Sprintf("✅ Group '%s' registered for topic '%s'", groupName, topicName)
			}
		} else {
			resp = "ERROR: coordinator not available"
		}

	case strings.HasPrefix(strings.ToUpper(cmd), "JOIN_GROUP "):
		args := parseKeyValueArgs(cmd[11:])

		topicName, ok := args["topic"]
		if !ok || topicName == "" {
			resp = "ERROR: JOIN_GROUP requires topic parameter"
			break
		}

		groupName, ok := args["group"]
		if !ok || groupName == "" {
			resp = "ERROR: JOIN_GROUP requires group parameter"
			break
		}

		consumerID, ok := args["member"]
		if !ok || consumerID == "" {
			resp = "ERROR: JOIN_GROUP requires member parameter"
			break
		}

		if ch.Coordinator != nil {
			assignments, err := ch.Coordinator.AddConsumer(groupName, consumerID)
			if err != nil {
				resp = fmt.Sprintf("ERROR: %v", err)
			} else {
				resp = fmt.Sprintf("✅ Joined group '%s' with partitions: %v", groupName, assignments)
			}
		} else {
			resp = "ERROR: coordinator not available"
		}

	case strings.HasPrefix(strings.ToUpper(cmd), "LEAVE_GROUP "):
		args := parseKeyValueArgs(cmd[12:])

		topicName, ok := args["topic"]
		if !ok || topicName == "" {
			resp = "ERROR: LEAVE_GROUP requires topic parameter"
			break
		}

		groupName, ok := args["group"]
		if !ok || groupName == "" {
			resp = "ERROR: LEAVE_GROUP requires group parameter"
			break
		}

		consumerID, ok := args["member"]
		if !ok || consumerID == "" {
			resp = "ERROR: LEAVE_GROUP requires member parameter"
			break
		}

		if ch.Coordinator != nil {
			err := ch.Coordinator.RemoveConsumer(groupName, consumerID)
			if err != nil {
				resp = fmt.Sprintf("ERROR: %v", err)
			} else {
				resp = fmt.Sprintf("✅ Left group '%s'", groupName)
			}
		} else {
			resp = "ERROR: coordinator not available"
		}

	case strings.HasPrefix(strings.ToUpper(cmd), "FETCH_OFFSET "):
		args := parseKeyValueArgs(cmd[13:])

		topicName, ok := args["topic"]
		if !ok || topicName == "" {
			resp = "ERROR: FETCH_OFFSET requires topic parameter"
			break
		}

		partitionStr, ok := args["partition"]
		if !ok || partitionStr == "" {
			resp = "ERROR: FETCH_OFFSET requires partition parameter"
			break
		}
		partition, err := strconv.Atoi(partitionStr)
		if err != nil {
			resp = "ERROR: invalid partition"
			break
		}

		groupName, ok := args["group"]
		if !ok || groupName == "" {
			resp = "ERROR: FETCH_OFFSET requires group parameter"
			break
		}

		if ch.OffsetManager != nil {
			offset, err := ch.OffsetManager.GetOffset(groupName, topicName, partition)
			if err != nil {
				resp = fmt.Sprintf("ERROR: %v", err)
			} else {
				resp = fmt.Sprintf("%d", offset)
			}
		} else {
			resp = "ERROR: offset manager not available"
		}

	case strings.HasPrefix(strings.ToUpper(cmd), "GROUP_STATUS "):
		args := parseKeyValueArgs(cmd[13:])

		groupName, ok := args["group"]
		if !ok || groupName == "" {
			resp = "ERROR: GROUP_STATUS requires group parameter"
			break
		}

		if ch.Coordinator == nil {
			resp = "ERROR: coordinator not available"
			break
		}

		status, err := ch.Coordinator.GetGroupStatus(groupName)
		if err != nil {
			resp = fmt.Sprintf("ERROR: %v", err)
			break
		}

		statusJSON, err := json.Marshal(status)
		if err != nil {
			resp = fmt.Sprintf("ERROR: failed to marshal status: %v", err)
			break
		}
		resp = string(statusJSON)

	case strings.HasPrefix(strings.ToUpper(cmd), "HEARTBEAT "):
		args := parseKeyValueArgs(cmd[10:])

		topicName, ok := args["topic"]
		if !ok || topicName == "" {
			resp = "ERROR: HEARTBEAT requires topic parameter"
			break
		}

		groupName, ok := args["group"]
		if !ok || groupName == "" {
			resp = "ERROR: HEARTBEAT requires group parameter"
			break
		}

		consumerID, ok := args["member"]
		if !ok || consumerID == "" {
			resp = "ERROR: HEARTBEAT requires member parameter"
			break
		}

		if ch.Coordinator != nil {
			err := ch.Coordinator.RecordHeartbeat(groupName, consumerID)
			if err != nil {
				resp = fmt.Sprintf("ERROR: %v", err)
			} else {
				resp = "OK"
			}
		} else {
			resp = "ERROR: coordinator not available"
		}

	case strings.HasPrefix(strings.ToUpper(cmd), "COMMIT_OFFSET "):
		args := parseKeyValueArgs(cmd[14:])

		topicName, ok := args["topic"]
		if !ok || topicName == "" {
			resp = "ERROR: COMMIT_OFFSET requires topic parameter"
			break
		}

		partitionStr, ok := args["partition"]
		if !ok || partitionStr == "" {
			resp = "ERROR: COMMIT_OFFSET requires partition parameter"
			break
		}
		partition, err := strconv.Atoi(partitionStr)
		if err != nil {
			resp = "ERROR: invalid partition"
			break
		}

		offsetStr, ok := args["offset"]
		if !ok || offsetStr == "" {
			resp = "ERROR: COMMIT_OFFSET requires offset parameter"
			break
		}
		offset, err := strconv.ParseUint(offsetStr, 10, 64)
		if err != nil {
			resp = "ERROR: invalid offset"
			break
		}

		if ch.OffsetManager != nil {
			err := ch.OffsetManager.CommitOffset(ctx.ConsumerGroup, topicName, partition, offset)
			if err != nil {
				resp = fmt.Sprintf("ERROR: %v", err)
			} else {
				resp = "OK"
			}
		} else {
			resp = "ERROR: offset manager not available"
		}

	default:
		resp = "ERROR: unknown command: " + cmd + ". Type HELP for available commands."
	}

	ch.logCommandResult(rawCmd, resp)
	return resp
}

func parseKeyValueArgs(argsStr string) map[string]string {
	result := make(map[string]string)
	messageIdx := strings.Index(argsStr, "message=")

	if messageIdx != -1 {
		beforeMessage := argsStr[:messageIdx]
		parts := strings.Fields(beforeMessage)
		for _, part := range parts {
			kv := strings.SplitN(part, "=", 2)
			if len(kv) == 2 {
				result[kv[0]] = kv[1]
			}
		}

		result["message"] = strings.TrimSpace(argsStr[messageIdx+8:])
	} else {
		parts := strings.Fields(argsStr)
		for _, part := range parts {
			kv := strings.SplitN(part, "=", 2)
			if len(kv) == 2 {
				result[kv[0]] = kv[1]
			}
		}
	}

	return result
}

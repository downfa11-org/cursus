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
	"github.com/downfa11-org/go-broker/pkg/stream"
	"github.com/downfa11-org/go-broker/pkg/topic"
	"github.com/downfa11-org/go-broker/pkg/types"
	"github.com/downfa11-org/go-broker/util"
)

const DefaultMaxPollRecords = 8192
const STREAM_DATA_SIGNAL = "STREAM_DATA"

type CommandHandler struct {
	TopicManager  *topic.TopicManager
	DiskManager   *disk.DiskManager
	Config        *config.Config
	Coordinator   *coordinator.Coordinator
	StreamManager *stream.StreamManager
}

type ConsumeArgs struct {
	Topic     string
	Partition int
	Offset    uint64
}

func NewCommandHandler(tm *topic.TopicManager, dm *disk.DiskManager, cfg *config.Config, cd *coordinator.Coordinator, sm *stream.StreamManager) *CommandHandler {
	return &CommandHandler{
		TopicManager:  tm,
		DiskManager:   dm,
		Config:        cfg,
		Coordinator:   cd,
		StreamManager: sm,
	}
}

func (ch *CommandHandler) logCommandResult(cmd, response string) {
	status := "SUCCESS"
	if strings.HasPrefix(response, "ERROR:") {
		status = "FAILURE"
	}

	cleanResponse := strings.ReplaceAll(response, "\n", " ")
	util.Debug("%s - [%s] to Response [%s]", status, cmd, cleanResponse)
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

	actualOffset, err := ch.resolveOffset(topicName, partition, requestedOffset, groupName, autoOffsetReset)
	if err != nil {
		return 0, err
	}

	maxMessages := DefaultMaxPollRecords
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

	if streamedCount > 0 && ch.Coordinator != nil {
		if err := ch.Coordinator.CommitOffset(ctx.ConsumerGroup, topicName, partition, lastOffset); err != nil {
			util.Warn("Failed to commit offset to OffsetManager for group '%s': %v", ctx.ConsumerGroup, err)
		} else {
			util.Debug("Successfully committed offset %d to OffsetManager for group '%s', topic '%s', partition %d",
				lastOffset, ctx.ConsumerGroup, topicName, partition)
		}

	}

	duration := time.Since(startTime)
	util.Debug("Streamed %d messages from topic '%s' partition %d in %v",
		streamedCount, topicName, partition, duration)

	return streamedCount, nil
}

func (ch *CommandHandler) HandleStreamCommand(conn net.Conn, rawCmd string, ctx *ClientContext) error {
	if len(rawCmd) < 7 {
		return fmt.Errorf("invalid STREAM command format")
	}

	args := parseKeyValueArgs(rawCmd[7:])
	topicName, ok := args["topic"]
	if !ok || topicName == "" {
		return fmt.Errorf("missing topic parameter")
	}

	partitionStr, ok := args["partition"]
	if !ok {
		return fmt.Errorf("missing partition parameter")
	}
	partition, err := strconv.Atoi(partitionStr)
	if err != nil {
		return fmt.Errorf("invalid partition ID: %w", err)
	}

	groupName := args["group"]
	if groupName == "" || groupName == "-" {
		groupName = "default-group"
		util.Info("Using default consumer group for topic '%s' partition %d", topicName, partition)
	}
	ctx.ConsumerGroup = groupName

	actualOffset, err := ch.resolveOffset(topicName, partition, 0, groupName, args["autoOffsetReset"])
	if err != nil {
		return err
	}

	streamKey := fmt.Sprintf("%s:%d:%s", topicName, partition, groupName)
	streamConn := stream.NewStreamConnection(conn, topicName, partition, groupName, actualOffset)

	if err := ch.StreamManager.AddStream(streamKey, streamConn); err != nil {
		return fmt.Errorf("failed to add stream: %w", err)
	}
	defer ch.StreamManager.RemoveStream(streamKey)

	return ch.streamLoop(streamConn)
}

func (ch *CommandHandler) streamLoop(stream *stream.StreamConnection) error {
	dh, err := ch.DiskManager.GetHandler(stream.Topic(), stream.Partition())
	if err != nil {
		util.Error("Failed to get disk handler for stream: %v", err)
		return err
	}

	tickCount := 0
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	lastCommitTime := time.Now()
	const commitInterval = 5 * time.Second

	for {
		select {
		case <-stream.StopCh():
			if ch.Coordinator != nil {

				if err := ch.Coordinator.CommitOffset(stream.Group(), stream.Topic(), stream.Partition(), uint64(stream.Offset())); err != nil {
					util.Warn("Failed to commit offset to OffsetManager for group '%s': %v", stream.Group(), err)
				}
			}
			util.Debug("Stream loop stopped for topic '%s' partition %d group '%s'",
				stream.Topic(), stream.Partition(), stream.Group())
			return nil
		case <-ticker.C:
			tickCount++
			messages, err := dh.ReadMessages(uint64(stream.Offset()), 100)
			if err != nil {
				util.Error("Failed to read messages in stream loop: %v", err)
				if ch.Coordinator != nil && stream.Offset() > 0 {
					err = ch.Coordinator.CommitOffset(stream.Group(), stream.Topic(), stream.Partition(), uint64(stream.Offset()))
					if err != nil {
						util.Error("Failed to commit offset: %v", err)
					}
				}
				return err
			}

			if len(messages) > 0 {
				util.Debug("Stream tick #%d: Read %d messages from offset %d for topic '%s' partition %d",
					tickCount, len(messages), stream.Offset(), stream.Topic(), stream.Partition())
			}

			for _, msg := range messages {
				if err := util.WriteWithLength(stream.Conn(), []byte(msg.Payload)); err != nil {
					util.Debug("Connection closed while streaming message for topic '%s' partition %d",
						stream.Topic(), stream.Partition())
					return err
				}
				stream.SetOffset(stream.Offset() + 1)
				stream.SetLastActive(time.Now())
			}

			if time.Since(lastCommitTime) > commitInterval && ch.Coordinator != nil {
				if err := ch.Coordinator.CommitOffset(stream.Group(), stream.Topic(), stream.Partition(), uint64(stream.Offset())); err != nil {
					util.Warn("Failed to commit offset to OffsetManager for group '%s': %v", stream.Group(), err)
				}
				lastCommitTime = time.Now()
			}
		}
	}
}

// HandleCommand processes non-streaming commands and returns a signal for streaming commands.
func (ch *CommandHandler) HandleCommand(rawCmd string, ctx *ClientContext) string {
	cmd := strings.TrimSpace(rawCmd)
	if cmd == "" {
		resp := "ERROR: empty command"
		ch.logCommandResult(rawCmd, resp)
		return resp
	}

	if strings.HasPrefix(strings.ToUpper(cmd), "STREAM ") {
		args := parseKeyValueArgs(cmd[7:])
		if args["topic"] == "" || args["partition"] == "" || args["group"] == "" {
			resp := "ERROR: invalid STREAM syntax. Expected: STREAM topic=<name> partition=<N> group=<name>"
			ch.logCommandResult(rawCmd, resp)
			return resp
		}
		return STREAM_DATA_SIGNAL
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
  CREATE topic=<name> [partitions=<N>]                     - create topic (default=4)      
  DELETE topic=<name>                                      - delete topic      
  LIST                                                     - list all topics         
  PUBLISH topic=<name> acks=<0|1> message=<text> [producerId=<id> seqNum=<N> epoch=<N>] - publish message      
  CONSUME topic=<name> partition=<N> offset=<N> group=<name> [autoOffsetReset=<earliest|latest>] - consume messages      
  JOIN_GROUP topic=<name> group=<name> consumer=<id>                    - join consumer group      
  LEAVE_GROUP group=<name> consumer=<id>                   - leave consumer group      
  HEARTBEAT topic=<name> group=<name> consumer=<id>                     - send heartbeat      
  COMMIT_OFFSET topic=<name> partition=<N> group=<name> offset=<N>     - commit offset      
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

		tm.CreateTopic(topicName, partitions)
		t := tm.GetTopic(topicName)

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
				seqNum, err := strconv.ParseUint(seqNumStr, 10, 64)
				if err != nil {
					resp = fmt.Sprintf("ERROR: invalid seqNum: %v", err)
					break
				}
				msg.SeqNum = seqNum
			}
			if epochStr, ok := args["epoch"]; ok {
				epoch, err := strconv.ParseInt(epochStr, 10, 64)
				if err != nil {
					resp = fmt.Sprintf("ERROR: invalid epoch: %v", err)
					break
				}
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

		consumerID, ok := args["consumer"]
		if !ok || consumerID == "" {
			resp = "ERROR: JOIN_GROUP requires consumer parameter"
			break
		}

		if ch.Coordinator == nil {
			resp = "ERROR: coordinator not available"
			break
		}

		assignments, err := ch.Coordinator.AddConsumer(groupName, consumerID)
		if err != nil {
			if strings.Contains(err.Error(), "not found") {
				t := tm.GetTopic(topicName)
				if t == nil {
					resp = fmt.Sprintf("ERROR: topic '%s' does not exist", topicName)
					break
				}

				if regErr := ch.Coordinator.RegisterGroup(topicName, groupName, len(t.Partitions)); regErr != nil {
					util.Debug("register group error, %v", regErr)
					resp = fmt.Sprintf("ERROR: failed to create group: %v", regErr)
					break
				}

				util.Debug("Group registered, Start to add consumer")

				assignments, err = ch.Coordinator.AddConsumer(groupName, consumerID)
				if err != nil {
					util.Debug("add consumer errr: %v,", err)
					resp = fmt.Sprintf("ERROR: failed to join after group creation: %v", err)
					break
				}
			} else {
				resp = fmt.Sprintf("ERROR: %v", err)
				break
			}
		}

		resp = fmt.Sprintf("✅ Joined group '%s' with partitions: %v", groupName, assignments)

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

		consumerID, ok := args["consumer"]
		if !ok || consumerID == "" {
			resp = "ERROR: LEAVE_GROUP requires consumer parameter"
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

		if ch.Coordinator != nil {
			offset, err := ch.Coordinator.GetOffset(groupName, topicName, partition)
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

		consumerID, ok := args["consumer"]
		if !ok || consumerID == "" {
			resp = "ERROR: HEARTBEAT requires consumer parameter"
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

		groupID, ok := args["group"]
		if !ok || groupID == "" {
			resp = "ERROR: COMMIT_OFFSET requires groupID parameter"
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

		if ch.Coordinator != nil {
			err := ch.Coordinator.CommitOffset(groupID, topicName, partition, offset)
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

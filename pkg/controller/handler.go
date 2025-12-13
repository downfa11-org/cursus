package controller

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math/big"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/downfa11-org/go-broker/pkg/cluster/routing"
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

	Router *routing.ClientRouter
}

type ConsumeArgs struct {
	Topic     string
	Partition int
	Offset    uint64
}

func NewCommandHandler(tm *topic.TopicManager, dm *disk.DiskManager, cfg *config.Config, cd *coordinator.Coordinator, sm *stream.StreamManager, router *routing.ClientRouter) *CommandHandler {
	return &CommandHandler{
		TopicManager:  tm,
		DiskManager:   dm,
		Config:        cfg,
		Coordinator:   cd,
		StreamManager: sm,
		Router:        router,
	}
}

func (ch *CommandHandler) logCommandResult(cmd, response string) {
	status := "SUCCESS"
	if strings.HasPrefix(response, "ERROR:") {
		status = "FAILURE"
	}
	cleanResponse := strings.ReplaceAll(response, "\n", " ")
	util.Debug("status: '%s', command: '%s' to Response '%s'", status, cmd, cleanResponse)
}

// HandleConsumeCommand is responsible for parsing the CONSUME command and streaming messages.
func (ch *CommandHandler) HandleConsumeCommand(conn net.Conn, rawCmd string, ctx *ClientContext) (int, error) {
	// CONSUME topic=<name> partition=<N> offset=<N> group=<name> [autoOffsetReset=<earliest|latest>]
	args := parseKeyValueArgs(rawCmd[8:])

	topicPattern, ok := args["topic"]
	if !ok || topicPattern == "" {
		return 0, fmt.Errorf("missing topic parameter")
	}

	matchedTopics, err := ch.matchTopicPattern(topicPattern)
	if err != nil {
		return 0, err
	}

	partitionStr, ok := args["partition"]
	if !ok {
		return 0, fmt.Errorf("missing partition parameter")
	}
	partition, err := strconv.Atoi(partitionStr)
	if err != nil {
		return 0, fmt.Errorf("invalid partition ID: %w", err)
	}

	totalStreamed := 0
	for _, topicName := range matchedTopics {
		streamed, err := ch.consumeFromTopic(conn, topicName, partition, args, ctx)
		if err != nil {
			return totalStreamed, err
		}
		totalStreamed += streamed
	}

	return totalStreamed, nil
}

func (ch *CommandHandler) consumeFromTopic(conn net.Conn, topicName string, partition int, args map[string]string, ctx *ClientContext) (int, error) {
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
	}

	autoOffsetReset := args["autoOffsetReset"]
	if autoOffsetReset == "" {
		autoOffsetReset = "earliest"
	}
	autoOffsetReset = strings.ToLower(autoOffsetReset)

	maxMessages := DefaultMaxPollRecords
	if batchStr := args["batch"]; batchStr != "" {
		if batch, err := strconv.Atoi(batchStr); err == nil && batch > 0 && batch <= DefaultMaxPollRecords {
			maxMessages = batch
		}
	}

	waitTimeout := 0 * time.Millisecond
	if waitStr := args["wait_ms"]; waitStr != "" {
		if waitMS, err := strconv.Atoi(waitStr); err == nil && waitMS > 0 {
			waitTimeout = time.Duration(waitMS) * time.Millisecond
		}
	}

	t := ch.TopicManager.GetTopic(topicName)
	if t == nil {
		return 0, fmt.Errorf("topic '%s' does not exist", topicName)
	}

	dh, err := ch.DiskManager.GetHandler(topicName, partition)
	if err != nil {
		util.Error("Failed to get disk handler: %v", err)
		return 0, fmt.Errorf("failed to get disk handler: %w", err)
	}

	actualOffset, err := ch.resolveOffset(topicName, partition, requestedOffset, groupName, autoOffsetReset)
	if err != nil {
		util.Debug("resolveOffset error")
		return 0, err
	}

	if !ch.ValidateOwnership(ctx.ConsumerGroup, ctx.MemberID, ctx.Generation, partition) {
		util.Debug("not validate ownership")
		return 0, fmt.Errorf("not partition owner or generation mismatch")
	}

	currentOffset := actualOffset
	streamedCount := 0

	messages, err := dh.ReadMessages(currentOffset, maxMessages)
	if err != nil {
		util.Error("Failed to read messages: %v", err)
		return streamedCount, err
	}

	if len(messages) == 0 && waitTimeout > 0 {
		startTime := time.Now()
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for time.Since(startTime) < waitTimeout {
			<-ticker.C
			newMessages, err := dh.ReadMessages(currentOffset, maxMessages)
			if err != nil {
				util.Error("Failed to read messages during wait: %v", err)
				return 0, err
			}
			if len(newMessages) > 0 {
				messages = newMessages
				break
			}
		}
	}

	batchData, err := util.EncodeBatchMessages(topicName, partition, messages)
	if err != nil {
		return 0, fmt.Errorf("failed to encode batch: %w", err)
	}

	if err := util.WriteWithLength(conn, batchData); err != nil {
		return 0, fmt.Errorf("failed to stream batch: %w", err)
	}
	streamedCount = len(messages)
	return streamedCount, nil
}

func (ch *CommandHandler) matchTopicPattern(pattern string) ([]string, error) {
	if !strings.Contains(pattern, "*") && !strings.Contains(pattern, "?") {
		if ch.TopicManager.GetTopic(pattern) == nil {
			return nil, fmt.Errorf("topic '%s' does not exist", pattern)
		}
		return []string{pattern}, nil
	}

	regexPattern := strings.ReplaceAll(pattern, "*", ".*")
	regexPattern = strings.ReplaceAll(regexPattern, "?", ".")
	regex, err := regexp.Compile("^" + regexPattern + "$")
	if err != nil {
		return nil, fmt.Errorf("invalid topic pattern: %w", err)
	}

	allTopics := ch.TopicManager.ListTopics()
	var matchedTopics []string
	for _, topic := range allTopics {
		if regex.MatchString(topic) {
			matchedTopics = append(matchedTopics, topic)
		}
	}

	if len(matchedTopics) == 0 {
		return nil, fmt.Errorf("no topics match pattern '%s'", pattern)
	}

	return matchedTopics, nil
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

	if genStr := args["gen"]; genStr != "" {
		generation, err := strconv.ParseInt(genStr, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid gen parameter: %w", err)
		}
		ctx.Generation = int(generation)

	}

	if memberID := args["member"]; memberID != "" {
		ctx.MemberID = memberID
	}

	maxMessages := DefaultMaxPollRecords
	if batchStr := args["batch"]; batchStr != "" {
		if batch, err := strconv.Atoi(batchStr); err == nil && batch > 0 && batch <= DefaultMaxPollRecords {
			maxMessages = batch
		}
	}

	offsetStr := args["offset"]
	var requestedOffset uint64
	if offsetStr != "" {
		if parsed, err := strconv.ParseUint(offsetStr, 10, 64); err == nil {
			requestedOffset = parsed
		}
	}
	actualOffset, err := ch.resolveOffset(topicName, partition, requestedOffset, groupName, args["autoOffsetReset"])
	if err != nil {
		return err
	}

	if !ch.ValidateOwnership(ctx.ConsumerGroup, ctx.MemberID, ctx.Generation, partition) {
		return fmt.Errorf("not partition owner or generation mismatch")
	}

	streamKey := fmt.Sprintf("%s:%d:%s", topicName, partition, groupName)
	streamConn := stream.NewStreamConnection(conn, topicName, partition, groupName, actualOffset)
	streamConn.SetBatchSize(maxMessages)
	streamConn.SetInterval(100 * time.Millisecond)
	streamConn.SetCoordinator(ch.Coordinator)

	dh, err := ch.DiskManager.GetHandler(topicName, partition)
	if err != nil {
		return fmt.Errorf("failed to get disk handler: %w", err)
	}

	readFn := func(offset uint64, max int) ([]types.Message, error) {
		messages, err := dh.ReadMessages(offset, max)
		if err != nil {
			return nil, err
		}

		if ch.TopicManager == nil {
			util.Warn("TopicManager is nil in stream readFn, skipping deduplication.")
			return messages, nil
		}

		return messages, nil
	}

	if err := ch.StreamManager.AddStream(streamKey, streamConn, readFn, ch.Config.StreamCommitInterval); err != nil {
		return err
	}

	return nil
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
		var err error

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

		producerID, ok := args["producerId"]
		if !ok || producerID == "" {
			resp = "ERROR: missing producerID parameter"
			break
		}

		var seqNum uint64
		if seqNumStr, ok := args["seqNum"]; ok {
			seqNum, err = strconv.ParseUint(seqNumStr, 10, 64)
			if err != nil {
				resp = fmt.Sprintf("ERROR: invalid seqNum: %v", err)
				break
			}
		}

		var epoch int64
		if epochStr, ok := args["epoch"]; ok {
			epoch, err = strconv.ParseInt(epochStr, 10, 64)
			if err != nil {
				resp = fmt.Sprintf("ERROR: invalid epoch: %v", err)
				break
			}
		}

		t := tm.GetTopic(topicName)
		if t == nil {
			resp = fmt.Sprintf("ERROR: topic '%s' does not exist", topicName)
			break
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
			resp = fmt.Sprintf("ERROR: %v", err)
			break
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

		if ch.Coordinator == nil {
			resp = "ERROR: coordinator not available"
			break
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
				assignments, err = ch.Coordinator.AddConsumer(groupName, consumerID)
				if err != nil {
					resp = fmt.Sprintf("ERROR: failed to join after group creation: %v", err)
					break
				}
			} else {
				resp = fmt.Sprintf("ERROR: %v", err)
				break
			}
		}

		ctx.MemberID = consumerID
		ctx.Generation = ch.Coordinator.GetGeneration(groupName)

		util.Debug("✅ Joined group '%s' member '%s' generation '%d' with partitions: %v", groupName, ctx.MemberID, ctx.Generation, assignments)
		resp = fmt.Sprintf("OK generation=%d member=%s assignments=%v", ctx.Generation, ctx.MemberID, assignments)

	case strings.HasPrefix(strings.ToUpper(cmd), "SYNC_GROUP "):
		args := parseKeyValueArgs(cmd[11:])

		topicName, ok := args["topic"]
		if !ok || topicName == "" {
			resp = "ERROR: SYNC_GROUP requires topic parameter"
			break
		}
		groupName, ok := args["group"]
		if !ok || groupName == "" {
			resp = "ERROR: SYNC_GROUP requires group parameter"
			break
		}
		memberID, ok := args["member"]
		if !ok || memberID == "" {
			resp = "ERROR: SYNC_GROUP requires member parameter"
			break
		}

		if ch.Coordinator == nil {
			resp = "ERROR: coordinator not available"
			break
		}

		assignments := ch.Coordinator.GetAssignments(groupName)
		if _, exists := assignments[memberID]; !exists {
			resp = fmt.Sprintf("ERROR: member %s not found in group", memberID)
			break
		}

		memberAssignments := assignments[memberID]
		resp = fmt.Sprintf("OK assignments=%v", memberAssignments)

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

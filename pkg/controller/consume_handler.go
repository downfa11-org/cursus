package controller

import (
	"fmt"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/downfa11-org/cursus/pkg/stream"
	"github.com/downfa11-org/cursus/pkg/types"
	"github.com/downfa11-org/cursus/util"
)

// HandleConsumeCommand is responsible for parsing the CONSUME command and streaming messages.
func (ch *CommandHandler) HandleConsumeCommand(conn net.Conn, rawCmd string, ctx *ClientContext) (int, error) {
	// CONSUME topic=<name> partition=<N> offset=<N> group=<name> [autoOffsetReset=<earliest|latest>]
	args := parseKeyValueArgs(rawCmd[8:])

	topicPattern, ok := args["topic"]
	if !ok || topicPattern == "" {
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

	if err := ch.checkLeaderOrRedirect(conn); err != nil {
		if err.Error() == "not leader" {
			return 0, nil
		}
		return 0, err
	}

	if !strings.ContainsAny(topicPattern, "*?") {
		if err := ch.checkPartitionAuthorization(topicPattern, partition); err != nil {
			return 0, err
		}
	}

	matchedTopics, err := ch.matchTopicPattern(topicPattern)
	if err != nil {
		return 0, err
	}

	if ch.Coordinator != nil && (strings.Contains(topicPattern, "*") || strings.Contains(topicPattern, "?")) {
		group := ch.Coordinator.GetGroup(ctx.ConsumerGroup)
		if group != nil {
			matched, _ := regexp.MatchString("^"+strings.ReplaceAll(strings.ReplaceAll(regexp.QuoteMeta(topicPattern), "\\*", ".*"), "\\?", ".")+"$", group.TopicName)
			if matched {
				matchedTopics = []string{group.TopicName}
			}
		}
	}

	if len(matchedTopics) == 0 {
		return 0, fmt.Errorf("no assigned topics match pattern '%s'", topicPattern)
	}

	totalStreamed := 0
	for _, topicName := range matchedTopics {
		t := ch.TopicManager.GetTopic(topicName)
		if t != nil && partition >= len(t.Partitions) {
			return totalStreamed, fmt.Errorf("partition %d does not exist for topic '%s' (has %d partitions)", partition, topicName, len(t.Partitions))
		}
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

	memberID := args["member"]
	if memberID == "" {
		return 0, fmt.Errorf("missing member parameter")
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

	if genStr := args["generation"]; genStr != "" {
		generation, err := strconv.ParseInt(genStr, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid generation parameter: %w", err)
		}
		ctx.Generation = int(generation)
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

	if !ch.ValidateOwnership(groupName, memberID, ctx.Generation, partition) {
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
				return streamedCount, err
			}
			if len(newMessages) > 0 {
				messages = newMessages
				break
			}
		}
	}

	batchData, err := util.EncodeBatchMessages(topicName, partition, "1", messages)
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
	const maxPatternLength = 256
	if len(pattern) > maxPatternLength {
		return nil, fmt.Errorf("topic pattern exceeds maximum length of %d characters", maxPatternLength)
	}

	if !strings.Contains(pattern, "*") && !strings.Contains(pattern, "?") {
		if ch.TopicManager.GetTopic(pattern) == nil {
			return nil, fmt.Errorf("topic '%s' does not exist", pattern)
		}
		return []string{pattern}, nil
	}

	escaped := regexp.QuoteMeta(pattern)
	regexPattern := strings.ReplaceAll(escaped, `\*`, ".*")
	regexPattern = strings.ReplaceAll(regexPattern, `\?`, ".")
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

	sort.Strings(matchedTopics)
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

	genStr := args["generation"]
	if genStr != "" {
		generation, err := strconv.ParseInt(genStr, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid generation parameter: %w", err)
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

	if err := ch.checkLeaderOrRedirect(conn); err != nil {
		return err
	}
	if err := ch.checkPartitionAuthorization(topicName, partition); err != nil {
		return err
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

func (ch *CommandHandler) validateStreamSyntax(cmd, raw string) string {
	args := parseKeyValueArgs(cmd[7:])
	if args["topic"] == "" || args["partition"] == "" || args["group"] == "" {
		return ch.fail(raw, "ERROR: invalid STREAM syntax")
	}
	return STREAM_DATA_SIGNAL
}

func (ch *CommandHandler) validateConsumeSyntax(cmd, raw string) string {
	args := parseKeyValueArgs(cmd[8:])
	if args["topic"] == "" || args["partition"] == "" || args["offset"] == "" || args["member"] == "" {
		return ch.fail(raw, "ERROR: invalid CONSUME syntax")
	}
	return STREAM_DATA_SIGNAL
}

// checkLeaderOrRedirect checks if this broker is the leader and writes a redirect error if not.
func (ch *CommandHandler) checkLeaderOrRedirect(conn net.Conn) error {
	if !ch.Config.EnabledDistribution || ch.Cluster.Router == nil {
		return nil
	}

	if ch.Cluster.RaftManager.IsLeader() {
		return nil
	}

	leaderAddr := ch.Cluster.RaftManager.GetLeaderAddress()
	if leaderAddr == "" {
		return fmt.Errorf("no leader available")
	}

	serviceLeader := leaderAddr
	if host, _, splitErr := net.SplitHostPort(leaderAddr); splitErr == nil {
		serviceLeader = net.JoinHostPort(host, strconv.Itoa(ch.Config.BrokerPort))
	}

	errResp := fmt.Sprintf("ERROR: NOT_LEADER LEADER_IS %s", serviceLeader)
	util.Warn("leader redirect: %s", errResp)
	if err := util.WriteWithLength(conn, []byte(errResp)); err != nil {
		return fmt.Errorf("failed to send leader redirect: %w", err)
	}
	return fmt.Errorf("not leader")
}

// checkPartitionAuthorization checks if the client is authorized for the given topic/partition.
func (ch *CommandHandler) checkPartitionAuthorization(topic string, partition int) error {
	if !ch.isAuthorizedForPartition(topic, partition) {
		return fmt.Errorf("ERROR: NOT_AUTHORIZED_FOR_PARTITION %s:%d", topic, partition)
	}
	return nil
}

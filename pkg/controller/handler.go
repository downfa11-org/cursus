package controller

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"

	"github.com/downfa11-org/go-broker/pkg/disk"
	"github.com/downfa11-org/go-broker/pkg/topic"
	"github.com/downfa11-org/go-broker/pkg/types"
	"github.com/downfa11-org/go-broker/util"
)

const STREAM_DATA_SIGNAL = "STREAM_DATA"

type CommandHandler struct {
	TopicManager *topic.TopicManager
	DiskManager  *disk.DiskManager
}

type ConsumeArgs struct {
	Topic     string
	Partition int
	Offset    int
}

func NewCommandHandler(tm *topic.TopicManager, dm *disk.DiskManager) *CommandHandler {
	return &CommandHandler{TopicManager: tm, DiskManager: dm}
}

func (ch *CommandHandler) logCommandResult(cmd, response string) {
	status := "SUCCESS"
	if strings.HasPrefix(response, "ERROR:") {
		status = "FAILURE"
	}

	cleanResponse := strings.ReplaceAll(response, "\n", " ")
	log.Printf("[CMD] %s | Command: [%s] | Response: %s", status, cmd, cleanResponse)
}

// HandleConsumeCommand is responsible for parsing the CONSUME command and streaming messages.
func (ch *CommandHandler) HandleConsumeCommand(conn net.Conn, rawCmd string) (int, error) {
	// Parse CONSUME <topic> <partition> <offset>
	parts := strings.Fields(rawCmd)
	if len(parts) != 4 {
		return 0, fmt.Errorf("invalid CONSUME syntax. Expected: CONSUME <topic> <partition> <offset>")
	}

	topicName := parts[1]
	partition, err := strconv.Atoi(parts[2])
	if err != nil {
		return 0, fmt.Errorf("invalid partition ID: %w", err)
	}
	offset, err := strconv.Atoi(parts[3])
	if err != nil {
		return 0, fmt.Errorf("invalid offset: %w", err)
	}

	dh, err := ch.DiskManager.GetHandler(topicName, partition)
	if err != nil {
		return 0, fmt.Errorf("failed to get disk handler: %w", err)
	}

	messages, err := dh.ReadMessages(offset, 8192)
	if err != nil {
		return 0, fmt.Errorf("failed to read messages from disk: %w", err)
	}

	streamedCount := 0
	for _, msg := range messages {
		msgBytes := []byte(msg.Payload)

		if err := util.WriteWithLength(conn, msgBytes); err != nil {
			return streamedCount, fmt.Errorf("failed to stream message: %w", err)
		}
		streamedCount++
	}

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
		parts := strings.Fields(cmd)
		if len(parts) != 4 {
			resp := "ERROR: invalid CONSUME syntax. Expected: CONSUME <topic> <partition> <offset>"
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
  CREATE <topic> [<partitions>] - create topic (default=4)
  DELETE <topic>                - delete topic
  LIST                          - list all topics
  SUBSCRIBE <topic> [<group>]  - subscribe to an existing topic (default group: "default")
  PUBLISH <topic> <message>     - publish a message
  CONSUME <topic> <pID> <offset> - consume messages (streaming)
  HELP                          - show this help message
  EXIT                          - exit`

	case strings.HasPrefix(strings.ToUpper(cmd), "CREATE "):
		args := strings.Fields(cmd[7:])
		if len(args) == 0 {
			resp = "ERROR: missing topic name"
			break
		}
		topicName := args[0]
		partitions := 4
		if len(args) > 1 {
			n, err := strconv.Atoi(args[1])
			if err != nil || n <= 0 {
				resp = "ERROR: partitions must be a positive integer"
				break
			}
			partitions = n
		}

		t := tm.CreateTopic(topicName, partitions)
		resp = fmt.Sprintf("✅ Topic '%s' now has %d partitions", topicName, len(t.Partitions))

	case strings.HasPrefix(strings.ToUpper(cmd), "DELETE "):
		topicName := strings.TrimSpace(cmd[7:])
		if tm.DeleteTopic(topicName) {
			resp = fmt.Sprintf("🗑️ Topic '%s' deleted", topicName)
			break
		}
		resp = fmt.Sprintf("ERROR: topic '%s' not found", topicName)

	case strings.EqualFold(cmd, "LIST"):
		names := tm.ListTopics()
		if len(names) == 0 {
			resp = "(no topics)"
			break
		}
		resp = strings.Join(names, ", ")

	case strings.HasPrefix(strings.ToUpper(cmd), "SUBSCRIBE "):
		parts := strings.Fields(cmd[10:])
		if len(parts) == 0 {
			resp = "ERROR: missing topic name"
			break
		}

		topicName := parts[0]
		groupName := "default"

		if len(parts) > 1 {
			groupName = parts[1]
		}

		t := tm.GetTopic(topicName)
		if t == nil {
			resp = fmt.Sprintf("ERROR: topic '%s' does not exist", topicName)
			break
		}
		ctx.CurrentTopics[topicName] = struct{}{}
		t.RegisterConsumerGroup(groupName, 1)

		resp = fmt.Sprintf("✅ Subscribed to '%s' with group '%s'", topicName, groupName)

	case strings.HasPrefix(strings.ToUpper(cmd), "PUBLISH "):
		parts := strings.SplitN(cmd[8:], " ", 2)
		if len(parts) < 2 {
			resp = "ERROR: invalid PUBLISH syntax"
			break
		}
		topicName := parts[0]
		payload := parts[1]

		t := tm.GetTopic(topicName)
		if t == nil {
			resp = fmt.Sprintf("ERROR: topic '%s' does not exist", topicName)
			break
		}

		msg := types.Message{
			ID:      util.GenerateID(payload),
			Payload: payload,
			Key:     payload,
		}
		if err := tm.Publish(topicName, msg); err != nil {
			resp = fmt.Sprintf("ERROR: %v", err)
			break
		}
		resp = fmt.Sprintf("📤 Published to '%s'", topicName)

	case strings.EqualFold(cmd, "CONSUME"):
		var output []string
		if len(output) == 0 {
			resp = "(no messages)"
			break
		}
		resp = strings.Join(output, "\n")

	default:
		resp = "ERROR: unknown command: " + cmd + ". Type HELP for available commands."
	}

	ch.logCommandResult(rawCmd, resp)
	return resp
}

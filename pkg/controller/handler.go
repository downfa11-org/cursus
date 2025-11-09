package controller

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/downfa11-org/go-broker/pkg/disk"
	"github.com/downfa11-org/go-broker/pkg/topic"
	"github.com/downfa11-org/go-broker/pkg/types"
	"github.com/downfa11-org/go-broker/util"
)

type CommandHandler struct {
	TopicManager *topic.TopicManager
	DiskManager  *disk.DiskManager
}

func NewCommandHandler(tm *topic.TopicManager, dm *disk.DiskManager) *CommandHandler {
	return &CommandHandler{TopicManager: tm, DiskManager: dm}
}

func (ch *CommandHandler) HandleCommand(rawCmd string, ctx *ClientContext) string {
	cmd := strings.TrimSpace(rawCmd)
	if cmd == "" {
		return "ERROR: empty command"
	}
	if dh, err := ch.DiskManager.GetHandler("broker", 0); err == nil {
		dh.Log("info", "Received command: "+cmd)
	}

	tm := ch.TopicManager

	switch {
	case strings.EqualFold(cmd, "HELP"):
		return `Available commands:
  CREATE <topic> [<partitions>] - create topic (default=4)
  DELETE <topic>                - delete topic
  LIST                          - list all topics
  SUBSCRIBE <topic>             - subscribe to an existing topic
  PUBLISH <topic> <message>     - publish a message
  CONSUME                       - consume messages from subscribed topics
  HELP                          - show this help message
  EXIT                          - exit`

	case strings.HasPrefix(strings.ToUpper(cmd), "CREATE "):
		args := strings.Fields(cmd[7:])
		if len(args) == 0 {
			return "ERROR: missing topic name"
		}
		topicName := args[0]
		partitions := 4
		if len(args) > 1 {
			n, err := strconv.Atoi(args[1])
			if err != nil || n <= 0 {
				return "ERROR: partitions must be a positive integer"
			}
			partitions = n
		}

		t := tm.CreateTopic(topicName, partitions)
		return fmt.Sprintf("✅ Topic '%s' now has %d partitions", topicName, len(t.Partitions))

	case strings.HasPrefix(strings.ToUpper(cmd), "DELETE "):
		topicName := strings.TrimSpace(cmd[7:])
		if tm.DeleteTopic(topicName) {
			return fmt.Sprintf("🗑️ Topic '%s' deleted", topicName)
		}
		return fmt.Sprintf("ERROR: topic '%s' not found", topicName)

	case strings.EqualFold(cmd, "LIST"):
		names := tm.ListTopics()
		if len(names) == 0 {
			return "(no topics)"
		}
		return strings.Join(names, ", ")

	case strings.HasPrefix(strings.ToUpper(cmd), "SUBSCRIBE "):
		topicName := strings.TrimSpace(cmd[10:])
		t := tm.GetTopic(topicName)
		if t == nil {
			return fmt.Sprintf("ERROR: topic '%s' does not exist", topicName)
		}
		ctx.CurrentTopics[topicName] = struct{}{}
		t.RegisterConsumerGroup(ctx.ConsumerGroup, 1)

		if dh, err := ch.DiskManager.GetHandler(topicName, 0); err == nil {
			dh.Log("info", "Subscribed to topic: "+topicName)
		}

		return fmt.Sprintf("✅ Subscribed to '%s'", topicName)

	case strings.HasPrefix(strings.ToUpper(cmd), "PUBLISH "):
		parts := strings.SplitN(cmd[8:], " ", 2)
		if len(parts) < 2 {
			return "ERROR: invalid PUBLISH syntax"
		}
		topicName := parts[0]
		payload := parts[1]

		t := tm.GetTopic(topicName)
		if t == nil {
			return fmt.Sprintf("ERROR: topic '%s' does not exist", topicName)
		}

		msg := types.Message{
			ID:      util.GenerateID(payload),
			Payload: payload,
			Key:     payload,
		}
		tm.Publish(topicName, msg)
		return fmt.Sprintf("📤 Published to '%s'", topicName)

	case strings.EqualFold(cmd, "CONSUME"):
		var output []string
		for topicName := range ctx.CurrentTopics {
			chMsg := tm.Consume(topicName, ctx.ConsumerGroup, ctx.ConsumerIdx)
			if chMsg == nil {
				continue
			}

		msgLoop:
			for i := 0; i < 10; i++ {
				select {
				case msg := <-chMsg:
					output = append(output, fmt.Sprintf("[%s] %s", topicName, msg.Payload))
				case <-time.After(50 * time.Millisecond):
					break msgLoop
				}
			}
		}

		if len(output) == 0 {
			return "(no messages)"
		}
		return strings.Join(output, "\n")

	default:
		return "ERROR: unknown command. Type HELP for available commands."
	}
}

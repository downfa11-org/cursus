package controller

import (
	"fmt"
	"log"
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
	Offset    int
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
	log.Printf("%s | Command: [%s] | Response: %s", status, cmd, cleanResponse)
}

// HandleConsumeCommand is responsible for parsing the CONSUME command and streaming messages.
func (ch *CommandHandler) HandleConsumeCommand(conn net.Conn, rawCmd string, ctx *ClientContext) (int, error) {
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
	requestedOffset, err := strconv.Atoi(parts[3])
	if err != nil {
		return 0, fmt.Errorf("invalid offset: %w", err)
	}

	t := ch.TopicManager.GetTopic(topicName)
	if t == nil {
		return 0, fmt.Errorf("topic '%s' does not exist", topicName)
	}

	dh, err := ch.DiskManager.GetHandler(topicName, partition)
	if err != nil {
		return 0, fmt.Errorf("failed to get disk handler: %w", err)
	}

	startTime := time.Now()

	actualOffset := requestedOffset
	log.Printf("Requested offset: %d for group '%s'", requestedOffset, ctx.ConsumerGroup)

	// check consumer group's committed offset
	if requestedOffset == -1 || requestedOffset == 0 {
		log.Printf("Checking consumer group '%s' committed offset for topic '%s' partition %d",
			ctx.ConsumerGroup, topicName, partition)

		if committedOffset, ok := t.GetCommittedOffset(ctx.ConsumerGroup, partition); ok {
			actualOffset = int(committedOffset)
			log.Printf("[OFFSET] Using consumer group committed offset %d for group '%s', topic '%s', partition %d",
				actualOffset, ctx.ConsumerGroup, topicName, partition)
		} else {
			if ch.OffsetManager != nil {
				savedOffset, err := ch.OffsetManager.GetOffset(ctx.ConsumerGroup, topicName, partition)
				if err == nil && savedOffset >= 0 {
					actualOffset = int(savedOffset)
					log.Printf("[OFFSET] Using OffsetManager saved offset %d for group '%s', topic '%s', partition %d",
						actualOffset, ctx.ConsumerGroup, topicName, partition)
				} else {
					log.Printf("[WARN] Failed to get saved offset, falling back to auto offset: %v", err)
					actualOffset, err = ch.resolveAutoOffset(dh, ctx.ConsumerGroup)
					if err != nil {
						return 0, err
					}
					log.Printf("[OFFSET] Fallback offset applied: %d for group '%s'", actualOffset, ctx.ConsumerGroup)
				}
			} else {
				log.Printf("[WARN] OffsetManager is nil, using auto offset for group '%s'", ctx.ConsumerGroup)
				actualOffset, err = ch.resolveAutoOffset(dh, ctx.ConsumerGroup)
				if err != nil {
					return 0, err
				}
				log.Printf("[OFFSET] Fallback offset applied: %d for group '%s'", actualOffset, ctx.ConsumerGroup)
			}
		}
	} else {
		log.Printf("Requested offset is specified: %d for group '%s'", requestedOffset, ctx.ConsumerGroup)
		log.Printf("Using requested offset as actual offset: %d for group '%s'", actualOffset, ctx.ConsumerGroup)
	}

	maxMessages := DefaultMaxPollRecords
	if ch.Config != nil && ch.Config.MaxPollRecords > 0 {
		maxMessages = ch.Config.MaxPollRecords
	}
	log.Printf("Max messages to fetch: %d", maxMessages)

	messages, err := dh.ReadMessages(actualOffset, maxMessages)
	if err != nil {
		return 0, fmt.Errorf("failed to read messages from disk: %w", err)
	}

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
		if err := t.CommitOffset(ctx.ConsumerGroup, partition, int64(lastOffset)); err != nil {
			log.Printf("[OFFSET_WARN] Failed to commit offset to topic for group '%s': %v", ctx.ConsumerGroup, err)
		} else {
			log.Printf("[OFFSET] Successfully committed offset %d to topic for group '%s', topic '%s', partition %d",
				lastOffset, ctx.ConsumerGroup, topicName, partition)
		}

		if ch.OffsetManager != nil {
			if err := ch.OffsetManager.CommitOffset(ctx.ConsumerGroup, topicName, partition, int64(lastOffset)); err != nil {
				log.Printf("[OFFSET_WARN] Failed to commit offset to OffsetManager for group '%s': %v", ctx.ConsumerGroup, err)
			} else {
				log.Printf("[OFFSET] Successfully committed offset %d to OffsetManager for group '%s', topic '%s', partition %d",
					lastOffset, ctx.ConsumerGroup, topicName, partition)
			}
		}
	}

	duration := time.Since(startTime)
	log.Printf("[METRICS] Streamed %d messages from topic '%s' partition %d in %v",
		streamedCount, topicName, partition, duration)

	return streamedCount, nil
}

// resolveAutoOffset applies the auto.offset.reset policy to determine the starting offset
func (ch *CommandHandler) resolveAutoOffset(dh *disk.DiskHandler, groupName string) (int, error) {
	if ch.Config != nil && ch.Config.AutoOffsetReset == "earliest" {
		log.Printf("[OFFSET] Using 'earliest' (0) for group '%s'", groupName)
		return 0, nil
	}
	if ch.Config == nil {
		log.Printf("[WARN] Config is nil, defaulting auto.offset.reset to 'latest' for group '%s'", groupName)
	} else if ch.Config.AutoOffsetReset != "latest" {
		log.Printf("[WARN] Unknown auto.offset.reset '%s', defaulting to 'latest' for group '%s'",
			ch.Config.AutoOffsetReset, groupName)
	}

	// "latest"
	latestOffset, err := dh.GetLatestOffset()
	if err != nil {
		return 0, fmt.Errorf("failed to get latest offset: %w", err)
	}
	log.Printf("[OFFSET] Using 'latest' (%d) for group '%s'", latestOffset, groupName)
	return latestOffset, nil
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
  SETGROUP <group-name>          - set consumer group name  
  CREATE <topic> [<partitions>]  - create topic (default=4)  
  DELETE <topic>                 - delete topic  
  LIST                           - list all topics  
  SUBSCRIBE <topic> [<group>]    - subscribe to an existing topic (default group: "default")  
  PUBLISH <topic> <message>      - publish a message  
  CONSUME <topic> <pID> <offset> - consume messages (streaming)  
  JOIN_GROUP <group> <consumer>  - join consumer group and get partition assignments  
  LEAVE_GROUP <group> <consumer> - leave consumer group  
  HEARTBEAT <group> <consumer>   - send heartbeat to coordinator  
  COMMIT_OFFSET <topic> <p> <o>  - commit offset for partition  
  HELP                           - show this help message  
  EXIT                           - exit`

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

		if ch.Coordinator != nil {
			err := ch.Coordinator.RegisterGroup(topicName, "default-group", partitions)
			if err != nil {
				log.Printf("[WARN] Failed to register default group: %v", err)
			}
		}

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

	case strings.HasPrefix(strings.ToUpper(cmd), "SETGROUP "):
		groupName := strings.TrimSpace(cmd[9:])
		if groupName == "" {
			resp = "ERROR: group name cannot be empty"
			break
		}
		ctx.SetConsumerGroup(groupName)
		resp = fmt.Sprintf("✅ Consumer group set to '%s'", groupName)
	case strings.HasPrefix(strings.ToUpper(cmd), "REGISTER_GROUP "):
		args := strings.Fields(cmd[15:])
		if len(args) < 2 {
			resp = "ERROR: REGISTER_GROUP requires <topic> <group>"
			break
		}
		topicName := args[0]
		groupName := args[1]

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
		args := strings.Fields(cmd[11:])
		if len(args) < 2 {
			resp = "ERROR: JOIN_GROUP requires <groupID> <consumerID>"
			break
		}
		groupName := args[0]
		consumerID := args[1]

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
		args := strings.Fields(cmd[12:])
		if len(args) < 2 {
			resp = "ERROR: LEAVE_GROUP requires <groupID> <consumerID>"
			break
		}
		groupName := args[0]
		consumerID := args[1]

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

	case strings.HasPrefix(strings.ToUpper(cmd), "HEARTBEAT "):
		args := strings.Fields(cmd[10:])
		if len(args) < 2 {
			resp = "ERROR: HEARTBEAT requires <group> <consumer_id>"
			break
		}
		groupName := args[0]
		consumerID := args[1]

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
		args := strings.Fields(cmd[14:])
		if len(args) < 3 {
			resp = "ERROR: COMMIT_OFFSET requires <topic> <partition> <offset>"
			break
		}
		topicName := args[0]
		partition, err := strconv.Atoi(args[1])
		if err != nil {
			resp = "ERROR: invalid partition"
			break
		}
		offset, err := strconv.Atoi(args[2])
		if err != nil {
			resp = "ERROR: invalid offset"
			break
		}

		if ch.OffsetManager != nil {
			err := ch.OffsetManager.CommitOffset(ctx.ConsumerGroup, topicName, partition, int64(offset))
			if err != nil {
				resp = fmt.Sprintf("ERROR: %v", err)
			} else {
				resp = "OK"
			}
		} else {
			resp = "ERROR: offset manager not available"
		}

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

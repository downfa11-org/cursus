package fsm

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/downfa11-org/cursus/pkg/types"
)

type MessageCommand struct {
	Topic     string
	Partition int
	Messages  []types.Message
}

func parseMessageCommand(jsonData string) (*MessageCommand, error) {
	raw := make(map[string]interface{})

	decoder := json.NewDecoder(strings.NewReader(jsonData))
	decoder.UseNumber()

	if err := decoder.Decode(&raw); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}

	topic, err := getStringField(raw, "Topic")
	if err != nil {
		return nil, err
	}

	partition, err := getIntField(raw, "Partition")
	if err != nil {
		return nil, err
	}

	var messages []interface{}
	if raw["Messages"] != nil {
		var ok bool
		messages, ok = raw["Messages"].([]interface{})
		if !ok {
			return nil, fmt.Errorf("Messages must be array")
		}
	} else {
		messages = []interface{}{raw}
	}

	parsed := make([]types.Message, 0, len(messages))
	for i, m := range messages {
		msgData, ok := m.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid message at index %d", i)
		}

		msg, err := parseSingleMessage(msgData)
		if err != nil {
			return nil, fmt.Errorf("message[%d]: %w", i, err)
		}

		parsed = append(parsed, msg)
	}

	return &MessageCommand{
		Topic:     topic,
		Partition: partition,
		Messages:  parsed,
	}, nil
}

func parseSingleMessage(data map[string]interface{}) (types.Message, error) {
	payload, err := getOptionalStringField(data, "Payload")
	if err != nil {
		return types.Message{}, err
	}

	producerID, err := getOptionalStringField(data, "ProducerID")
	if err != nil {
		return types.Message{}, err
	}

	offset, err := getOptionalInt64Field(data, "Offset")
	if err != nil {
		return types.Message{}, err
	}
	if offset < 0 {
		return types.Message{}, fmt.Errorf("negative offset")
	}

	seqNum, err := getOptionalUint64Field(data, "SeqNum")
	if err != nil {
		return types.Message{}, err
	}

	epoch, err := getOptionalInt64Field(data, "Epoch")
	if err != nil {
		return types.Message{}, err
	}

	return types.Message{
		Payload:    payload,
		ProducerID: producerID,
		Offset:     uint64(offset),
		SeqNum:     seqNum,
		Epoch:      epoch,
	}, nil
}

func validateMessageCommand(cmd *MessageCommand) error {
	if len(cmd.Messages) == 0 {
		return fmt.Errorf("empty message batch")
	}

	base := cmd.Messages[0]

	for i, m := range cmd.Messages {
		if m.ProducerID != base.ProducerID {
			return fmt.Errorf("mixed producer at %d", i)
		}
		if m.Epoch != base.Epoch {
			return fmt.Errorf("mixed epoch at %d", i)
		}
	}

	return nil
}

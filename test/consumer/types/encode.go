package types

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// todo. need to migrated util/encode.go DecodeBathMessage
func DecodeBatchMessages(data []byte) (*Batch, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("data too short")
	}

	reader := bytes.NewReader(data)

	var magic uint16
	if err := binary.Read(reader, binary.BigEndian, &magic); err != nil {
		return nil, fmt.Errorf("failed to read magic number: %w", err)
	}
	if magic != 0xBA7C {
		return nil, fmt.Errorf("invalid magic number: %x", magic)
	}

	var topicLen uint16
	if err := binary.Read(reader, binary.BigEndian, &topicLen); err != nil {
		return nil, fmt.Errorf("failed to read topic length: %w", err)
	}
	topicBytes := make([]byte, topicLen)
	if _, err := io.ReadFull(reader, topicBytes); err != nil {
		return nil, fmt.Errorf("failed to read topic bytes: %w", err)
	}

	var partition int32
	if err := binary.Read(reader, binary.BigEndian, &partition); err != nil {
		return nil, fmt.Errorf("failed to read partition: %w", err)
	}

	var acksLen uint8
	if err := binary.Read(reader, binary.BigEndian, &acksLen); err != nil {
		return nil, fmt.Errorf("failed to read acks length: %w", err)
	}
	acksBytes := make([]byte, acksLen)
	if _, err := io.ReadFull(reader, acksBytes); err != nil {
		return nil, fmt.Errorf("failed to read acks bytes: %w", err)
	}

	var batchStart, batchEnd uint64
	if err := binary.Read(reader, binary.BigEndian, &batchStart); err != nil {
		return nil, fmt.Errorf("failed to read batch start: %w", err)
	}
	if err := binary.Read(reader, binary.BigEndian, &batchEnd); err != nil {
		return nil, fmt.Errorf("failed to read batch end: %w", err)
	}

	var msgCount int32
	if err := binary.Read(reader, binary.BigEndian, &msgCount); err != nil {
		return nil, fmt.Errorf("failed to read message count: %w", err)
	}

	batch := &Batch{
		Topic:     string(topicBytes),
		Partition: int(partition),
		Acks:      string(acksBytes),
		Messages:  make([]Message, 0, msgCount),
	}

	for i := 0; i < int(msgCount); i++ {
		var m Message

		if err := binary.Read(reader, binary.BigEndian, &m.Offset); err != nil {
			return nil, fmt.Errorf("failed to read message[%d] offset: %w", i, err)
		}
		if err := binary.Read(reader, binary.BigEndian, &m.SeqNum); err != nil {
			return nil, fmt.Errorf("failed to read message[%d] seqNum: %w", i, err)
		}

		var pIdLen uint16
		if err := binary.Read(reader, binary.BigEndian, &pIdLen); err != nil {
			return nil, fmt.Errorf("failed to read message[%d] producerID length: %w", i, err)
		}
		pIdBytes := make([]byte, pIdLen)
		if _, err := io.ReadFull(reader, pIdBytes); err != nil {
			return nil, fmt.Errorf("failed to read message[%d] producerID bytes: %w", i, err)
		}
		m.ProducerID = string(pIdBytes)

		var keyLen uint16
		if err := binary.Read(reader, binary.BigEndian, &keyLen); err != nil {
			return nil, fmt.Errorf("failed to read message[%d] key length: %w", i, err)
		}
		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(reader, keyBytes); err != nil {
			return nil, fmt.Errorf("failed to read message[%d] key bytes: %w", i, err)
		}
		m.Key = string(keyBytes)

		if err := binary.Read(reader, binary.BigEndian, &m.Epoch); err != nil {
			return nil, fmt.Errorf("failed to read message[%d] epoch: %w", i, err)
		}

		var payloadLen uint32
		if err := binary.Read(reader, binary.BigEndian, &payloadLen); err != nil {
			return nil, fmt.Errorf("failed to read message[%d] payload length: %w", i, err)
		}
		payloadBytes := make([]byte, payloadLen)
		if _, err := io.ReadFull(reader, payloadBytes); err != nil {
			return nil, fmt.Errorf("failed to read message[%d] payload bytes: %w", i, err)
		}
		m.Payload = string(payloadBytes)

		batch.Messages = append(batch.Messages, m)
	}

	return batch, nil
}

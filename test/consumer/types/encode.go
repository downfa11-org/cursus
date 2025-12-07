package types

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// DecodeBatchMessages decodes a batch encoded by EncodeBatchMessages
func DecodeBatchMessages(data []byte) (*Batch, error) {
	if len(data) < 2 || data[0] != 0xBA || data[1] != 0x7C {
		return nil, fmt.Errorf("invalid batch header")
	}
	data = data[2:]

	offset := 0
	read := func(size int) ([]byte, error) {
		if offset+size > len(data) {
			return nil, errors.New("data too short")
		}
		b := data[offset : offset+size]
		offset += size
		return b, nil
	}

	// topic
	topicLenBytes, err := read(2)
	if err != nil {
		return nil, err
	}
	topicLen := int(binary.BigEndian.Uint16(topicLenBytes))
	topicBytes, err := read(topicLen)
	if err != nil {
		return nil, err
	}
	topic := string(topicBytes)

	// partition
	partBytes, err := read(4)
	if err != nil {
		return nil, err
	}
	partition := int(binary.BigEndian.Uint32(partBytes))

	// batch start/end
	batchStartBytes, err := read(8)
	if err != nil {
		return nil, err
	}
	batchStart := binary.BigEndian.Uint64(batchStartBytes)

	batchEndBytes, err := read(8)
	if err != nil {
		return nil, err
	}
	batchEnd := binary.BigEndian.Uint64(batchEndBytes)

	// num messages
	numMsgsBytes, err := read(4)
	if err != nil {
		return nil, err
	}
	numMsgs := int(binary.BigEndian.Uint32(numMsgsBytes))

	msgs := make([]Message, 0, numMsgs)

	for i := 0; i < numMsgs; i++ {
		// offset
		offsetBytes, err := read(8)
		if err != nil {
			return nil, err
		}
		currentOffset := binary.BigEndian.Uint64(offsetBytes)

		// seqNum (8 bytes)
		seqBytes, err := read(8)
		if err != nil {
			return nil, err
		}
		seq := binary.BigEndian.Uint64(seqBytes)

		// producerID
		producerIDLenBytes, err := read(2)
		if err != nil {
			return nil, err
		}
		producerIDLen := int(binary.BigEndian.Uint16(producerIDLenBytes))
		producerIDBytes, err := read(producerIDLen)
		if err != nil {
			return nil, err
		}

		// key
		keyLenBytes, err := read(2)
		if err != nil {
			return nil, err
		}
		keyLen := int(binary.BigEndian.Uint16(keyLenBytes))
		keyBytes, err := read(keyLen)
		if err != nil {
			return nil, err
		}

		// epoch (8 bytes)
		epochBytes, err := read(8)
		if err != nil {
			return nil, err
		}
		epoch := int64(binary.BigEndian.Uint64(epochBytes))

		// payload
		payloadLenBytes, err := read(4)
		if err != nil {
			return nil, err
		}
		payloadLen := int(binary.BigEndian.Uint32(payloadLenBytes))
		payloadBytes, err := read(payloadLen)
		if err != nil {
			return nil, err
		}

		msgs = append(msgs, Message{
			Offset:     currentOffset,
			SeqNum:     seq,
			ProducerID: string(producerIDBytes),
			Key:        string(keyBytes),
			Epoch:      epoch,
			Payload:    string(payloadBytes),
		})
	}

	return &Batch{
		Topic:      topic,
		Partition:  partition,
		BatchStart: batchStart,
		BatchEnd:   batchEnd,
		Messages:   msgs,
	}, nil
}

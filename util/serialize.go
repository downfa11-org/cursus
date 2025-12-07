package util

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/downfa11-org/go-broker/pkg/types"
)

// EncodeMessage serializes topic and payload into bytes.
func EncodeMessage(topic string, payload string) []byte {
	topicBytes := []byte(topic)
	payloadBytes := []byte(payload)
	data := make([]byte, 2+len(topicBytes)+len(payloadBytes))
	binary.BigEndian.PutUint16(data[:2], uint16(len(topicBytes)))
	copy(data[2:2+len(topicBytes)], topicBytes)
	copy(data[2+len(topicBytes):], payloadBytes)
	return data
}

// DecodeMessage deserializes bytes into topic and payload.
func DecodeMessage(data []byte) (string, string) {
	if len(data) < 2 {
		return "", ""
	}
	topicLen := binary.BigEndian.Uint16(data[:2])
	if int(topicLen)+2 > len(data) {
		return "", ""
	}
	topic := string(data[2 : 2+topicLen])
	payload := string(data[2+int(topicLen):])
	return topic, payload
}

// WriteWithLength writes data with a 4-byte length prefix.
func WriteWithLength(conn net.Conn, data []byte) error {
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
	if _, err := conn.Write(lenBuf); err != nil {
		return fmt.Errorf("write length: %w", err)
	}
	if _, err := conn.Write(data); err != nil {
		return fmt.Errorf("write body: %w", err)
	}
	return nil
}

// ReadWithLength reads data with a 4-byte length prefix.
func ReadWithLength(conn net.Conn) ([]byte, error) {
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, lenBuf); err != nil {
		return nil, fmt.Errorf("read length: %w", err)
	}
	length := binary.BigEndian.Uint32(lenBuf)
	buf := make([]byte, length)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	return buf, nil
}

func EncodeBatchMessages(topic string, partition int, msgs []types.Message) ([]byte, error) {
	var buf bytes.Buffer
	buf.Write([]byte{0xBA, 0x7C})

	write := func(v any) error {
		if err := binary.Write(&buf, binary.BigEndian, v); err != nil {
			return fmt.Errorf("encode value failed: %w", err)
		}
		return nil
	}

	// topic
	topicBytes := []byte(topic)
	if err := write(uint16(len(topicBytes))); err != nil {
		return nil, err
	}
	if _, err := buf.Write(topicBytes); err != nil {
		return nil, fmt.Errorf("write topic bytes failed: %w", err)
	}

	// partition
	if err := write(int32(partition)); err != nil {
		return nil, err
	}

	// batch start/end seqNum
	var batchStart, batchEnd uint64
	if len(msgs) > 0 {
		batchStart = msgs[0].SeqNum
		batchEnd = msgs[len(msgs)-1].SeqNum
	}
	if err := write(batchStart); err != nil {
		return nil, err
	}
	if err := write(batchEnd); err != nil {
		return nil, err
	}

	if err := write(int32(len(msgs))); err != nil {
		return nil, err
	}

	for _, m := range msgs {
		// offset
		if err := write(m.Offset); err != nil {
			return nil, err
		}

		// seqNum
		if err := write(m.SeqNum); err != nil {
			return nil, err
		}

		// producerID
		producerIDBytes := []byte(m.ProducerID)
		if err := write(uint16(len(producerIDBytes))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(producerIDBytes); err != nil {
			return nil, err
		}

		// key
		keyBytes := []byte(m.Key)
		if err := write(uint16(len(keyBytes))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(keyBytes); err != nil {
			return nil, err
		}

		// epoch
		if err := write(m.Epoch); err != nil {
			return nil, err
		}

		// payload
		payloadBytes := []byte(m.Payload)
		if err := write(uint32(len(payloadBytes))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(payloadBytes); err != nil {
			return nil, fmt.Errorf("write payload bytes failed: %w", err)
		}
	}

	return buf.Bytes(), nil
}

// DecodeBatchMessages decodes a batch encoded by EncodeBatchMessages
func DecodeBatchMessages(data []byte) (*types.Batch, error) {
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

	msgs := make([]types.Message, 0, numMsgs)

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

		msgs = append(msgs, types.Message{
			Offset:     currentOffset,
			SeqNum:     seq,
			ProducerID: string(producerIDBytes),
			Key:        string(keyBytes),
			Epoch:      epoch,
			Payload:    string(payloadBytes),
		})
	}

	return &types.Batch{
		Topic:      topic,
		Partition:  partition,
		BatchStart: batchStart,
		BatchEnd:   batchEnd,
		Messages:   msgs,
	}, nil
}

// serializeMessage converts types.Message to serialized bytes
func SerializeMessage(msg types.Message) ([]byte, error) {
	var buf bytes.Buffer
	var err error

	// ProducerID (length + string)
	producerBytes := []byte(msg.ProducerID)
	if err = binary.Write(&buf, binary.BigEndian, uint16(len(producerBytes))); err != nil {
		return nil, fmt.Errorf("write producer length: %w", err)
	}
	if _, err = buf.Write(producerBytes); err != nil {
		return nil, fmt.Errorf("write producer bytes: %w", err)
	}

	// SeqNum (8 bytes)
	if err = binary.Write(&buf, binary.BigEndian, msg.SeqNum); err != nil {
		return nil, fmt.Errorf("write sequence number: %w", err)
	}

	// Payload (length + string)
	payloadBytes := []byte(msg.Payload)
	if err = binary.Write(&buf, binary.BigEndian, uint32(len(payloadBytes))); err != nil {
		return nil, fmt.Errorf("write payload length: %w", err)
	}
	if _, err = buf.Write(payloadBytes); err != nil {
		return nil, fmt.Errorf("write payload bytes: %w", err)
	}

	// Key (length + string)
	keyBytes := []byte(msg.Key)
	if err = binary.Write(&buf, binary.BigEndian, uint16(len(keyBytes))); err != nil {
		return nil, fmt.Errorf("write key length: %w", err)
	}
	if _, err = buf.Write(keyBytes); err != nil {
		return nil, fmt.Errorf("write key bytes: %w", err)
	}

	// Epoch (8 bytes)
	if err = binary.Write(&buf, binary.BigEndian, msg.Epoch); err != nil {
		return nil, fmt.Errorf("write epoch: %w", err)
	}

	return buf.Bytes(), nil
}

// deserializeMessage converts bytes back to types.Message
func DeserializeMessage(data []byte) (types.Message, error) {
	var msg types.Message
	offset := 0

	// ProducerID
	producerLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2
	msg.ProducerID = string(data[offset : offset+producerLen])
	offset += producerLen

	// SeqNum (8 bytes)
	msg.SeqNum = binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8

	// Payload
	payloadLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4
	msg.Payload = string(data[offset : offset+payloadLen])
	offset += payloadLen

	// Key
	keyLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2
	msg.Key = string(data[offset : offset+keyLen])
	offset += keyLen

	// Epoch (8 bytes)
	msg.Epoch = int64(binary.BigEndian.Uint64(data[offset : offset+8]))

	return msg, nil
}

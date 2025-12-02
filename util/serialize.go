package util

import (
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

type Batch struct {
	Topic      string
	Partition  int32
	BatchStart uint64
	BatchEnd   uint64
	Acks       int
	Messages   []types.Message
}

// DecodeBatchMessages decodes a batch encoded by EncodeBatchMessages
func DecodeBatchMessages(data []byte) (*Batch, error) {
	offset := 0
	read := func(size int) ([]byte, error) {
		if offset+size > len(data) {
			return nil, errors.New("data too short")
		}
		b := data[offset : offset+size]
		offset += size
		return b, nil
	}

	// Topic
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

	// Partition
	partBytes, err := read(4)
	if err != nil {
		return nil, err
	}
	partition := int32(binary.BigEndian.Uint32(partBytes))

	// Batch start/end
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

	// Num messages
	numMsgsBytes, err := read(4)
	if err != nil {
		return nil, err
	}
	numMsgs := int(binary.BigEndian.Uint32(numMsgsBytes))

	msgs := make([]types.Message, 0, numMsgs)
	for i := 0; i < numMsgs; i++ {
		seqBytes, err := read(8)
		if err != nil {
			return nil, err
		}
		seq := binary.BigEndian.Uint64(seqBytes)

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
			SeqNum:  seq,
			Payload: string(payloadBytes),
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

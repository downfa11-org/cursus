package util

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
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

// EncodeIdempotentMessage encodes a message with Producer ID, SeqNum, and Epoch for exactly-once semantics
func EncodeIdempotentMessage(topic, payload, producerID string, seqNum uint64, epoch int64) []byte {
	// [producerID_length][producerID][seqNum][epoch][topic_length][topic][payload_length][payload]

	producerIDBytes := []byte(producerID)
	topicBytes := []byte(topic)
	payloadBytes := []byte(payload)

	// Calculate total size
	totalSize := 2 + len(producerIDBytes) + 8 + 8 + 2 + len(topicBytes) + 2 + len(payloadBytes)
	buf := make([]byte, totalSize)

	offset := 0

	// Write producer ID
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(producerIDBytes)))
	offset += 2
	copy(buf[offset:], producerIDBytes)
	offset += len(producerIDBytes)

	// Write sequence number
	binary.BigEndian.PutUint64(buf[offset:], seqNum)
	offset += 8

	// Write epoch
	binary.BigEndian.PutUint64(buf[offset:], uint64(epoch))
	offset += 8

	// Write topic
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(topicBytes)))
	offset += 2
	copy(buf[offset:], topicBytes)
	offset += len(topicBytes)

	// Write payload
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(payloadBytes)))
	offset += 2
	copy(buf[offset:], payloadBytes)

	return buf
}

// DecodeIdempotentMessage decodes a message with Producer ID, SeqNum, and Epoch
func DecodeIdempotentMessage(data []byte) (producerID string, seqNum uint64, epoch int64, topic string, payload string, err error) {
	if len(data) < 14 { // Minimum: 2 (producerID len) + 8 (seqNum) + 8 (epoch) + 2 (topic len) + 2 (payload len)
		return "", 0, 0, "", "", fmt.Errorf("data too short")
	}

	offset := 0

	// Read producer ID
	producerIDLen := binary.BigEndian.Uint16(data[offset:])
	offset += 2
	if offset+int(producerIDLen) > len(data) {
		return "", 0, 0, "", "", fmt.Errorf("invalid producer ID length")
	}
	producerID = string(data[offset : offset+int(producerIDLen)])
	offset += int(producerIDLen)

	// Read sequence number
	if offset+8 > len(data) {
		return "", 0, 0, "", "", fmt.Errorf("invalid seqNum")
	}
	seqNum = binary.BigEndian.Uint64(data[offset:])
	offset += 8

	// Read epoch
	if offset+8 > len(data) {
		return "", 0, 0, "", "", fmt.Errorf("invalid epoch")
	}
	epoch = int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8

	// Read topic
	if offset+2 > len(data) {
		return "", 0, 0, "", "", fmt.Errorf("invalid topic length")
	}
	topicLen := binary.BigEndian.Uint16(data[offset:])
	offset += 2
	if offset+int(topicLen) > len(data) {
		return "", 0, 0, "", "", fmt.Errorf("invalid topic")
	}
	topic = string(data[offset : offset+int(topicLen)])
	offset += int(topicLen)

	// Read payload
	if offset+2 > len(data) {
		return "", 0, 0, "", "", fmt.Errorf("invalid payload length")
	}
	payloadLen := binary.BigEndian.Uint16(data[offset:])
	offset += 2
	if offset+int(payloadLen) > len(data) {
		return "", 0, 0, "", "", fmt.Errorf("invalid payload")
	}
	payload = string(data[offset : offset+int(payloadLen)])

	return producerID, seqNum, epoch, topic, payload, nil
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

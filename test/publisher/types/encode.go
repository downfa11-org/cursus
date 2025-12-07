package types

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
)

// todo. need to migrated util/serialize.go EncodeBathMessage
func EncodeBatchMessages(topic string, partition int, msgs []Message) ([]byte, error) {
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
	if len(topicBytes) > 0xFFFF {
		return nil, fmt.Errorf("topic too long: %d bytes", len(topicBytes))
	}
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
		if len(producerIDBytes) > 0xFFFF {
			return nil, fmt.Errorf("producerID too long: %d bytes", len(producerIDBytes))
		}
		if err := write(uint16(len(producerIDBytes))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(producerIDBytes); err != nil {
			return nil, err
		}

		// key
		keyBytes := []byte(m.Key)
		if len(keyBytes) > 0xFFFF {
			return nil, fmt.Errorf("key too long: %d bytes", len(keyBytes))
		}
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
		if len(payloadBytes) > 0xFFFFFFFF {
			return nil, fmt.Errorf("payload too large: %d bytes", len(payloadBytes))
		}
		if err := write(uint32(len(payloadBytes))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(payloadBytes); err != nil {
			return nil, fmt.Errorf("write payload bytes failed: %w", err)
		}
	}

	return buf.Bytes(), nil
}

func DecompressMessage(data []byte) ([]byte, error) {
	gr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer gr.Close()
	return io.ReadAll(gr)
}

func CompressMessage(data []byte, enableGzip bool) ([]byte, error) {
	if !enableGzip {
		return data, nil
	}

	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	if _, err := gw.Write(data); err != nil {
		return nil, err
	}
	if err := gw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

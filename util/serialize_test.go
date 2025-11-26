package util_test

import (
	"testing"

	"github.com/downfa11-org/go-broker/util"
)

func TestEncodeDecodeMessage(t *testing.T) {
	topic := "default"
	payload := "hello world"

	data := util.EncodeMessage(topic, payload)
	if len(data) != 2+len(topic)+len(payload) {
		t.Errorf("Unexpected encoded length: got %d", len(data))
	}

	decodedTopic, decodedPayload := util.DecodeMessage(data)
	if decodedTopic != topic {
		t.Errorf("Expected topic %s, got %s", topic, decodedTopic)
	}
	if decodedPayload != payload {
		t.Errorf("Expected payload %s, got %s", payload, decodedPayload)
	}
}

func TestDecodeMessageInvalidData(t *testing.T) {
	empty := []byte{}
	topic, payload := util.DecodeMessage(empty)
	if topic != "" || payload != "" {
		t.Errorf("Expected empty topic/payload, got %s/%s", topic, payload)
	}

	short := []byte{0x01}
	topic, payload = util.DecodeMessage(short)
	if topic != "" || payload != "" {
		t.Errorf("Expected empty topic/payload for short data, got %s/%s", topic, payload)
	}

	// topicLen > data length
	data := []byte{0x00, 0x05, 'a'}
	topic, payload = util.DecodeMessage(data)
	if topic != "" || payload != "" {
		t.Errorf("Expected empty topic/payload for invalid length, got %s/%s", topic, payload)
	}
}

func TestEncodeDecodeIdempotentMessage(t *testing.T) {
	producerID := "producer-123"
	seqNum := uint64(42)
	epoch := int64(9)
	topic := "event"
	payload := "payload-data"

	data := util.EncodeIdempotentMessage(topic, payload, producerID, seqNum, epoch)

	decPID, decSeq, decEpoch, decTopic, decPayload, err :=
		util.DecodeIdempotentMessage(data)

	if err != nil {
		t.Fatalf("DecodeIdempotentMessage returned error: %v", err)
	}

	if decPID != producerID {
		t.Errorf("ProducerID mismatch: expected %s, got %s", producerID, decPID)
	}
	if decSeq != seqNum {
		t.Errorf("SeqNum mismatch: expected %d, got %d", seqNum, decSeq)
	}
	if decEpoch != epoch {
		t.Errorf("Epoch mismatch: expected %d, got %d", epoch, decEpoch)
	}
	if decTopic != topic {
		t.Errorf("Topic mismatch: expected %s, got %s", topic, decTopic)
	}
	if decPayload != payload {
		t.Errorf("Payload mismatch: expected %s, got %s", payload, decPayload)
	}
}

func TestDecodeIdempotentMessageCorruptedData(t *testing.T) {
	// Completely broken data
	data := []byte{0x00}

	_, _, _, _, _, err := util.DecodeIdempotentMessage(data)
	if err == nil {
		t.Errorf("Expected error for corrupted data, got nil")
	}
}

package util_test

import (
	"math"
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

func TestIdempotentMessageEmptyFields(t *testing.T) {
	data := util.EncodeIdempotentMessage("", "", "", 0, 0)
	pid, seq, epoch, topic, payload, err := util.DecodeIdempotentMessage(data)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pid != "" || seq != 0 || epoch != 0 || topic != "" || payload != "" {
		t.Errorf("Unexpected values: pid=%s seq=%d epoch=%d topic=%s payload=%s",
			pid, seq, epoch, topic, payload)
	}
}

func TestIdempotentMessageExtremeNumbers(t *testing.T) {
	producerID := "id"
	topic := "t"
	payload := "p"

	data := util.EncodeIdempotentMessage(
		topic, payload, producerID,
		math.MaxUint64, math.MinInt64,
	)

	_, seq, epoch, _, _, err := util.DecodeIdempotentMessage(data)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if seq != math.MaxUint64 {
		t.Errorf("expected %v, got %d", uint64(math.MaxUint64), seq)
	}
	if epoch != math.MinInt64 {
		t.Errorf("expected %d, got %d", math.MinInt64, epoch)
	}
}

func TestUnicodeMessage(t *testing.T) {
	topic := "🚀"
	payload := "🌏"
	producerID := "🔑"

	data := util.EncodeIdempotentMessage(topic, payload, producerID, 1, 1)
	pid, _, _, decTopic, decPayload, err := util.DecodeIdempotentMessage(data)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pid != producerID || decTopic != topic || decPayload != payload {
		t.Errorf("unicode round-trip failed")
	}
}

func TestIdempotentMessagePartialTruncation(t *testing.T) {
	topic := "topic"
	payload := "payload"
	producerID := "pid"

	data := util.EncodeIdempotentMessage(topic, payload, producerID, 10, 20)
	corrupted := data[:len(data)/2]

	_, _, _, _, _, err := util.DecodeIdempotentMessage(corrupted)
	if err == nil {
		t.Errorf("expected error for truncated message")
	}
}

func TestIdempotentMessageCorruptedLengthField(t *testing.T) {
	topic := "topic"
	payload := "payload"
	producerID := "pid"

	data := util.EncodeIdempotentMessage(topic, payload, producerID, 10, 20)
	data[0] = 0xFF
	data[1] = 0xFF

	_, _, _, _, _, err := util.DecodeIdempotentMessage(data)
	if err == nil {
		t.Errorf("expected error for corrupted length field")
	}
}

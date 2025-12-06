package types_test

import (
	"encoding/json"
	"testing"

	"github.com/downfa11-org/go-broker/publisher/types"
)

func TestMessageStruct(t *testing.T) {
	msg := types.Message{
		ID:         123,
		ProducerID: "producer-1",
		SeqNum:     42,
		Payload:    "hello",
		Offset:     100,
		Key:        "key1",
		Epoch:      987654321,
		RetryCount: 2,
		Retry:      true,
	}

	if msg.ID != 123 || msg.SeqNum != 42 || msg.Payload != "hello" {
		t.Errorf("Message struct fields not set correctly")
	}

	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal Message: %v", err)
	}

	var decoded types.Message
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal Message: %v", err)
	}

	if decoded.SeqNum != msg.SeqNum || decoded.Payload != msg.Payload {
		t.Errorf("Decoded Message mismatch: got %+v, want %+v", decoded, msg)
	}
}

func TestAckResponseStruct(t *testing.T) {
	ack := types.AckResponse{
		Status:        "OK",
		LastOffset:    50,
		ProducerEpoch: 1001,
		ProducerID:    "producer-1",
		SeqStart:      1,
		SeqEnd:        10,
		ErrorMsg:      "",
	}

	if ack.Status != "OK" || ack.SeqEnd != 10 {
		t.Errorf("AckResponse struct fields not set correctly")
	}

	data, err := json.Marshal(ack)
	if err != nil {
		t.Fatalf("Failed to marshal AckResponse: %v", err)
	}

	var decoded types.AckResponse
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal AckResponse: %v", err)
	}

	if decoded.ProducerEpoch != ack.ProducerEpoch || decoded.SeqStart != ack.SeqStart {
		t.Errorf("Decoded AckResponse mismatch: got %+v, want %+v", decoded, ack)
	}
}

func TestAckResponseWithError(t *testing.T) {
	ack := types.AckResponse{
		Status:   "FAIL",
		ErrorMsg: "something went wrong",
	}

	data, err := json.Marshal(ack)
	if err != nil {
		t.Fatalf("Failed to marshal AckResponse: %v", err)
	}

	var decoded types.AckResponse
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal AckResponse: %v", err)
	}

	if decoded.ErrorMsg != "something went wrong" {
		t.Errorf("ErrorMsg not preserved, got %s", decoded.ErrorMsg)
	}
}

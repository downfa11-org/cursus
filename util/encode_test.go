package util_test

import (
	"testing"

	"github.com/downfa11-org/go-broker/pkg/types"
	"github.com/downfa11-org/go-broker/util"
)

func TestEncodeDecodeMessage(t *testing.T) {
	topic := "default"
	payload := "hello world"

	data := util.EncodeMessage(topic, payload)

	// 2(topicLen) + len(topic) + len(payload)
	expectedLen := 2 + len(topic) + len(payload)
	if len(data) != expectedLen {
		t.Errorf("Unexpected encoded length: got %d, want %d", len(data), expectedLen)
	}

	decodedTopic, decodedPayload, err := util.DecodeMessage(data)
	if err != nil {
		t.Fatalf("DecodeMessage failed unexpectedly: %v", err)
	}
	if decodedTopic != topic {
		t.Errorf("Expected topic %s, got %s", topic, decodedTopic)
	}
	if decodedPayload != payload {
		t.Errorf("Expected payload %s, got %s", payload, decodedPayload)
	}
}

func TestDecodeMessageInvalidData(t *testing.T) {
	t.Run("EmptyData", func(t *testing.T) {
		empty := []byte{}
		_, _, err := util.DecodeMessage(empty)
		if err == nil {
			t.Error("Expected error for empty data, but got nil")
		}
	})

	t.Run("ShortData", func(t *testing.T) {
		short := []byte{0x00}
		_, _, err := util.DecodeMessage(short)
		if err == nil {
			t.Error("Expected error for short data, but got nil")
		}
	})

	t.Run("InvalidTopicLength", func(t *testing.T) {
		data := []byte{0x00, 0x05, 'a'}
		_, _, err := util.DecodeMessage(data)
		if err == nil {
			t.Error("Expected error for invalid topic length, but got nil")
		}
		expectedErr := "invalid topic length"
		if err != nil && err.Error() != expectedErr {
			t.Errorf("Expected error message '%s', got '%v'", expectedErr, err)
		}
	})
}

func TestBatchMessagesRoundTrip(t *testing.T) {
	testTopic := "batch-test-topic"
	testPartition := 1
	acksValues := []string{"0", "1", "-1", "all"}

	for _, acks := range acksValues {
		t.Run("Acks_"+acks, func(t *testing.T) {
			msgs := []types.Message{
				{Offset: 100, SeqNum: 1, ProducerID: "p1", Key: "k1", Payload: "msg1", Epoch: 1},
				{Offset: 101, SeqNum: 2, ProducerID: "p1", Key: "k2", Payload: "msg2", Epoch: 1},
			}

			data, err := util.EncodeBatchMessages(testTopic, testPartition, acks, msgs)
			if err != nil {
				t.Fatalf("EncodeBatchMessages failed: %v", err)
			}

			batch, err := util.DecodeBatchMessages(data)
			if err != nil {
				t.Fatalf("DecodeBatchMessages failed: %v", err)
			}

			if batch.Topic != testTopic {
				t.Errorf("Topic mismatch: got %s, want %s", batch.Topic, testTopic)
			}
			if batch.Partition != testPartition {
				t.Errorf("Partition mismatch: got %d, want %d", batch.Partition, testPartition)
			}
			if batch.Acks != acks {
				t.Errorf("Acks mismatch: got %s, want %s", batch.Acks, acks)
			}
			if len(batch.Messages) != len(msgs) {
				t.Errorf("Message count mismatch: got %d, want %d", len(batch.Messages), len(msgs))
			}

			if batch.Messages[0].Payload != msgs[0].Payload {
				t.Errorf("First message payload mismatch: got %s, want %s", batch.Messages[0].Payload, msgs[0].Payload)
			}
		})
	}
}

func TestBatchMessagesEdgeCases(t *testing.T) {
	t.Run("EmptyBatch", func(t *testing.T) {
		data, err := util.EncodeBatchMessages("topic", 0, "1", []types.Message{})
		if err != nil {
			t.Fatalf("Encoding empty batch failed: %v", err)
		}
		batch, err := util.DecodeBatchMessages(data)
		if err != nil {
			t.Fatalf("Decoding empty batch failed: %v", err)
		}
		if len(batch.Messages) != 0 {
			t.Errorf("Expected 0 messages, got %d", len(batch.Messages))
		}
	})

	t.Run("LargePayload", func(t *testing.T) {
		largePayload := string(make([]byte, 1024*10)) // 10KB
		msgs := []types.Message{{Payload: largePayload}}
		data, err := util.EncodeBatchMessages("topic", 0, "1", msgs)
		if err != nil {
			t.Fatalf("Encoding large payload failed: %v", err)
		}
		batch, err := util.DecodeBatchMessages(data)
		if err != nil || len(batch.Messages) != 1 || batch.Messages[0].Payload != largePayload {
			t.Error("Large payload integrity check failed")
		}
	})
}

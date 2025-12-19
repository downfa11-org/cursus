package util_test

import (
	"testing"

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

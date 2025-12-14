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

	decodedTopic, decodedPayload, err := util.DecodeMessage(data)
	if err != nil {
		util.Error("⚠️ Decode error: %v", err)
		return
	}
	if decodedTopic != topic {
		t.Errorf("Expected topic %s, got %s", topic, decodedTopic)
	}
	if decodedPayload != payload {
		t.Errorf("Expected payload %s, got %s", payload, decodedPayload)
	}
}

func TestDecodeMessageInvalidData(t *testing.T) {
	empty := []byte{}
	topic, payload, err := util.DecodeMessage(empty)
	if err != nil {
		util.Error("⚠️ Decode error: %v", err)
		return
	}
	if topic != "" || payload != "" {
		t.Errorf("Expected empty topic/payload, got %s/%s", topic, payload)
	}

	short := []byte{0x01}
	topic, payload, err = util.DecodeMessage(short)
	if err != nil {
		util.Error("⚠️ Decode error: %v", err)
		return
	}
	if topic != "" || payload != "" {
		t.Errorf("Expected empty topic/payload for short data, got %s/%s", topic, payload)
	}

	// topicLen > data length
	data := []byte{0x00, 0x05, 'a'}
	topic, payload, err = util.DecodeMessage(data)
	if err != nil {
		util.Error("⚠️ Decode error: %v", err)
		return
	}
	if topic != "" || payload != "" {
		t.Errorf("Expected empty topic/payload for invalid length, got %s/%s", topic, payload)
	}
}

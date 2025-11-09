package util_test

import (
	"testing"

	"github.com/downfa11-org/go-broker/util"
)

func TestGenerateID(t *testing.T) {
	payload1 := "hello world"
	payload2 := "hello Go"

	id1 := util.GenerateID(payload1)
	id2 := util.GenerateID(payload1)
	id3 := util.GenerateID(payload2)

	if id1 != id2 {
		t.Errorf("Expected same ID for same payload, got %d and %d", id1, id2)
	}

	if id1 == id3 {
		t.Errorf("Expected different IDs for different payloads, got %d", id1)
	}
}

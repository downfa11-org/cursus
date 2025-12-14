package disk_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/downfa11-org/go-broker/pkg/config"
	"github.com/downfa11-org/go-broker/pkg/disk"
)

// TestDiskHandlerBasic verifies basic append and flush behavior
func TestDiskHandlerBasic(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{
		DiskFlushBatchSize: 3,
		LingerMS:           50,
		ChannelBufferSize:  5,
		DiskWriteTimeoutMS: 100,
		LogDir:             dir,
	}

	topic := "testlog"
	segmentSize := 1024

	dh, err := disk.NewDiskHandler(cfg, topic, 0, segmentSize)
	if err != nil {
		t.Fatalf("NewDiskHandler: %v", err)
	}
	defer dh.Close()

	messages := []string{"msg1", "msg2", "msg3", "msg4", "msg5"}
	for i, msg := range messages {
		dh.AppendMessage(topic, 0, uint64(i), msg)
	}

	time.Sleep(150 * time.Millisecond)

	pattern := filepath.Join(cfg.LogDir, topic, "partition_0_segment_*.log")
	files, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatalf("glob %s: %v", pattern, err)
	}
	if len(files) == 0 {
		t.Fatalf("Expected at least 1 segment file, got %d", len(files))
	}

	readMsgs, err := dh.ReadMessages(0, len(messages))
	if err != nil {
		t.Fatalf("failed to read messages: %v", err)
	}

	if len(readMsgs) != len(messages) {
		t.Fatalf("expected %d messages, got %d", len(messages), len(readMsgs))
	}

	for i, msg := range readMsgs {
		expectedMsg := messages[i]
		if msg.Payload != expectedMsg {
			t.Errorf("message %d: expected payload %q, got %q", i, expectedMsg, msg.Payload)
		}
		if msg.Offset != uint64(i) {
			t.Errorf("message %d: expected offset %d, got %d", i, i, msg.Offset)
		}
	}
}

// TestDiskHandlerChannelOverflow ensures synchronous fallback works
func TestDiskHandlerChannelOverflow(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{
		DiskFlushBatchSize: 2,
		LingerMS:           50,
		ChannelBufferSize:  2,
		DiskWriteTimeoutMS: 100,
		LogDir:             dir,
	}

	topic := "overflowlog"
	segmentSize := 1024

	dh, err := disk.NewDiskHandler(cfg, topic, 0, segmentSize)
	if err != nil {
		t.Fatalf("NewDiskHandler: %v", err)
	}
	defer dh.Close()

	dh.AppendMessage(topic, 0, 0, "first")
	dh.AppendMessage(topic, 0, 1, "second")

	time.Sleep(50 * time.Millisecond)

	pattern := filepath.Join(cfg.LogDir, topic, "partition_0_segment_*.log")
	files, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatalf("glob %s: %v", pattern, err)
	}
	if len(files) == 0 {
		t.Fatalf("Expected segment file to be created")
	}

	readMsgs, err := dh.ReadMessages(0, 2)
	if err != nil {
		t.Fatalf("failed to read messages: %v", err)
	}

	if len(readMsgs) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(readMsgs))
	}

	expectedPayloads := []string{"first", "second"}
	for i, msg := range readMsgs {
		if msg.Payload != expectedPayloads[i] {
			t.Errorf("message %d: expected payload %q, got %q", i, expectedPayloads[i], msg.Payload)
		}
		if msg.Offset != uint64(i) {
			t.Errorf("message %d: expected offset %d, got %d", i, i, msg.Offset)
		}
	}
}

// TestDiskHandlerRotation verifies segment rotation
func TestDiskHandlerRotation(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{
		DiskFlushBatchSize: 1,
		LingerMS:           10,
		ChannelBufferSize:  10,
		DiskWriteTimeoutMS: 100,
		LogDir:             dir,
	}

	topic := "rotationlog"
	segmentSize := 10

	dh, err := disk.NewDiskHandler(cfg, topic, 0, segmentSize)
	if err != nil {
		t.Fatalf("NewDiskHandler: %v", err)
	}
	defer dh.Close()

	msgs := []string{"12345", "67890", "abcde"}
	for i, m := range msgs {
		dh.AppendMessage(topic, 0, uint64(i), m)
	}

	time.Sleep(50 * time.Millisecond)

	pattern := filepath.Join(cfg.LogDir, topic, "partition_0_segment_*.log")
	files, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatalf("glob %s: %v", pattern, err)
	}
	if len(files) < 2 {
		t.Errorf("Expected multiple segment files, got %d", len(files))
	}

	readMsgs, err := dh.ReadMessages(0, len(msgs))
	if err != nil {
		t.Fatalf("failed to read messages: %v", err)
	}

	if len(readMsgs) != len(msgs) {
		t.Fatalf("expected %d messages, got %d", len(msgs), len(readMsgs))
	}

	for i, msg := range readMsgs {
		if msg.Payload != msgs[i] {
			t.Errorf("message %d: expected payload %q, got %q", i, msgs[i], msg.Payload)
		}
		if msg.Offset != uint64(i) {
			t.Errorf("message %d: expected offset %d, got %d", i, i, msg.Offset)
		}
	}
}

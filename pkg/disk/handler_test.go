package disk_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/downfa11-org/cursus/pkg/config"
	"github.com/downfa11-org/cursus/pkg/disk"
	"github.com/downfa11-org/cursus/pkg/types"
)

// TestDiskHandlerBasic verifies basic append and flush behavior
func TestDiskHandlerBasic(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{
		DiskFlushBatchSize:  5,
		DiskFlushIntervalMS: 50,
		LingerMS:            50,
		ChannelBufferSize:   5,
		DiskWriteTimeoutMS:  100,
		IndexIntervalBytes:  4096,
		LogDir:              dir,
	}

	topic := "testlog"
	dh, err := disk.NewDiskHandler(cfg, topic, 0)
	if err != nil {
		t.Fatalf("NewDiskHandler: %v", err)
	}
	defer func() { _ = dh.Close() }()

	messages := []string{"msg1", "msg2", "msg3", "msg4", "msg5"}
	for i, payload := range messages {
		offset, err := dh.AppendMessage(topic, 0, &types.Message{
			Payload: payload,
			SeqNum:  uint64(i + 1),
		})
		if err != nil {
			t.Fatalf("failed to append message %d: %v", i, err)
		}
		if offset != uint64(i) {
			t.Errorf("AppendMessage %d: expected returned offset %d, got %d", i, i, offset)
		}
	}

	time.Sleep(150 * time.Millisecond)

	pattern := filepath.Join(cfg.LogDir, topic, "partition_0_segment_*.log")
	files, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatalf("glob %s: %v", pattern, err)
	}
	if len(files) == 0 {
		t.Fatalf("expected at least 1 segment file, got %d", len(files))
	}

	readMsgs, err := dh.ReadMessages(0, len(messages))
	if err != nil {
		t.Fatalf("failed to read messages: %v", err)
	}

	if len(readMsgs) != len(messages) {
		t.Fatalf("expected %d messages, got %d", len(messages), len(readMsgs))
	}

	for i, msg := range readMsgs {
		expectedPayload := messages[i]
		if msg.Payload != expectedPayload {
			t.Errorf("message %d: expected payload %q, got %q", i, expectedPayload, msg.Payload)
		}
		if msg.Offset != uint64(i) {
			t.Errorf("message %d: expected offset %d, got %d", i, i, msg.Offset)
		}
		if msg.SeqNum != uint64(i+1) {
			t.Errorf("message %d: expected SeqNum %d, got %d", i, i+1, msg.SeqNum)
		}
	}
}

// TestDiskHandlerChannelOverflow ensures synchronous fallback works
func TestDiskHandlerChannelOverflow(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{
		DiskFlushBatchSize:  2,
		DiskFlushIntervalMS: 50,
		LingerMS:            50,
		ChannelBufferSize:   2,
		DiskWriteTimeoutMS:  100,
		IndexIntervalBytes:  4096,
		LogDir:              dir,
	}

	topic := "overflowlog"
	dh, err := disk.NewDiskHandler(cfg, topic, 0)
	if err != nil {
		t.Fatalf("NewDiskHandler: %v", err)
	}
	defer func() { _ = dh.Close() }()

	off1, err := dh.AppendMessage(topic, 0, &types.Message{
		Payload: "first",
		SeqNum:  10,
	})
	if err != nil {
		t.Fatalf("failed to append first message: %v", err)
	}
	if off1 != 0 {
		t.Errorf("expected offset 0, got %d", off1)
	}

	off2, err := dh.AppendMessage(topic, 0, &types.Message{
		Payload: "second",
		SeqNum:  20,
	})
	if err != nil {
		t.Fatalf("failed to append second message: %v", err)
	}
	if off2 != 1 {
		t.Errorf("expected offset 1, got %d", off2)
	}

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

	expected := []struct {
		payload string
		seqNum  uint64
	}{
		{"first", 10},
		{"second", 20},
	}

	for i, msg := range readMsgs {
		if msg.Payload != expected[i].payload {
			t.Errorf("message %d: expected payload %q, got %q", i, expected[i].payload, msg.Payload)
		}
		if msg.Offset != uint64(i) {
			t.Errorf("message %d: expected offset %d, got %d", i, i, msg.Offset)
		}
		if msg.SeqNum != expected[i].seqNum {
			t.Errorf("message %d: expected SeqNum %d, got %d", i, expected[i].seqNum, msg.SeqNum)
		}
	}
}

// TestDiskHandlerRotation verifies segment rotation
func TestDiskHandlerRotation(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{
		DiskFlushBatchSize:  1,
		DiskFlushIntervalMS: 10,
		LingerMS:            10,
		ChannelBufferSize:   10,
		DiskWriteTimeoutMS:  100,
		IndexIntervalBytes:  4096,
		LogDir:              dir,
		SegmentSize:         20,
	}

	topic := "rotationlog"
	dh, err := disk.NewDiskHandler(cfg, topic, 0)
	if err != nil {
		t.Fatalf("NewDiskHandler: %v", err)
	}
	defer func() { _ = dh.Close() }()

	msgs := []string{"12345", "67890", "abcde"}
	for i, m := range msgs {
		offset, err := dh.AppendMessage(topic, 0, &types.Message{
			Payload: m,
			SeqNum:  uint64(100 + i),
		})
		if err != nil {
			t.Fatalf("failed to append message %d during rotation test: %v", i, err)
		}
		if offset != uint64(i) {
			t.Errorf("AppendMessage %d (rotation): expected offset %d, got %d", i, i, offset)
		}
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
		if msg.SeqNum != uint64(100+i) {
			t.Errorf("message %d: expected SeqNum %d, got %d", i, 100+i, msg.SeqNum)
		}
	}
}

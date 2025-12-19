package disk_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/downfa11-org/go-broker/pkg/config"
	"github.com/downfa11-org/go-broker/pkg/disk"
)

func setupDiskHandler(t *testing.T) *disk.DiskHandler {
	tmpDir := t.TempDir()
	cfg := &config.Config{
		LogDir:             tmpDir,
		DiskFlushBatchSize: 2,
		LingerMS:           100,
		DiskWriteTimeoutMS: 500,
		SegmentRollTimeMS:  500,
		SegmentSize:        1024,
	}

	dh, err := disk.NewDiskHandler(cfg, "testTopic", 0, cfg.SegmentSize)
	if err != nil {
		t.Fatalf("failed to create DiskHandler: %v", err)
	}

	return dh
}

func TestWriteDirectAndFlush(t *testing.T) {
	dh := setupDiskHandler(t)
	defer dh.Close()

	for i := 0; i < 3; i++ {
		if err := dh.WriteDirect("testTopic", 0, uint64(i), "msg"+string(rune('A'+i))); err != nil {
			t.Fatalf("WriteDirect failed: %v", err)
		}
	}

	dh.Flush()

	if dh.GetAbsoluteOffset() != 3 {
		t.Fatalf("expected AbsoluteOffset 3, got %d", dh.GetAbsoluteOffset())
	}

	filePath := filepath.Join(dh.BaseName + "_segment_0.log")
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Fatalf("expected segment file to exist at %s", filePath)
	}
}

func TestWriteBatchRotation(t *testing.T) {
	dh := setupDiskHandler(t)
	defer dh.Close()

	msg := make([]byte, dh.SegmentSize/2)
	for i := range msg {
		msg[i] = 'x'
	}

	if err := dh.WriteDirect("testTopic", 0, 0, string(msg)); err != nil {
		t.Fatalf("WriteDirect failed: %v", err)
	}
	if err := dh.WriteDirect("testTopic", 0, 1, string(msg)); err != nil {
		t.Fatalf("WriteDirect failed: %v", err)
	}

	currentSeg := dh.GetCurrentSegment()
	if currentSeg != 1 {
		t.Fatalf("expected CurrentSegment 1 after rotation, got %d", currentSeg)
	}
}

func TestFlushLoopAsync(t *testing.T) {
	dh := setupDiskHandler(t)
	defer dh.Close()

	dh.AppendMessage("testTopic", 0, 0, "async1")
	dh.AppendMessage("testTopic", 0, 1, "async2")
	dh.AppendMessage("testTopic", 0, 2, "async3")

	t.Logf("waiting for flushLoop to process messages...")
	<-time.After(200 * time.Millisecond)

	if dh.GetAbsoluteOffset() != 3 {
		t.Fatalf("expected AbsoluteOffset 3 after async flush, got %d", dh.GetAbsoluteOffset())
	}
}

func TestReadMessagesWithMetadata(t *testing.T) {
	dh := setupDiskHandler(t)
	defer dh.Close()

	testMessages := []struct {
		topic     string
		partition int
		offset    uint64
		payload   string
	}{
		{"testTopic", 0, 0, "message1"},
		{"testTopic", 0, 1, "message2"},
		{"testTopic", 0, 2, "message3"},
	}

	for _, msg := range testMessages {
		if err := dh.WriteDirect(msg.topic, msg.partition, msg.offset, msg.payload); err != nil {
			t.Fatalf("WriteDirect failed: %v", err)
		}
	}

	dh.Flush()

	messages, err := dh.ReadMessages(0, 3)
	if err != nil {
		t.Fatalf("failed to read messages: %v", err)
	}

	if len(messages) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(messages))
	}

	for i, msg := range messages {
		expected := testMessages[i]
		if msg.Offset != expected.offset {
			t.Errorf("message %d: expected offset %d, got %d", i, expected.offset, msg.Offset)
		}
		if msg.Payload != expected.payload {
			t.Errorf("message %d: expected payload %q, got %q", i, expected.payload, msg.Payload)
		}
	}
}

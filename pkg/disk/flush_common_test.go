package disk_test

import (
	"os"
	"path/filepath"
	"strconv"
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
		dh.WriteDirect("msg" + strconv.Itoa('A'+i))
	}

	dh.Flush()

	if dh.AbsoluteOffset != 3 {
		t.Fatalf("expected AbsoluteOffset 3, got %d", dh.AbsoluteOffset)
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

	dh.WriteDirect(string(msg))
	dh.WriteDirect(string(msg))

	currentSeg := dh.GetCurrentSegment()
	if currentSeg != 1 {
		t.Fatalf("expected CurrentSegment 1 after rotation, got %d", currentSeg)
	}
}

func TestFlushLoopAsync(t *testing.T) {
	dh := setupDiskHandler(t)
	defer dh.Close()

	dh.AppendMessage("async1")
	dh.AppendMessage("async2")
	dh.AppendMessage("async3")

	t.Logf("waiting for flushLoop to process messages...")
	<-time.After(200 * time.Millisecond)

	if dh.GetAbsoluteOffset() != 3 {
		t.Fatalf("expected AbsoluteOffset 3 after async flush, got %d", dh.GetAbsoluteOffset())
	}
}

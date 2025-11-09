package disk_test

import (
	"os"
	"testing"

	"github.com/downfa11-org/go-broker/pkg/disk"
)

func TestAppendMessage(t *testing.T) {
	dir := t.TempDir()
	baseName := dir + "/testlog"
	dh := disk.NewDiskHandler(baseName, 1024)
	defer dh.Close()

	res1, err := dh.AppendMessage("hello")
	if err != nil {
		t.Fatalf("AppendMessage failed: %v", err)
	}
	if res1.Offset != 0 {
		t.Errorf("Expected offset 0, got %d", res1.Offset)
	}

	res2, err := dh.AppendMessage("world")
	if err != nil {
		t.Fatalf("AppendMessage failed: %v", err)
	}
	if res2.Offset <= res1.Offset {
		t.Errorf("Offset not increasing: %d <= %d", res2.Offset, res1.Offset)
	}

	if _, err := os.Stat(baseName + "_00000.log"); err != nil {
		t.Errorf("Segment file not created: %v", err)
	}
}

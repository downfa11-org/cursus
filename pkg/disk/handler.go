package disk

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/downfa11-org/go-broker/pkg/types"
)

type DiskManager struct {
	BaseDir     string
	SegmentSize int
	handlers    map[string]*DiskHandler // key: topic_partition
	mu          sync.Mutex
}

func NewDiskManager(baseDir string, segmentSize int) *DiskManager {
	return &DiskManager{
		BaseDir:     baseDir,
		SegmentSize: segmentSize,
		handlers:    make(map[string]*DiskHandler),
	}
}

// getKey generates a unique key for the topic and partition combination.
func (dm *DiskManager) getKey(topic string, partition int) string {
	return fmt.Sprintf("%s_%d", topic, partition)
}

// GetHandler retrieves or creates a DiskHandler for the specified topic and partition.
func (dm *DiskManager) GetHandler(topic string, partition int) (*DiskHandler, error) {
	key := dm.getKey(topic, partition)

	dm.mu.Lock()
	defer dm.mu.Unlock()

	if handler, ok := dm.handlers[key]; ok {
		return handler, nil
	}

	topicDir := filepath.Join(dm.BaseDir, topic)
	if err := os.MkdirAll(topicDir, 0755); err != nil {
		return nil, err
	}

	handler := NewDiskHandler(filepath.Join(topicDir, fmt.Sprintf("partition_%d", partition)), dm.SegmentSize)
	dm.handlers[key] = handler
	return handler, nil
}

// CloseAll closes all DiskHandlers managed by DiskManager.
func (dm *DiskManager) CloseAll() error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	for _, h := range dm.handlers {
		h.Close()
	}
	return nil
}

// DiskHandler manages disk-based message storage with segmented files.
// It supports thread-safe appends and keeps track of the current segment and offset.
type DiskHandler struct {
	BaseName       string // Base name for segment files
	SegmentSize    int    // Maximum size of a single segment file
	CurrentOffset  int    // Current write offset in the active segment
	CurrentSegment int    // Current segment index

	mu     sync.Mutex    // Mutex to protect concurrent access
	file   *os.File      // Currently opened segment file
	writer *bufio.Writer // Buffered writer for the current file
}

// NewDiskHandler creates a new DiskHandler instance.
func NewDiskHandler(baseName string, segmentSize int) *DiskHandler {
	return &DiskHandler{
		BaseName:    baseName,
		SegmentSize: segmentSize,
	}
}

// AppendMessage appends a serialized message to the current segment file.
// It creates the file if not already open and returns an AppendResult containing the segment index and offset of the message.
func (d *DiskHandler) AppendMessage(serialized string) (*types.AppendResult, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.file == nil {
		if err := d.openSegment(); err != nil {
			return nil, err
		}
	}

	data := []byte(serialized + "\n")
	if d.CurrentOffset+len(data) > d.SegmentSize {
		if err := d.rotateSegment(); err != nil {
			return nil, err
		}
		data = []byte(serialized + "\n")
	}

	n, err := d.writer.Write(data)
	if err != nil {
		return nil, err
	}
	d.writer.Flush()

	res := &types.AppendResult{SegmentIndex: d.CurrentSegment, Offset: d.CurrentOffset}
	d.CurrentOffset += n
	return res, nil
}

func (d *DiskHandler) openSegment() error {
	f, err := os.OpenFile(fmt.Sprintf("%s_%05d.log", d.BaseName, d.CurrentSegment),
		os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	d.file = f
	d.writer = bufio.NewWriter(f)
	return nil
}

func (d *DiskHandler) rotateSegment() error {
	if d.writer != nil {
		d.writer.Flush()
	}
	if d.file != nil {
		d.file.Close()
	}
	d.CurrentSegment++
	d.CurrentOffset = 0
	return d.openSegment()
}

// Log prints a message to stdout with a given log level.
func (d *DiskHandler) Log(level string, msg string) {
	fmt.Printf("[%s] %s\n", level, msg)
}

func (d *DiskHandler) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.file != nil {
		err := d.file.Close()
		d.file = nil
		return err
	}
	return nil
}

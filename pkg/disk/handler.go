package disk

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/downfa11-org/go-broker/pkg/config"
	"github.com/downfa11-org/go-broker/pkg/types"
	"golang.org/x/exp/mmap"
)

type DiskHandler struct {
	BaseName       string
	SegmentSize    int
	CurrentOffset  int
	CurrentSegment int

	writeCh      chan string
	done         chan struct{}
	batchSize    int
	linger       time.Duration
	writeTimeout time.Duration

	mu   sync.Mutex // metadata(offset, segment), file handler(d.file)
	ioMu sync.Mutex // bufio.Writer, flush

	file   *os.File
	writer *bufio.Writer

	closeOnce sync.Once
	shutdown  sync.WaitGroup
}

func NewDiskHandler(cfg *config.Config, topicName string, partitionID, segmentSize int) (*DiskHandler, error) {
	base := fmt.Sprintf("%s%c%s%cpartition_%d", cfg.LogDir, os.PathSeparator, topicName, os.PathSeparator, partitionID)
	if err := os.MkdirAll(filepath.Dir(base), 0o755); err != nil {
		return nil, err
	}

	filePath := base + "_segment_0.log"
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	dh := &DiskHandler{
		BaseName:     base,
		SegmentSize:  segmentSize,
		writeCh:      make(chan string, cfg.ChannelBufferSize),
		done:         make(chan struct{}),
		batchSize:    cfg.DiskFlushBatchSize,
		linger:       time.Duration(cfg.LingerMS) * time.Millisecond,
		writeTimeout: time.Duration(cfg.DiskWriteTimeoutMS) * time.Millisecond,
		file:         file,
		writer:       bufio.NewWriter(file),
	}

	dh.shutdown.Add(1)
	go func() {
		defer dh.shutdown.Done()
		dh.flushLoop()
	}()

	return dh, nil
}

// AppendMessage sends a message to the internal write channel for asynchronous disk persistence.
func (d *DiskHandler) AppendMessage(msg string) {
	for {
		select {
		case <-d.done:
			return
		case d.writeCh <- msg:
			return
		default:
		}

		if d.writeTimeout > 0 {
			timer := time.NewTimer(d.writeTimeout)
			select {
			case <-d.done:
				timer.Stop()
				return
			case d.writeCh <- msg:
				timer.Stop()
				return
			case <-timer.C:
				timer.Stop()
				log.Printf("⚠️ DiskHandler enqueue timed out after %s; retrying", d.writeTimeout)
			}
			continue
		}

		select {
		case <-d.done:
			return
		case d.writeCh <- msg:
			return
		}
	}
}

// ReadMessages reads a batch of messages from the disk log, starting from the given offset.
func (dh *DiskHandler) ReadMessages(offset, max int) ([]types.Message, error) {
	dh.mu.Lock()
	segment := dh.CurrentSegment
	dh.mu.Unlock()

	filePath := fmt.Sprintf("%s_segment_%d.log", dh.BaseName, segment)

	reader, err := mmap.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("mmap open failed: %w", err)
	}
	defer reader.Close()

	messages := []types.Message{}
	pos := 0
	for i := 0; i < max; i++ {
		if pos+4 > reader.Len() {
			break
		}

		lenBytes := make([]byte, 4)
		_, err := reader.ReadAt(lenBytes, int64(pos))
		if err != nil {
			log.Println("read length error:", err)
			break
		}
		msgLen := binary.BigEndian.Uint32(lenBytes)
		pos += 4

		if pos+int(msgLen) > reader.Len() {
			break
		}

		data := make([]byte, msgLen)
		_, err = reader.ReadAt(data, int64(pos))
		if err != nil {
			log.Println("read data error:", err)
			break
		}
		pos += int(msgLen)

		msg := types.Message{
			Payload: string(data),
		}

		if offset > 0 {
			offset--
			continue
		}

		messages = append(messages, msg)
	}
	return messages, nil
}

// Close signals the flushLoop to terminate and cleans up resources.
func (d *DiskHandler) Close() {
	d.closeOnce.Do(func() {
		close(d.writeCh)
		close(d.done)
		d.shutdown.Wait()
	})
}

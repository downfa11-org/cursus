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
	AbsoluteOffset int64

	writeCh      chan string
	done         chan struct{}
	batchSize    int
	linger       time.Duration
	writeTimeout time.Duration

	segmentRollTime  time.Duration
	segmentCreatedAt time.Time

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

	pattern := base + "_segment_*.log"
	files, _ := filepath.Glob(pattern)

	var currentSegment int
	var absoluteOffset int64

	if len(files) > 0 {
		for _, f := range files {
			var segNum int
			_, err := fmt.Sscanf(filepath.Base(f), "partition_%d_segment_%d.log", &partitionID, &segNum)
			if err != nil {
				return nil, fmt.Errorf("failed to parse filename %s: %w", f, err)
			}
			if segNum > currentSegment {
				currentSegment = segNum
			}
		}

		for _, f := range files {
			count, err := countMessagesInFile(f)
			if err != nil {
				log.Printf("⚠️ Failed to count messages in %s: %v", f, err)
				continue
			}
			absoluteOffset += int64(count)
		}
	}

	filePath := fmt.Sprintf("%s_segment_%d.log", base, currentSegment)
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}
	currentOffset := int(fileInfo.Size())

	dh := &DiskHandler{
		BaseName:         base,
		SegmentSize:      segmentSize,
		CurrentSegment:   currentSegment,
		CurrentOffset:    currentOffset,
		AbsoluteOffset:   absoluteOffset,
		writeCh:          make(chan string, cfg.ChannelBufferSize),
		done:             make(chan struct{}),
		batchSize:        cfg.DiskFlushBatchSize,
		linger:           time.Duration(cfg.LingerMS) * time.Millisecond,
		writeTimeout:     time.Duration(cfg.DiskWriteTimeoutMS) * time.Millisecond,
		segmentRollTime:  time.Duration(cfg.SegmentRollTimeMS) * time.Millisecond,
		segmentCreatedAt: time.Now(),
		file:             file,
		writer:           bufio.NewWriter(file),
	}

	dh.shutdown.Add(1)
	go func() {
		defer dh.shutdown.Done()
		dh.flushLoop()
	}()

	return dh, nil
}

func countMessagesInFile(filePath string) (int, error) {
	reader, err := mmap.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer reader.Close()

	count := 0
	pos := 0
	for {
		if pos+4 > reader.Len() {
			break
		}

		lenBytes := make([]byte, 4)
		_, err := reader.ReadAt(lenBytes, int64(pos))
		if err != nil {
			return count, fmt.Errorf("failed to read length at pos %d: %w", pos, err)
		}

		msgLen := binary.BigEndian.Uint32(lenBytes)

		if pos+4+int(msgLen) > reader.Len() {
			log.Printf("⚠️ Incomplete message at pos %d in %s (expected %d bytes, file ends at %d)",
				pos, filePath, msgLen, reader.Len())
			break
		}

		pos += 4 + int(msgLen)
		count++
	}
	return count, nil
}

func (d *DiskHandler) AppendMessageSync(payload string) error {
	d.WriteDirect(payload)
	return nil
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
	currentMsgIndex := 0

	for len(messages) < max {
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

		if currentMsgIndex >= offset {
			messages = append(messages, types.Message{
				Payload: string(data),
			})
		}
		currentMsgIndex++
	}
	return messages, nil
}

func (dh *DiskHandler) GetLatestOffset() (int, error) {
	dh.mu.Lock()
	defer dh.mu.Unlock()

	return int(dh.AbsoluteOffset), nil
}

// Close signals the flushLoop to terminate and cleans up resources.
func (d *DiskHandler) Close() {
	d.closeOnce.Do(func() {
		close(d.writeCh)
		close(d.done)
		d.shutdown.Wait()
	})
}

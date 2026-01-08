package disk

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/downfa11-org/cursus/pkg/config"
	"github.com/downfa11-org/cursus/pkg/types"
	"github.com/downfa11-org/cursus/util"
	"golang.org/x/exp/mmap"
)

type DiskHandler struct {
	BaseName       string
	SegmentSize    uint64
	CurrentOffset  uint64
	CurrentSegment int
	AbsoluteOffset uint64

	writeCh      chan types.DiskMessage
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
	var absoluteOffset uint64

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
				util.Error("⚠️ Failed to count messages in %s: %v", f, err)
				continue
			}
			absoluteOffset += uint64(count)
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
	currentOffset := uint64(fileInfo.Size())

	dh := &DiskHandler{
		BaseName:         base,
		SegmentSize:      uint64(segmentSize),
		CurrentSegment:   currentSegment,
		CurrentOffset:    currentOffset,
		AbsoluteOffset:   absoluteOffset,
		writeCh:          make(chan types.DiskMessage, cfg.ChannelBufferSize),
		done:             make(chan struct{}),
		batchSize:        cfg.DiskFlushBatchSize,
		linger:           time.Duration(cfg.LingerMS) * time.Millisecond,
		writeTimeout:     time.Duration(cfg.DiskWriteTimeoutMS) * time.Millisecond,
		segmentRollTime:  time.Duration(cfg.SegmentRollTimeMS) * time.Millisecond,
		segmentCreatedAt: time.Now(),
		file:             file,
		writer:           bufio.NewWriter(file),
	}

	dh.shutdown.Add(2)
	go func() {
		defer dh.shutdown.Done()
		dh.flushLoop()
	}()

	go func() {
		defer dh.shutdown.Done()
		dh.syncLoop()
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
			util.Error("⚠️ Incomplete message at pos %d in %s (expected %d bytes, file ends at %d)",
				pos, filePath, msgLen, reader.Len())
			break
		}

		pos += 4 + int(msgLen)
		count++
	}
	return count, nil
}

func (d *DiskHandler) AppendMessageSync(topic string, partition int, msg *types.Message) (uint64, error) {
	d.mu.Lock()
	offset := d.AbsoluteOffset
	d.AbsoluteOffset++
	d.mu.Unlock()

	msg.Offset = offset
	if err := d.WriteDirect(topic, partition, *msg); err != nil {
		return 0, fmt.Errorf("WriteDirect failed: %w", err)
	}
	return offset, nil
}

// AppendMessage sends a message to the internal write channel for asynchronous disk persistence.
func (d *DiskHandler) AppendMessage(topic string, partition int, msg *types.Message) (uint64, error) {
	util.Debug("Attempting to append message (len=%d) to disk.writeCh (cap=%d, len=%d)", len(msg.Payload), cap(d.writeCh), len(d.writeCh))

	d.mu.Lock()
	offset := d.AbsoluteOffset
	d.AbsoluteOffset++
	d.mu.Unlock()

	msg.Offset = offset
	diskMsg := types.DiskMessage{
		Topic:      topic,
		Partition:  int32(partition),
		Offset:     offset,
		ProducerID: msg.ProducerID,
		SeqNum:     msg.SeqNum,
		Epoch:      msg.Epoch,
		Payload:    msg.Payload,
	}

	if d.writeTimeout > 0 {
		timer := time.NewTimer(d.writeTimeout)
		defer timer.Stop()

		select {
		case <-d.done:
			util.Debug("done channel closed for %s", d.BaseName)
			return 0, fmt.Errorf("disk handler is shutting down")
		case d.writeCh <- diskMsg:
			return diskMsg.Offset, nil
		case <-timer.C:
			util.Error("enqueue timed out after %s for topic %s", d.writeTimeout, topic)
			return 0, fmt.Errorf("enqueue timeout after %s", d.writeTimeout)
		}
	} else {
		select {
		case <-d.done:
			return 0, fmt.Errorf("disk handler is shutting down")
		case d.writeCh <- diskMsg:
			return diskMsg.Offset, nil
		}
	}
}

// ReadMessages reads a batch of messages from the disk log, starting from the given offset.
func (dh *DiskHandler) ReadMessages(offset uint64, max int) ([]types.Message, error) {
	var allMessages []types.Message

	dh.mu.Lock()
	currentSegment := dh.CurrentSegment
	dh.mu.Unlock()

	remainingToSkip := offset
	cumulativeOffset := uint64(0)
	for segmentID := 0; segmentID <= currentSegment; segmentID++ {
		filePath := fmt.Sprintf("%s_segment_%d.log", dh.BaseName, segmentID)
		reader, err := mmap.Open(filePath)
		if err != nil {
			continue
		}

		segmentMsgCount := dh.countMessagesFromReader(reader)
		if remainingToSkip >= uint64(segmentMsgCount) {
			remainingToSkip -= uint64(segmentMsgCount)
			cumulativeOffset += uint64(segmentMsgCount)
			reader.Close()
			continue
		}

		messages := dh.readMessagesFromSegment(reader, remainingToSkip, max-len(allMessages), cumulativeOffset)
		remainingToSkip = 0
		cumulativeOffset += uint64(segmentMsgCount)
		allMessages = append(allMessages, messages...)
		reader.Close()

		if len(allMessages) >= max {
			break
		}
	}

	return allMessages, nil
}

func (dh *DiskHandler) countMessagesFromReader(reader *mmap.ReaderAt) int {
	count := 0
	pos := 0
	for pos+4 <= reader.Len() {
		lenBytes := make([]byte, 4)
		if _, err := reader.ReadAt(lenBytes, int64(pos)); err != nil {
			break
		}
		msgLen := binary.BigEndian.Uint32(lenBytes)
		if pos+4+int(msgLen) > reader.Len() {
			break
		}
		pos += 4 + int(msgLen)
		count++
	}
	return count
}

func (dh *DiskHandler) readMessagesFromSegment(reader *mmap.ReaderAt, startOffset uint64, max int, segmentStartOffset uint64) []types.Message {
	messages := []types.Message{}
	pos := 0
	currentMsgIndex := uint64(0)

	for len(messages) < max {
		if pos+4 > reader.Len() {
			break
		}
		lenBytes := make([]byte, 4)
		_, err := reader.ReadAt(lenBytes, int64(pos))
		if err != nil {
			break
		}
		msgLen := binary.BigEndian.Uint32(lenBytes)
		if pos+4+int(msgLen) > reader.Len() {
			break
		}

		pos += 4
		data := make([]byte, msgLen)
		_, err = reader.ReadAt(data, int64(pos))
		if err != nil {
			break
		}
		pos += int(msgLen)

		if currentMsgIndex >= startOffset {
			diskMsg, err := util.DeserializeDiskMessage(data)
			if err != nil {
				util.Error("deserialize message failed: %v", err)
				continue
			}
			calculatedOffset := segmentStartOffset + currentMsgIndex
			msg := types.Message{
				Offset:     calculatedOffset,
				ProducerID: diskMsg.ProducerID,
				SeqNum:     diskMsg.SeqNum,
				Epoch:      diskMsg.Epoch,
				Payload:    diskMsg.Payload,
			}
			messages = append(messages, msg)
		}
		currentMsgIndex++
	}

	return messages
}

func (d *DiskHandler) countMessagesInSegment() (int, error) {
	return d.countMessagesInSegmentID(d.CurrentSegment)
}

func (d *DiskHandler) countMessagesInSegmentID(segmentID int) (int, error) {
	filePath := fmt.Sprintf("%s_segment_%d.log", d.BaseName, segmentID)
	reader, err := mmap.Open(filePath)
	if err != nil {
		return 0, fmt.Errorf("mmap open failed: %w", err)
	}
	defer reader.Close()

	count := 0
	pos := 0
	for pos < reader.Len() {
		if pos+4 > reader.Len() {
			break
		}
		lenBytes := make([]byte, 4)
		_, err := reader.ReadAt(lenBytes, int64(pos))
		if err != nil {
			break
		}
		msgLen := binary.BigEndian.Uint32(lenBytes)
		if pos+4+int(msgLen) > reader.Len() {
			break
		}
		pos += 4 + int(msgLen)
		count++
	}
	return count, nil
}

func (dh *DiskHandler) GetLatestOffset() (uint64, error) {
	dh.mu.Lock()
	defer dh.mu.Unlock()

	return dh.AbsoluteOffset, nil
}

// Close signals the flushLoop to terminate and cleans up resources.
func (d *DiskHandler) Close() {
	d.closeOnce.Do(func() {
		close(d.done)
		d.shutdown.Wait()
		if d.writer != nil {
			if err := d.writer.Flush(); err != nil {
				util.Error("flush error on close: %v", err)
			}
		}
		if d.file != nil {
			if err := d.file.Close(); err != nil {
				util.Error("file close error: %v", err)
			}
		}
	})
}

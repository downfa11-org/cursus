package disk

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/downfa11-org/go-broker/pkg/types"
	"github.com/downfa11-org/go-broker/util"
)

// flushLoop continuously processes write batches and handles segment rotation.
func (d *DiskHandler) flushLoop() {
	batch := make([]types.DiskMessage, 0, d.batchSize)
	ticker := time.NewTicker(d.linger)
	defer ticker.Stop()

	var segmentTicker *time.Ticker
	if d.segmentRollTime > 0 {
		segmentTicker = time.NewTicker(d.segmentRollTime)
		defer segmentTicker.Stop()
	}

	for {
		select {
		case msg, ok := <-d.writeCh:
			if !ok {
				d.drainAndShutdown(batch)
				return
			}

			batch = append(batch, msg)
			util.Debug("Received message, batch size now: %d/%d", len(batch), d.batchSize)

			if len(batch) >= d.batchSize {
				util.Debug("Batch size threshold reached, flushing %d messages", len(batch))
				if err := d.WriteBatch(batch); err != nil {
					util.Error("WriteBatch failed: %v", err)
				}
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				util.Debug("Flushing %d messages on timer", len(batch))
				if err := d.WriteBatch(batch); err != nil {
					util.Error("WriteBatch failed: %v", err)
				}
				batch = batch[:0]
			}
		case <-d.getSegmentTickerChan(segmentTicker):
			// Time-based segment rotation
			d.mu.Lock()
			d.ioMu.Lock()
			if time.Since(d.segmentCreatedAt) >= d.segmentRollTime {
				if err := d.rotateSegment(); err != nil {
					util.Error("time-based segment rotation failed: %v", err)
				}
			}
			d.ioMu.Unlock()
			d.mu.Unlock()

		case <-d.done:
			// Gracefully drain all pending writes before shutdown
			d.drainAndShutdown(batch)
			return
		}
	}
}

func (d *DiskHandler) syncLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			d.ioMu.Lock()
			if d.file != nil {
				if err := d.file.Sync(); err != nil {
					util.Error("failed to sync file: %v", err)
				}
			}
			d.ioMu.Unlock()
		case <-d.done:
			return
		}
	}
}

// WriteBatch writes a batch of messages into the current segment file.
func (d *DiskHandler) WriteBatch(batch []types.DiskMessage) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ioMu.Lock()
	defer d.ioMu.Unlock()

	if d.file == nil {
		if err := d.openSegment(); err != nil {
			return fmt.Errorf("open segment failed: %w", err)
		}
	}

	if len(batch) == 0 {
		return nil
	}

	serializedMsgs := make([][]byte, 0, len(batch))
	totalSize := 0

	for i, msg := range batch {
		serialized, err := util.SerializeDiskMessage(msg)
		if err != nil {
			return fmt.Errorf("serialize failed at index %d: %w", i, err)
		}
		if len(serialized) > 0xFFFFFFFF {
			return fmt.Errorf("message too large at index %d: %d bytes", i, len(serialized))
		}
		serializedMsgs = append(serializedMsgs, serialized)
		totalSize += 4 + len(serialized)
	}

	buffer := make([]byte, 0, totalSize)
	lenBuf := make([]byte, 4)

	for _, serialized := range serializedMsgs {
		msgLen := len(serialized)
		binary.BigEndian.PutUint32(lenBuf, uint32(msgLen))
		buffer = append(buffer, lenBuf...)
		buffer = append(buffer, serialized...)
	}

	if d.CurrentOffset+uint64(len(buffer)) > d.SegmentSize {
		if err := d.rotateSegment(); err != nil {
			return fmt.Errorf("rotateSegment failed: %w", err)
		}
	}

	if _, err := d.writer.Write(buffer); err != nil {
		return fmt.Errorf("write batch failed: %w", err)
	}

	if err := d.writer.Flush(); err != nil {
		return fmt.Errorf("flush failed after batch: %w", err)
	}

	d.CurrentOffset += uint64(len(buffer))
	return nil
}

// WriteDirect writes a single message immediately without batching.
func (d *DiskHandler) WriteDirect(topic string, partition int, msg types.Message) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ioMu.Lock()
	defer d.ioMu.Unlock()

	diskMsg := types.DiskMessage{
		Topic:     topic,
		Partition: int32(partition),
		Offset:    msg.Offset,
		SeqNum:    msg.SeqNum,
		Epoch:     msg.Epoch,
		Payload:   msg.Payload,
	}

	serialized, err := util.SerializeDiskMessage(diskMsg)
	if err != nil {
		return fmt.Errorf("serialize failed: %w", err)
	}

	if len(serialized) > 0xFFFFFFFF {
		return fmt.Errorf("message too large: %d bytes", len(serialized))
	}

	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(serialized)))
	totalLen := uint64(4 + len(serialized))

	if d.CurrentOffset+totalLen > d.SegmentSize {
		if err := d.rotateSegment(); err != nil {
			return fmt.Errorf("rotateSegment failed: %w", err)
		}
	}

	if _, err := d.writer.Write(lenBuf[:]); err != nil {
		return fmt.Errorf("write length failed: %w", err)
	}
	if _, err := d.writer.Write(serialized); err != nil {
		return fmt.Errorf("write payload failed: %w", err)
	}

	if err := d.writer.Flush(); err != nil {
		return fmt.Errorf("flush failed: %w", err)
	}

	d.CurrentOffset += totalLen
	return nil
}

// rotateSegment closes the current segment and opens a new one.
func (d *DiskHandler) rotateSegment() error {
	var errs []error

	if d.writer != nil {
		if err := d.writer.Flush(); err != nil {
			util.Error("flush failed during rotation: %v", err)
			errs = append(errs, err)
		}
	}

	if d.file != nil {
		if err := d.file.Close(); err != nil {
			util.Error("close failed during rotation: %v", err)
			errs = append(errs, err)
		}
	}

	d.CurrentSegment++
	d.CurrentOffset = 0
	d.segmentCreatedAt = time.Now()

	if err := d.openSegment(); err != nil {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("rotateSegment errors: %v", errs)
	}
	return nil
}

// Flush forces all pending data to be written and synced to disk.
func (d *DiskHandler) Flush() {
	batch := make([]types.DiskMessage, 0, len(d.writeCh))

	for {
		select {
		case msg := <-d.writeCh:
			batch = append(batch, msg)
		default:
			goto perform_write
		}
	}

perform_write:
	if len(batch) > 0 {
		if err := d.WriteBatch(batch); err != nil {
			util.Error("Flush write failed: %v", err)
			return
		}
		return
	}

	d.ioMu.Lock()
	defer d.ioMu.Unlock()

	if d.writer != nil {
		if err := d.writer.Flush(); err != nil {
			util.Error("flush failed in Flush: %v", err)
		}
	}

	if d.file != nil {
		if err := d.file.Sync(); err != nil {
			util.Error("failed to sync disk file: %v", err)
		}
	}
}

// GetAbsoluteOffset returns the current absolute offset in a thread-safe manner
func (d *DiskHandler) GetAbsoluteOffset() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.AbsoluteOffset
}

// GetCurrentSegment returns the current segment number in a thread-safe manner
func (d *DiskHandler) GetCurrentSegment() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.CurrentSegment
}

func (d *DiskHandler) drainAndShutdown(batch []types.DiskMessage) {
	for {
		stop := false
		select {
		case msg, ok := <-d.writeCh:
			if !ok {
				stop = true
			} else {
				batch = append(batch, msg)
			}
		default:
			stop = true
		}

		if len(batch) >= d.batchSize {
			if err := d.WriteBatch(batch); err != nil {
				util.Error("WriteBatch failed: %v", err)
			}
			batch = batch[:0]
		}

		if stop {
			break
		}
	}

	if len(batch) > 0 {
		if err := d.WriteBatch(batch); err != nil {
			util.Error("finalize WriteBatch failed: %v", err)
		}
	}

	d.ioMu.Lock()
	defer d.ioMu.Unlock()

	if d.writer != nil {
		if err := d.writer.Flush(); err != nil {
			util.Error("writer flush failed: %v", err)
		}
		d.writer = nil
	}

	if d.file != nil {
		if err := d.file.Sync(); err != nil {
			util.Error("file sync failed: %v", err)
		}
		if err := d.file.Close(); err != nil {
			util.Error("file close failed: %v", err)
		}
		d.file = nil
	}
}

func (d *DiskHandler) getSegmentTickerChan(ticker *time.Ticker) <-chan time.Time {
	if ticker != nil {
		return ticker.C
	}
	return nil
}

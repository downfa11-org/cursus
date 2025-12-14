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
				continue
			}
			batch = append(batch, msg)
			util.Debug("Received message, batch size now: %d/%d", len(batch), d.batchSize)

			if len(batch) >= d.batchSize {
				util.Debug("🔥 Batch size threshold reached, flushing %d messages", len(batch))
				d.writeBatch(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				util.Debug("🔥 Flushing %d messages on timer", len(batch))
				d.writeBatch(batch)
				batch = batch[:0]
			}
		case <-func() <-chan time.Time {
			if segmentTicker != nil {
				return segmentTicker.C
			}
			return nil
		}():
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
			draining := true
			for draining {
				if len(batch) >= d.batchSize {
					d.writeBatch(batch)
					batch = batch[:0]
					continue
				}
				select {
				case msg, ok := <-d.writeCh:
					if !ok {
						draining = false
						continue
					}
					batch = append(batch, msg)
				default:
					draining = false
				}
			}

			if len(batch) > 0 {
				d.writeBatch(batch)
			}

			d.ioMu.Lock()
			if d.file != nil {
				if err := d.writer.Flush(); err != nil {
					util.Error("flush failed in shutdown: %v", err)
				}
				if err := d.file.Sync(); err != nil {
					util.Error("sync failed during shutdown: %v", err)
				}
				if err := d.file.Close(); err != nil {
					util.Error("close failed during shutdown: %v", err)
				}
				d.file = nil
				d.writer = nil
			}
			d.ioMu.Unlock()
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

// writeBatch writes a batch of messages into the current segment file.
func (d *DiskHandler) writeBatch(batch []types.DiskMessage) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ioMu.Lock()
	defer d.ioMu.Unlock()

	if d.file == nil {
		if err := d.openSegment(); err != nil {
			util.Fatal("failed to open segment: %v", err)
		}
	}

	if len(batch) == 0 {
		return
	}

	serializedMsgs := make([][]byte, 0, len(batch))
	totalSize := 0

	for _, msg := range batch {
		serialized, err := util.SerializeDiskMessage(msg)
		if err != nil {
			util.Error("failed to serialize message: %v", err)
			continue
		}
		if len(serialized) > 0xFFFFFFFF {
			util.Error("message too large to write: %d bytes", len(serialized))
			continue
		}
		serializedMsgs = append(serializedMsgs, serialized)
		totalSize += 4 + len(serialized)
	}

	if len(serializedMsgs) == 0 {
		return
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
			util.Error("rotateSegment failed: %v", err)
			return
		}
	}

	if _, err := d.writer.Write(buffer); err != nil {
		util.Error("writeBatch failed: %v", err)
		return
	}

	d.CurrentOffset += uint64(len(buffer))
	d.AbsoluteOffset += uint64(len(serializedMsgs))

	if err := d.writer.Flush(); err != nil {
		util.Error("flush failed after batch: %v", err)
		return
	}
}

// WriteDirect writes a single message immediately without batching.
func (d *DiskHandler) WriteDirect(topic string, partition int, offset uint64, payload string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ioMu.Lock()
	defer d.ioMu.Unlock()

	diskMsg := types.DiskMessage{
		Topic:     topic,
		Partition: int32(partition),
		Offset:    offset,
		Payload:   payload,
	}

	serialized, err := util.SerializeDiskMessage(diskMsg)
	if err != nil {
		util.Error("failed to serialize message: %v", err)
		return
	}

	if len(serialized) > 0xFFFFFFFF {
		util.Error("message too large to write: %d bytes", len(serialized))
		return
	}

	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(serialized)))
	totalLen := uint64(4 + len(serialized))

	if d.CurrentOffset+totalLen > d.SegmentSize {
		if err := d.rotateSegment(); err != nil {
			util.Fatal("rotateSegment failed: %v", err)
		}
	}

	if _, err := d.writer.Write(lenBuf[:]); err != nil {
		util.Error("writeDirect failed writing length: %v", err)
		return
	}
	if _, err := d.writer.Write(serialized); err != nil {
		util.Error("writeDirect failed writing data: %v", err)
		return
	}

	d.CurrentOffset += totalLen
	d.AbsoluteOffset++

	if err := d.writer.Flush(); err != nil {
		util.Error("flush failed in WriteDirect: %v", err)
	}
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
		d.writeBatch(batch)
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

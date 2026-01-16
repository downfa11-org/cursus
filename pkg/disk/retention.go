package disk

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/downfa11-org/cursus/pkg/config"
	"github.com/downfa11-org/cursus/util"
)

type ReadSession struct {
	File    *os.File
	handler *DiskHandler
}

func (s *ReadSession) Close() error {
	atomic.AddInt32(&s.handler.activeReaders, -1)
	return s.File.Close()
}

func (d *DiskHandler) OpenForRead(offset uint64) (*ReadSession, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	path, _, err := d.findSegmentForOffset(offset)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	atomic.AddInt32(&d.activeReaders, 1)
	return &ReadSession{
		File:    f,
		handler: d,
	}, nil
}

func (d *DiskHandler) EnforceRetention(cfg *config.Config) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if readers := atomic.LoadInt32(&d.activeReaders); readers > 0 {
		util.Debug("Retention: deferred (active readers: %d)", readers)
		return
	}

	pattern := d.BaseName + "_segment_*.log"
	files, _ := filepath.Glob(pattern)
	if len(files) <= 1 {
		return
	}
	sort.Strings(files)

	var totalSize int64
	for _, f := range files {
		if info, err := os.Stat(f); err == nil {
			totalSize += info.Size()
		}
	}

	now := time.Now()
	retentionDuration := time.Duration(cfg.RetentionHours) * time.Hour

	for i := 0; i < len(files)-1; i++ {
		filePath := files[i]
		info, err := os.Stat(filePath)
		if err != nil {
			continue
		}

		if info.Mode().Perm() != 0444 {
			if _, err := os.Stat(filePath); err == nil {
				if err := os.Chmod(filePath, 0444); err != nil {
					util.Error("failed to set read-only permission: %v", err)
				}
			}
			indexPath := filePath[:len(filePath)-4] + ".index"
			if _, err := os.Stat(indexPath); err == nil {
				if err := os.Chmod(indexPath, 0444); err != nil {
					util.Error("failed to set read-only permission: %v", err)
				}
			}
			util.Debug("Segment %d closed and secured (read-only)", d.CurrentSegment)
		}

		isExpired := cfg.RetentionHours > 0 && now.Sub(info.ModTime()) > retentionDuration
		isOverCapacity := cfg.RetentionBytes > 0 && totalSize > cfg.RetentionBytes

		if isExpired || isOverCapacity {
			if atomic.LoadInt32(&d.activeReaders) == 0 {
				d.markAsDeleted(filePath)
			} else {
				util.Debug("Retention: %s is target but busy, skipping", filePath)
				break
			}
		}
	}
}

func (d *DiskHandler) markAsDeleted(logPath string) error {
	fileName := filepath.Base(logPath)
	parts := strings.Split(fileName, "_")
	if len(parts) < 4 {
		return fmt.Errorf("invalid segment filename: %s", fileName)
	}
	numStr := strings.TrimSuffix(parts[3], ".log")
	segmentOffset, _ := strconv.ParseUint(numStr, 10, 64)

	deletedLogPath := logPath + ".deleted"
	indexPath := logPath[:len(logPath)-4] + ".index"
	deletedIndexPath := indexPath + ".deleted"

	if err := os.Rename(logPath, deletedLogPath); err != nil {
		return err
	}

	if _, err := os.Stat(indexPath); err == nil {
		if err := os.Rename(indexPath, deletedIndexPath); err != nil {
			return fmt.Errorf("rename index failed: %w", err)
		}
	}

	for i, s := range d.segments {
		if s == segmentOffset {
			d.segments = append(d.segments[:i], d.segments[i+1:]...)
			break
		}
	}

	return nil
}

func (d *DiskHandler) retentionLoop(cfg *config.Config) {
	interval := cfg.RetentionCheckIntervalMS
	if interval <= 0 {
		interval = 300000
	}

	ticker := time.NewTicker(time.Duration(interval) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if cfg.CleanupPolicy == "delete" {
				d.EnforceRetention(cfg)
			}
		case <-d.done:
			return
		}
	}
}

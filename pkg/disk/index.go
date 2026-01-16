package disk

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"sort"

	"github.com/downfa11-org/cursus/pkg/types"
	"github.com/downfa11-org/cursus/util"
	"golang.org/x/exp/mmap"
)

func (d *DiskHandler) openIndexFiles() error {
	indexPath := d.GetIndexPath(d.CurrentSegment)
	f, err := os.OpenFile(indexPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		if os.IsPermission(err) {
			util.Debug("Index file %s is read-only, opening in read-only mode", indexPath)
			f, err = os.Open(indexPath)
			if err != nil {
				return fmt.Errorf("failed to open read-only index: %w", err)
			}
		} else {
			return fmt.Errorf("failed to open index file: %w", err)
		}
	}

	var writer *bufio.Writer
	info, err := f.Stat()
	if err == nil && info.Mode().Perm()&0200 != 0 {
		writer = bufio.NewWriter(f)
	}

	d.indexMu.Lock()
	d.indexFile = f
	d.indexBytesWritten = uint64(info.Size())
	d.indexWriter = writer
	err = d.refreshIndexMapper(indexPath)
	d.indexMu.Unlock()

	return err
}

func (d *DiskHandler) refreshIndexMapper(path string) error {
	if d.indexMapper != nil {
		d.indexMapper.Close()
	}
	mapper, err := mmap.Open(path)
	if err != nil {
		return err
	}
	d.indexMapper = mapper
	return nil
}

func (d *DiskHandler) closeIndexFiles() error {
	var errs []error

	if d.indexWriter != nil {
		if err := d.indexWriter.Flush(); err != nil {
			return fmt.Errorf("flush index writer failed: %w", err)
		}
	}
	if d.indexFile != nil {
		if err := d.indexFile.Sync(); err != nil {
			return fmt.Errorf("sync index file fialed: %w", err)
		}
	}

	if d.indexMapper != nil {
		if err := d.indexMapper.Close(); err != nil {
			errs = append(errs, err)
		}
		d.indexMapper = nil
	}

	if d.indexFile != nil {
		if err := d.indexFile.Close(); err != nil {
			errs = append(errs, err)
		}
		d.indexFile = nil
	}

	if len(errs) > 0 {
		return fmt.Errorf("closeIndexFiles errors: %v", errs)
	}
	return nil
}

func getLastOffsetFromIndex(indexPath string, baseOffset uint64) (lastOffset uint64, count int, err error) {
	const entrySize = types.IndexEntrySize
	info, err := os.Stat(indexPath)

	if err != nil || info.Size() < entrySize {
		return 0, 0, fmt.Errorf("index too small or not found")
	}

	f, err := os.Open(indexPath)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()

	buf := make([]byte, entrySize)
	_, err = f.ReadAt(buf, info.Size()-entrySize)
	if err != nil {
		return 0, 0, err
	}

	lastOffset = binary.BigEndian.Uint64(buf[0:8])
	count = int(lastOffset - baseOffset + 1)
	return lastOffset, count, nil
}

// findSegmentForOffset finds which segment contains the given offset
func (dh *DiskHandler) findSegmentForOffset(offset uint64) (string, uint64, error) {
	dh.mu.Lock()
	defer dh.mu.Unlock()

	if len(dh.segments) == 0 {
		return "", 0, fmt.Errorf("no segments available")
	}

	idx := sort.Search(len(dh.segments), func(i int) bool {
		return dh.segments[i] > offset
	})

	var baseOffset uint64
	if idx > 0 {
		baseOffset = dh.segments[idx-1]
	} else {
		baseOffset = dh.CurrentSegment
	}

	if offset >= dh.CurrentSegment {
		baseOffset = dh.CurrentSegment
	}

	return dh.GetSegmentPath(baseOffset), baseOffset, nil
}

func (d *DiskHandler) FindOffsetPosition(offset uint64) (uint64, error) {
	d.indexMu.RLock()
	defer d.indexMu.RUnlock()

	if d.indexMapper == nil {
		return 0, fmt.Errorf("index not available")
	}

	const entrySize = types.IndexEntrySize
	length := d.indexMapper.Len()
	entryCount := length / entrySize

	low, high := 0, entryCount-1
	var lastFoundPos uint64 = 0
	buf := make([]byte, entrySize)

	for low <= high {
		mid := (low + high) / 2
		_, err := d.indexMapper.ReadAt(buf, int64(mid*entrySize))
		if err != nil {
			break
		}

		eOffset := binary.BigEndian.Uint64(buf[0:8])
		ePosition := binary.BigEndian.Uint64(buf[8:16])

		if eOffset == 0 && ePosition == 0 && mid > 0 {
			high = mid - 1
			continue
		}

		if eOffset == offset {
			return ePosition, nil
		} else if eOffset < offset {
			lastFoundPos = ePosition
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return lastFoundPos, nil
}

package offset

import (
	"fmt"
	"sync"
)

type OffsetManager struct {
	mu      sync.RWMutex
	offsets map[string]map[string]map[int]int64 // groupID -> topic -> partition -> offset
}

func NewOffsetManager() *OffsetManager {
	return &OffsetManager{
		offsets: make(map[string]map[string]map[int]int64),
	}
}

func (om *OffsetManager) GetOffset(groupID, topic string, partition int) (int64, error) {
	om.mu.RLock()
	defer om.mu.RUnlock()

	if topics, ok := om.offsets[groupID]; ok {
		if partitions, ok := topics[topic]; ok {
			if offset, ok := partitions[partition]; ok {
				return offset, nil
			}
		}
	}
	return -1, fmt.Errorf("no offset found")
}

func (om *OffsetManager) CommitOffset(groupID, topic string, partition int, offset int64) error {
	om.mu.Lock()
	defer om.mu.Unlock()

	if _, ok := om.offsets[groupID]; !ok {
		om.offsets[groupID] = make(map[string]map[int]int64)
	}
	if _, ok := om.offsets[groupID][topic]; !ok {
		om.offsets[groupID][topic] = make(map[int]int64)
	}
	om.offsets[groupID][topic][partition] = offset
	return nil
}

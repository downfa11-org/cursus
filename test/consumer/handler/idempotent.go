package handler

import (
	"hash/fnv"
	"log"
	"sync"
	"sync/atomic"

	"github.com/downfa11-org/go-broker/consumer/types"
)

type BatchHandler interface {
	Handle([]types.Message) error
}

type MemoryIdempotentHandler struct {
	processedSeqNums map[string]map[uint64]struct{}
	processedHashes  map[uint64]struct{}
	mu               sync.RWMutex

	enableBenchmark bool
	processedCount  int64
}

func NewMemoryIdempotentHandler(enableBenchmark bool) *MemoryIdempotentHandler {
	return &MemoryIdempotentHandler{
		processedSeqNums: make(map[string]map[uint64]struct{}),
		processedHashes:  make(map[uint64]struct{}),
		enableBenchmark:  enableBenchmark,
	}
}
func (h *MemoryIdempotentHandler) Handle(msgs []types.Message) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	var newlyProcessed int64
	for _, msg := range msgs {
		isDuplicate := false

		if msg.ProducerID == "" || msg.SeqNum == 0 {
			// at-least-once [payload hash]
			payloadHash := generateHash(msg.Payload)
			if _, exists := h.processedHashes[payloadHash]; exists {
				isDuplicate = true
			} else {
				h.processedHashes[payloadHash] = struct{}{}
			}
		} else {
			// exactly-once [ProducerID+SeqNum]
			if seqMap, exists := h.processedSeqNums[msg.ProducerID]; exists {
				if _, processed := seqMap[msg.SeqNum]; processed {
					isDuplicate = true
				}
			} else {
				h.processedSeqNums[msg.ProducerID] = make(map[uint64]struct{})
			}

			if !isDuplicate {
				h.processedSeqNums[msg.ProducerID][msg.SeqNum] = struct{}{}
			}
		}

		if !isDuplicate {
			newlyProcessed++
		}

		if !h.enableBenchmark && !isDuplicate {
			if err := h.processMessage(msg); err != nil {
				return err
			}
		}
	}

	if h.enableBenchmark && newlyProcessed > 0 {
		atomic.AddInt64(&h.processedCount, newlyProcessed)
	}

	return nil
}

func (h *MemoryIdempotentHandler) processMessage(msg types.Message) error {
	const maxPrintLen = 30

	payloadStr := string(msg.Payload)
	if len(payloadStr) > maxPrintLen {
		payloadStr = payloadStr[:maxPrintLen] + "..."
	}

	log.Printf("Processing message Payload: '%s'", payloadStr)
	return nil
}

func (h *MemoryIdempotentHandler) GetProcessedCount() int64 {
	return atomic.SwapInt64(&h.processedCount, 0)
}

func generateHash(payload string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(payload))
	return h.Sum64()
}

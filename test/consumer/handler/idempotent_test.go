package handler

import (
	"fmt"
	"sync"
	"testing"

	"github.com/downfa11-org/go-broker/consumer/types"
)

func TestMemoryIdempotentHandler_Handle_AtLeastOnce(t *testing.T) {
	h := NewMemoryIdempotentHandler(false)

	msgs := []types.Message{
		{Payload: "msg1"},
		{Payload: "msg2"},
		{Payload: "msg3"},
	}

	if err := h.Handle(msgs); err != nil {
		t.Fatalf("Handle failed: %v", err)
	}

	if err := h.Handle(msgs); err != nil {
		t.Fatalf("Handle failed on repeated messages: %v", err)
	}

	h.mu.RLock()
	if len(h.processedHashes) != 3 {
		t.Errorf("Expected 3 processed hashes, got %d", len(h.processedHashes))
	}
	h.mu.RUnlock()
}

func TestMemoryIdempotentHandler_Handle_ExactlyOnce(t *testing.T) {
	h := NewMemoryIdempotentHandler(false)

	msgs := []types.Message{
		{ProducerID: "p1", SeqNum: 1, Payload: "msg1"},
		{ProducerID: "p1", SeqNum: 2, Payload: "msg2"},
		{ProducerID: "p1", SeqNum: 3, Payload: "msg3"},
	}

	if err := h.Handle(msgs); err != nil {
		t.Fatalf("Handle failed: %v", err)
	}

	if err := h.Handle(msgs); err != nil {
		t.Fatalf("Handle failed on repeated messages: %v", err)
	}

	h.mu.RLock()
	if len(h.processedSeqNums) != 1 {
		t.Errorf("Expected 1 producer, got %d", len(h.processedSeqNums))
	}
	if len(h.processedSeqNums["p1"]) != 3 {
		t.Errorf("Expected 3 seq nums for p1, got %d", len(h.processedSeqNums["p1"]))
	}
	h.mu.RUnlock()
}

func TestMemoryIdempotentHandler_Handle_Concurrent(t *testing.T) {
	h := NewMemoryIdempotentHandler(false)

	total := 1000
	var msgs []types.Message
	for i := 0; i < total; i++ {
		msgs = append(msgs, types.Message{Payload: fmt.Sprintf("payload-%d", i)})
	}

	wg := sync.WaitGroup{}
	concurrency := 10
	wg.Add(concurrency)

	errCh := make(chan error, concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			if err := h.Handle(msgs); err != nil {
				errCh <- err
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Errorf("Concurrent Handle failed: %v", err)
	}

	h.mu.RLock()
	if len(h.processedHashes) != total {
		t.Errorf("Expected %d processed hashes, got %d", total, len(h.processedHashes))
	}
	h.mu.RUnlock()
}

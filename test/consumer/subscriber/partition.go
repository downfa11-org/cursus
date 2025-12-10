package subscriber

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/downfa11-org/go-broker/consumer/types"
	"github.com/downfa11-org/go-broker/util"
)

type PartitionConsumer struct {
	partitionID int
	consumer    *Consumer
	offset      uint64
	conn        net.Conn
	mu          sync.Mutex
	closed      bool
}

func (pc *PartitionConsumer) ensureConnection() error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.closed {
		return fmt.Errorf("partition consumer closed")
	}

	if pc.conn != nil {
		pc.conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		if _, err := pc.conn.Read([]byte{}); err != nil {
			if !errors.Is(err, io.EOF) && !strings.Contains(err.Error(), "timeout") {
				pc.conn.Close()
				pc.conn = nil
			}
		} else {
			pc.conn.SetReadDeadline(time.Time{})
			return nil
		}
	}

	var err error
	for i := 0; i < pc.consumer.config.MaxConnectRetries; i++ {
		pc.conn, err = pc.consumer.client.Connect(pc.consumer.config.BrokerAddr)
		if err == nil {
			return nil
		}
		util.Error("Partition [%d] connect retry %d failed: %v", pc.partitionID, i+1, err)
		duration := time.Duration(pc.consumer.config.ConnectRetryBackoffMS) * time.Millisecond
		time.Sleep(duration)
	}
	return fmt.Errorf("failed to connect after retries: %w", err)
}

func (pc *PartitionConsumer) pollAndProcess() {
	if err := pc.ensureConnection(); err != nil {
		util.Warn("Partition [%d] cannot poll: %v", pc.partitionID, err)
		return
	}

	pc.mu.Lock()
	conn := pc.conn
	currentOffset := pc.offset
	pc.mu.Unlock()

	pc.consumer.mu.RLock()
	memberID := pc.consumer.memberID
	generation := pc.consumer.generation
	pc.consumer.mu.RUnlock()

	consumeCmd := fmt.Sprintf("CONSUME topic=%s partition=%d offset=%d group=%s gen=%d member=%s",
		pc.consumer.config.Topic, pc.partitionID, currentOffset, pc.consumer.config.GroupID, generation, memberID)

	if err := util.WriteWithLength(conn, util.EncodeMessage(pc.consumer.config.Topic, consumeCmd)); err != nil {
		util.Error("Partition [%d] send command failed: %v", pc.partitionID, err)
		return
	}

	batchData, err := util.ReadWithLength(conn)
	if err != nil {
		util.Error("Partition [%d] read batch error: %v", pc.partitionID, err)
		return
	}

	batch, err := types.DecodeBatchMessages(batchData)
	if err != nil {
		util.Error("Partition [%d] decode batch error: %v", pc.partitionID, err)
		return
	}

	if pc.consumer.config.EnableBenchmark {
		pc.printConsumedMessage(batch)
	}

	if len(batch.Messages) > 0 {
		// first := batch.Messages[0], last := batch.Messages[len(batch.Messages)-1]
		if err := pc.consumer.processBatchSync(batch.Messages, pc.partitionID); err != nil {
			util.Error("Partition [%d] batch processing error: %v", pc.partitionID, err)
		}
		pc.updateOffsetAndCommit(batch.Messages)
	}
}

func (pc *PartitionConsumer) printConsumedMessage(batch *types.Batch) {
	util.Info("📥 Partition [%d] Batch Received: Topic='%s', TotalMessages=%d",
		pc.partitionID, batch.Topic, len(batch.Messages))

	if len(batch.Messages) > 0 {
		util.Info("   ├─ Message Details (First 5 messages):")

		limit := 5
		if len(batch.Messages) < limit {
			limit = len(batch.Messages)
		}

		for i := 0; i < limit; i++ {
			msg := batch.Messages[i]

			payload := msg.Payload
			if len(payload) > 50 {
				payload = payload[:50] + "..."
			}

			if msg.Key == "" {
				util.Info("   │  └─ Msg %d: Payload='%s'", i, payload)
			} else {
				util.Info("   │  └─ Msg %d: Key=%s, Payload='%s'", i, msg.Key, payload)
			}
		}

		if len(batch.Messages) > 5 {
			util.Info("   └─ ... and %d more messages.", len(batch.Messages)-5)
		} else {
			util.Info("   └─ All messages listed above.")
		}
	}
}

func (pc *PartitionConsumer) updateOffsetAndCommit(msgs []types.Message) {
	if len(msgs) == 0 {
		return
	}

	lastOffset := msgs[len(msgs)-1].Offset

	pc.mu.Lock()
	pc.offset = lastOffset + 1
	pc.mu.Unlock()

	pc.commitOffsetAt(lastOffset)
}

func (pc *PartitionConsumer) commitOffsetAt(offset uint64) {
	select {
	case pc.consumer.commitCh <- commitEntry{
		partition: pc.partitionID,
		offset:    offset,
	}:
	default:
		go func() {
			if atomic.LoadInt32(&pc.consumer.rebalancing) == 1 {
				return
			}
			if err := pc.consumer.directCommit(pc.partitionID, offset); err != nil {
				util.Error("Partition [%d] direct commit failed: %v", pc.partitionID, err)
				if strings.Contains(err.Error(), "GEN_MISMATCH") {
					util.Warn("Generation mismatch detected, waiting for commitWorker to handle")
					return
				}
				util.Error("Direct commit failed, retrying async commit")
				select {
				case pc.consumer.commitCh <- commitEntry{
					partition: pc.partitionID,
					offset:    offset,
				}:
				default:
					util.Error("Async commit also failed for partition [%d]", pc.partitionID)
				}
			}
		}()
	}
}

func (pc *PartitionConsumer) commitOffset() {
	pc.mu.Lock()
	if pc.offset == 0 {
		pc.mu.Unlock()
		return // nothing to commit yet
	}
	currentOffset := pc.offset - 1
	pc.mu.Unlock()

	conn, err := pc.consumer.client.Connect(pc.consumer.config.BrokerAddr)
	if err != nil {
		util.Error("Partition [%d] commit connect failed: %v", pc.partitionID, err)
		return
	}
	defer conn.Close()

	commitCmd := fmt.Sprintf("COMMIT_OFFSET topic=%s partition=%d group=%s offset=%d",
		pc.consumer.config.Topic, pc.partitionID, pc.consumer.config.GroupID, currentOffset)
	if err := util.WriteWithLength(conn, util.EncodeMessage(pc.consumer.config.Topic, commitCmd)); err != nil {
		util.Error("Partition [%d] commit send failed: %v", pc.partitionID, err)
		return
	}

	resp, err := util.ReadWithLength(conn)
	if err != nil {
		util.Error("Partition [%d] commit response failed: %v", pc.partitionID, err)
		return
	}

	if strings.Contains(string(resp), "ERROR:") {
		util.Error("Partition [%d] commit error: %s", pc.partitionID, string(resp))
	}

	pc.consumer.mu.Lock()
	pc.consumer.offsets[pc.partitionID] = currentOffset
	pc.consumer.mu.Unlock()
}

func (pc *PartitionConsumer) close() {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.closed {
		return
	}
	pc.closed = true
	if pc.conn != nil {
		pc.conn.Close()
		pc.conn = nil
	}
}

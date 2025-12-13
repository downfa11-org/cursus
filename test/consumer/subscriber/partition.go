package subscriber

import (
	"fmt"
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
		pc.conn.SetReadDeadline(time.Now().Add(1 * time.Millisecond))
		_, err := pc.conn.Read(make([]byte, 1))
		if err == nil {
			pc.conn.SetReadDeadline(time.Time{})
			return nil
		}
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			pc.conn.SetReadDeadline(time.Time{})
			return nil
		}
		pc.conn.Close()
		pc.conn = nil
	}

	var err error
	for attempt := 0; attempt < pc.consumer.config.MaxConnectRetries; attempt++ {
		conn, broker, connectErr := pc.consumer.client.ConnectWithFailover()
		if connectErr == nil {
			pc.conn = conn
			util.Info("Partition [%d] connected to %s", pc.partitionID, broker)
			return nil
		}
		err = connectErr
		util.Info("Partition [%d] connect attempt %d failed: %v", pc.partitionID, attempt+1, err)

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

	util.Debug("Partition [%d] Polling at offset %d", pc.partitionID, currentOffset)

	pc.consumer.mu.RLock()
	memberID := pc.consumer.memberID
	generation := pc.consumer.generation
	pc.consumer.mu.RUnlock()

	consumeCmd := fmt.Sprintf("CONSUME topic=%s partition=%d offset=%d group=%s generation=%d member=%s",
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
		util.Debug("Partition [%d] Received %d messages, offsets %d to %d",
			pc.partitionID, len(batch.Messages),
			batch.Messages[0].Offset, batch.Messages[len(batch.Messages)-1].Offset)

		if err := pc.consumer.processBatchSync(batch.Messages, pc.partitionID); err != nil {
			util.Error("Partition [%d] batch processing error: %v", pc.partitionID, err)
		}
		pc.updateOffsetAndCommit(batch.Messages)
	} else {
		util.Debug("Partition [%d] No messages received at offset %d", pc.partitionID, currentOffset)
	}
}

func (pc *PartitionConsumer) startStreamLoop() {
	pid := pc.partitionID
	c := pc.consumer
	retryDelay := time.Duration(c.config.StreamingRetryIntervalMS) * time.Millisecond

	for {
		select {
		case <-c.doneCh:
			pc.closeConnection()
			return
		default:
		}

		if atomic.LoadInt32(&c.rebalancing) == 1 {
			pc.closeConnection()
			time.Sleep(1 * time.Second)
			continue
		}

		if err := pc.ensureConnection(); err != nil {
			util.Warn("Partition [%d] streaming connection failed, retrying: %v", pid, err)
			time.Sleep(retryDelay)
			continue
		}

		pc.mu.Lock()
		conn := pc.conn
		currentOffset := pc.offset
		pc.mu.Unlock()

		c.mu.RLock()
		memberID := c.memberID
		generation := c.generation
		c.mu.RUnlock()

		streamCmd := fmt.Sprintf("STREAM topic=%s partition=%d group=%s offset=%d generation=%d member=%s",
			c.config.Topic, pid, c.config.GroupID, currentOffset, generation, memberID)
		util.Debug("📤 Partition [%d] sending STREAM command with offset %d", pid, currentOffset)

		if err := util.WriteWithLength(conn, util.EncodeMessage("", streamCmd)); err != nil {
			util.Error("Partition [%d] STREAM command send failed: %v", pid, err)
			pc.closeConnection()
			time.Sleep(retryDelay)
			continue
		}

		idleTimeout := time.Duration(c.config.StreamingReadDeadlineMS) * time.Millisecond

		for {
			select {
			case <-c.doneCh:
				return
			default:
			}

			conn.SetReadDeadline(time.Now().Add(idleTimeout))
			batchData, err := util.ReadWithLength(conn)
			if err != nil {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					util.Debug("Partition [%d] idle timeout, continuing stream read.", pid)
					continue
				}

				util.Error("Partition [%d] Stream read fatal error: %v", pid, err)
				pc.closeConnection()
				break
			}

			if len(batchData) == 0 {
				continue // keepalive
			}

			batch, err := types.DecodeBatchMessages(batchData)
			if err != nil {
				util.Error("Partition [%d] Failed to decode batch message: %v", pid, err)
				pc.closeConnection()
				break
			}

			if len(batch.Messages) > 0 {
				if err := c.processBatchSync(batch.Messages, pid); err != nil {
					util.Error("Partition [%d] Failed to process stream batch: %v", pid, err)
					pc.closeConnection()
					break
				}

				pc.updateOffsetAndCommit(batch.Messages)
				lastOffset := batch.Messages[len(batch.Messages)-1].Offset
				util.Debug("Partition [%d] Processed batch of %d messages, last offset: %d", pid, len(batch.Messages), lastOffset)
			}
		}

		time.Sleep(retryDelay)
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

	if pc.consumer.isDistributedMode() {
		if err := pc.commitOffsetWithRetry(lastOffset); err != nil {
			util.Error("Partition [%d] Failed to commit offset %d: %v", pc.partitionID, lastOffset, err)
			return
		}
	} else {
		pc.commitOffsetAt(lastOffset)
	}
}

func (pc *PartitionConsumer) commitOffsetWithRetry(offset uint64) error {
	const maxRetries = 3
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		select {
		case pc.consumer.commitCh <- commitEntry{
			partition: pc.partitionID,
			offset:    offset,
		}:
			return nil
		default:
			if err := pc.consumer.directCommit(pc.partitionID, offset); err != nil {
				lastErr = err
				util.Warn("Partition [%d] Direct commit attempt %d failed: %v", pc.partitionID, attempt+1, err)
				time.Sleep(time.Duration(100*(attempt+1)) * time.Millisecond)
				continue
			}
			return nil
		}
	}

	return fmt.Errorf("commit failed after %d attempts: %w", maxRetries, lastErr)
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

	conn, _, err := pc.consumer.client.ConnectWithFailover()
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

func (pc *PartitionConsumer) closeConnection() {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.conn != nil {
		pc.conn.Close()
		pc.conn = nil
	}
}

package subscriber

import (
	"fmt"
	"strings"
	"time"

	"github.com/downfa11-org/go-broker/consumer/types"
	"github.com/downfa11-org/go-broker/util"
)

func (pc *PartitionConsumer) ensureConnection() error {
	if pc.consumer.mainCtx.Err() != nil {
		return fmt.Errorf("consumer shutting down")
	}

	pc.mu.Lock()
	if pc.conn != nil {
		pc.mu.Unlock()
		return nil
	}
	if pc.closed {
		pc.mu.Unlock()
		return fmt.Errorf("partition consumer closed")
	}
	pc.mu.Unlock()

	bo := newBackoff(
		time.Duration(pc.consumer.config.ConnectRetryBackoffMS)*time.Millisecond, 5*time.Second,
	)

	var err error
	for attempt := 0; attempt < pc.consumer.config.MaxConnectRetries; attempt++ {
		pc.mu.Lock()
		if pc.closed {
			pc.mu.Unlock()
			return fmt.Errorf("partition consumer closed during connection attempts")
		}
		pc.mu.Unlock()

		conn, broker, connectErr := pc.consumer.client.ConnectWithFailover()
		if connectErr == nil {
			pc.mu.Lock()
			if pc.closed {
				conn.Close()
				pc.mu.Unlock()
				return fmt.Errorf("partition consumer closed")
			}
			pc.conn = conn
			pc.mu.Unlock()
			util.Info("Partition [%d] connected to %s", pc.partitionID, broker)
			return nil
		}

		err = connectErr
		wait := bo.duration()
		util.Warn("Partition [%d] connect fail (attempt %d): %v. Retrying in %v", pc.partitionID, attempt+1, err, wait)
		time.Sleep(wait)
	}
	return fmt.Errorf("failed to connect after retries: %w", err)
}

func (pc *PartitionConsumer) handleBrokerError(data []byte) bool {
	respStr := string(data)
	if !strings.HasPrefix(respStr, "ERROR:") {
		return false
	}

	util.Warn("Partition [%d] broker error: %s", pc.partitionID, respStr)

	if strings.Contains(respStr, "NOT_LEADER") {
		pc.consumer.handleLeaderRedirection(respStr)
	}

	if strings.Contains(respStr, "GEN_MISMATCH") || strings.Contains(respStr, "REBALANCE_REQUIRED") {
		pc.close()
		pc.consumer.handleRebalanceSignal()
		return true
	}

	pc.closeConnection()

	wait := time.Duration(100+(pc.partitionID*50)) * time.Millisecond
	time.Sleep(wait) // jitter
	return true
}

func (pc *PartitionConsumer) commitOffsetWithRetry(offset uint64) error {
	const maxRetries = 3
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		if pc.consumer.mainCtx.Err() != nil {
			return fmt.Errorf("stopping commit: consumer context cancelled")
		}

		resultCh := make(chan error, 1)

		pc.consumer.commitResultMu.Lock()
		pc.consumer.commitResultCh[pc.partitionID] = resultCh
		pc.consumer.commitResultMu.Unlock()

		select {
		case pc.consumer.commitCh <- commitEntry{
			partition: pc.partitionID,
			offset:    offset,
		}:
			select {
			case err := <-resultCh:
				if err != nil {
					util.Error("Partition %d batch commit result error: %v", pc.partitionID, err)
					lastErr = err
				} else {
					util.Debug("Partition %d batch commit success for offset %d", pc.partitionID, offset)
					return nil
				}
			case <-pc.consumer.mainCtx.Done():
				return fmt.Errorf("commit cancelled due to rebalance")
			case <-time.After(20 * time.Second):
				util.Warn("Partition %d batch commit-timeout for offset %d", pc.partitionID, offset)
				return fmt.Errorf("commit timeout")
			}
		default:
			util.Warn("Partition %d commitCh full, attempting directCommit for offset %d", pc.partitionID, offset)
			if err := pc.consumer.directCommit(pc.partitionID, offset); err != nil {
				util.Error("Partition %d directCommit failed: %v", pc.partitionID, err)
				lastErr = err
			} else {
				return nil
			}
		}
		backoff := time.Duration(200*(attempt+1)) * time.Millisecond
		time.Sleep(backoff)
	}

	return fmt.Errorf("commit failed after %d attempts: %w", maxRetries, lastErr)
}

func (pc *PartitionConsumer) printConsumedMessage(batch *types.Batch) {
	if len(batch.Messages) == 0 {
		return
	}

	util.Info("📥 Partition [%d] Batch Received: Topic='%s', TotalMessages=%d", pc.partitionID, batch.Topic, len(batch.Messages))
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
	pc.closeDataCh()
}

func (pc *PartitionConsumer) closeConnection() {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.conn != nil {
		pc.conn.Close()
		pc.conn = nil
	}
}

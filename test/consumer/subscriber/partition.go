package subscriber

import (
	"fmt"
	"net"
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

	dataCh chan []byte
	once   sync.Once
}

func (pc *PartitionConsumer) initWorker() {
	pc.once.Do(func() {
		pc.dataCh = make(chan []byte, 1000)
		go pc.runWorker()
	})
}

func (pc *PartitionConsumer) runWorker() {
	for {
		select {
		case <-pc.consumer.doneCh:
			return
		case batchData := <-pc.dataCh:
			batch, err := types.DecodeBatchMessages(batchData)
			if err != nil {
				util.Error("Partition [%d] decode error: %v", pc.partitionID, err)
				continue
			}

			if len(batch.Messages) > 0 {
				if pc.consumer.config.EnableBenchmark {
				} else {
					pc.printConsumedMessage(batch)
				}

				if err := pc.consumer.processBatchSync(batch.Messages, pc.partitionID); err != nil {
					util.Error("Partition [%d] process error: %v", pc.partitionID, err)
				}
				pc.updateOffsetAndCommit(batch.Messages)
			}
		}
	}
}

func (pc *PartitionConsumer) pollAndProcess() {
	pc.initWorker()

	if err := pc.ensureConnection(); err != nil {
		util.Warn("Partition [%d] cannot poll: %v", pc.partitionID, err)
		return
	}

	pc.mu.Lock()
	conn := pc.conn
	currentOffset := atomic.LoadUint64(&pc.offset)
	pc.mu.Unlock()

	util.Debug("Partition [%d] Polling at offset %d", pc.partitionID, currentOffset)

	c := pc.consumer
	c.mu.RLock()
	memberID := c.memberID
	generation := c.generation
	c.mu.RUnlock()

	consumeCmd := fmt.Sprintf("CONSUME topic=%s partition=%d offset=%d group=%s generation=%d member=%s",
		pc.consumer.config.Topic, pc.partitionID, currentOffset, pc.consumer.config.GroupID, generation, memberID)

	if err := util.WriteWithLength(conn, util.EncodeMessage(pc.consumer.config.Topic, consumeCmd)); err != nil {
		util.Error("Partition [%d] send command failed: %v", pc.partitionID, err)
		pc.closeConnection()
		return
	}

	batchData, err := util.ReadWithLength(conn)
	if err != nil {
		util.Error("Partition [%d] read batch error: %v", pc.partitionID, err)
		pc.closeConnection()
		return
	}
	if pc.handleBrokerError(batchData) {
		return
	}

	if len(batchData) > 0 {
		pc.dataCh <- batchData
	}
}

func (pc *PartitionConsumer) startStreamLoop() {
	pc.initWorker()
	pid := pc.partitionID
	c := pc.consumer

	pc.consumer.wg.Add(1)
	go pc.workerLoop()

	minRetry := time.Duration(c.config.StreamingRetryIntervalMS) * time.Millisecond
	bo := newBackoff(minRetry, 30*time.Second)

	for {
		select {
		case <-c.doneCh:
			pc.closeConnection()
			close(pc.dataCh)
			return
		default:
		}

		if atomic.LoadInt32(&c.rebalancing) == 1 {
			pc.closeConnection()
			time.Sleep(500 * time.Millisecond)
			continue
		}

		if err := pc.ensureConnection(); err != nil {
			util.Warn("Partition [%d] streaming connection failed, retrying: %v", pid, err)
			time.Sleep(bo.duration())
			continue
		}

		bo.reset()

		pc.mu.Lock()
		conn := pc.conn
		currentOffset := atomic.LoadUint64(&pc.offset)
		pc.mu.Unlock()

		c.mu.RLock()
		memberID, generation := c.memberID, c.generation
		c.mu.RUnlock()

		streamCmd := fmt.Sprintf("STREAM topic=%s partition=%d group=%s offset=%d generation=%d member=%s",
			c.config.Topic, pid, c.config.GroupID, currentOffset, generation, memberID)
		util.Debug("📤 Partition [%d] sending STREAM command with offset %d", pid, currentOffset)

		if err := util.WriteWithLength(conn, util.EncodeMessage("", streamCmd)); err != nil {
			util.Error("Partition [%d] STREAM command send failed: %v", pid, err)
			pc.closeConnection()
			time.Sleep(bo.duration())
			continue
		}

		idleTimeout := time.Duration(c.config.StreamingReadDeadlineMS) * time.Millisecond
		for {
			if atomic.LoadInt32(&c.rebalancing) == 1 {
				break
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

			if pc.handleBrokerError(batchData) {
				break
			}

			select {
			case pc.dataCh <- batchData:
			case <-c.doneCh:
				return
			}
			bo.reset()
		}
		time.Sleep(bo.duration())
	}
}

func (pc *PartitionConsumer) workerLoop() {
	defer pc.consumer.wg.Done()
	for data := range pc.dataCh {
		batch, err := types.DecodeBatchMessages(data)
		if err != nil {
			util.Error("Decode error on P[%d]: %v", pc.partitionID, err)
			continue
		}

		pc.consumer.processBatchSync(batch.Messages, pc.partitionID)

		if len(batch.Messages) > 0 {
			lastOffset := batch.Messages[len(batch.Messages)-1].Offset
			atomic.StoreUint64(&pc.offset, lastOffset+1)
			pc.consumer.commitCh <- commitEntry{pc.partitionID, lastOffset + 1}
		}
	}
}

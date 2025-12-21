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

	dataCh    chan *types.Batch
	once      sync.Once
	closeOnce sync.Once
}

func (pc *PartitionConsumer) initWorker() {
	pc.once.Do(func() {
		pc.dataCh = make(chan *types.Batch, 1000)
		pc.consumer.wg.Add(1)
		go pc.runWorker()
	})
}

func (pc *PartitionConsumer) closeDataCh() {
	pc.closeOnce.Do(func() {
		if pc.dataCh != nil {
			close(pc.dataCh)
		}
	})
}

func (pc *PartitionConsumer) runWorker() {
	defer pc.consumer.wg.Done()

	for batch := range pc.dataCh {
		msgCount := len(batch.Messages)
		if msgCount == 0 {
			continue
		}

		if !pc.consumer.config.EnableBenchmark {
			pc.printConsumedMessage(batch)
		}

		if err := pc.consumer.processBatchSync(batch.Messages, pc.partitionID); err != nil {
			util.Error("Partition [%d] process error: %v", pc.partitionID, err)
		}

		lastOffset := batch.Messages[len(batch.Messages)-1].Offset
		if err := pc.commitOffsetWithRetry(lastOffset + 1); err != nil {
			util.Error("Partition [%d] failed to commit offset: %v", pc.partitionID, err)
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

	if pc.handleBrokerError(batchData) || len(batchData) == 0 {
		return
	}

	batch, err := types.DecodeBatchMessages(batchData)
	if err != nil {
		util.Error("Partition [%d] decode error: %v", pc.partitionID, err)
		return
	}

	select {
	case pc.dataCh <- batch:
	case <-c.doneCh:
		pc.closeDataCh()
	}
}

func (pc *PartitionConsumer) startStreamLoop() {
	pc.initWorker()
	pid := pc.partitionID
	c := pc.consumer

	minRetry := time.Duration(c.config.StreamingRetryIntervalMS) * time.Millisecond
	bo := newBackoff(minRetry, 30*time.Second)
	defer pc.closeDataCh()

	for {
		select {
		case <-c.doneCh:
			pc.closeConnection()
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

			if len(batchData) == 0 || pc.handleBrokerError(batchData) {
				continue
			}

			batch, err := types.DecodeBatchMessages(batchData)
			if err != nil {
				continue
			}

			select {
			case pc.dataCh <- batch:
			case <-c.doneCh:
				return
			}
			bo.reset()
		}
		time.Sleep(bo.duration())
	}
}

package subscriber

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/downfa11-org/go-broker/consumer/bench"
	"github.com/downfa11-org/go-broker/consumer/client"
	"github.com/downfa11-org/go-broker/consumer/config"
	"github.com/downfa11-org/go-broker/consumer/handler"
	"github.com/downfa11-org/go-broker/consumer/types"
	"github.com/downfa11-org/go-broker/util"
)

type Consumer struct {
	config             config.ConsumerConfig
	client             *client.ConsumerClient
	partitionConsumers map[int]*PartitionConsumer

	generation    int64
	memberID      string
	commitCh      chan commitEntry
	wg            sync.WaitGroup
	sessionCtx    context.Context
	sessionCancel context.CancelFunc
	rebalancing   int32

	offsets map[int]uint64
	doneCh  chan struct{}
	mu      sync.RWMutex

	closed  bool
	closeMu sync.Mutex

	bmStartTime time.Time

	batchHandler handler.BatchHandler
	metrics      *bench.ConsumerMetrics
}

type commitEntry struct {
	partition int
	offset    uint64
}

func NewConsumer(cfg config.ConsumerConfig) (*Consumer, error) {
	client := client.NewConsumerClient()
	c := &Consumer{
		config:             cfg,
		client:             client,
		partitionConsumers: make(map[int]*PartitionConsumer),
		offsets:            make(map[int]uint64),
		doneCh:             make(chan struct{}),
	}

	if cfg.EnableBenchmark {
		c.metrics = bench.NewConsumerMetrics(int64(cfg.NumMessages))
	}

	c.commitCh = make(chan commitEntry, 1024)
	c.sessionCtx, c.sessionCancel = context.WithCancel(context.Background())

	return c, nil
}

func (c *Consumer) Start() error {
	gen, mid, assignments, err := c.joinGroup()
	if err != nil {
		return fmt.Errorf("join group failed: %w", err)
	}
	c.generation = gen
	c.memberID = mid

	if len(assignments) == 0 {
		assignments, err = c.syncGroup(gen, mid)
		if err != nil {
			return fmt.Errorf("sync group failed: %w", err)
		}
	}

	log.Printf("✅ Successfully joined topic '%s' for group '%s' with %d partitions: %v (generation=%d, member=%s)",
		c.config.Topic, c.config.GroupID, len(assignments), assignments, gen, mid)

	c.partitionConsumers = make(map[int]*PartitionConsumer)
	for _, pid := range assignments {
		pc := &PartitionConsumer{partitionID: pid, consumer: c, offset: 0}
		c.partitionConsumers[pid] = pc
	}

	if c.config.Mode != config.ModePolling {
		c.startStreaming()
	} else {
		c.startConsuming()
	}
	c.startCommitLoop()
	return nil
}

func (c *Consumer) SetBatchHandler(h handler.BatchHandler) {
	c.batchHandler = h
}

// heartbeatLoop runs in background; if coordinator indicates generation mismatch or error => trigger rejoin
func (c *Consumer) heartbeatLoop() {
	interval := time.Duration(c.config.HeartbeatIntervalMS) * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.doneCh:
			return
		case <-ticker.C:
			// send heartbeat with generation and memberID
			conn, err := c.client.Connect(c.config.BrokerAddr)
			if err != nil {
				log.Printf("heartbeat connect failed: %v", err)
				continue
			}

			hb := fmt.Sprintf("HEARTBEAT topic=%s group=%s member=%s generation=%d",
				c.config.Topic, c.config.GroupID, c.memberID, c.generation)

			if err := util.WriteWithLength(conn, util.EncodeMessage("", hb)); err != nil {
				log.Printf("heartbeat send failed: %v", err)
				conn.Close()
				continue
			}

			resp, err := util.ReadWithLength(conn)
			conn.Close()
			if err != nil {
				log.Printf("heartbeat response failed: %v", err)
				continue
			}

			respStr := string(resp)
			if strings.Contains(respStr, "REBALANCE_REQUIRED") || strings.Contains(respStr, "GEN_MISMATCH") {
				log.Printf("heartbeat indicated rebalance/mismatch: %s", respStr)
				c.handleRebalanceSignal()
				return
			}
		}
	}
}

func (c *Consumer) handleRebalanceSignal() {
	if !atomic.CompareAndSwapInt32(&c.rebalancing, 0, 1) {
		return
	}

	c.mu.Lock()
	for _, pc := range c.partitionConsumers {
		pc.close()
	}
	c.partitionConsumers = make(map[int]*PartitionConsumer)
	c.mu.Unlock()

	go func() {
		defer atomic.StoreInt32(&c.rebalancing, 0)
		time.Sleep(1 * time.Second)

		gen, mid, assignments, err := c.joinGroup()
		if err != nil {
			log.Printf("Rebalance join failed: %v", err)
			return
		}

		c.mu.Lock()
		c.generation = gen
		c.memberID = mid
		for _, pid := range assignments {
			pc := &PartitionConsumer{partitionID: pid, consumer: c, offset: 0}
			c.partitionConsumers[pid] = pc
		}
		c.mu.Unlock()
		c.startConsuming()
	}()
}

func (c *Consumer) startConsuming() {
	if atomic.LoadInt32(&c.rebalancing) == 1 {
		return
	}

	atomic.StoreInt32(&c.rebalancing, 1)
	defer atomic.StoreInt32(&c.rebalancing, 0)

	if c.config.EnableBenchmark {
		c.bmStartTime = time.Now()
	}

	c.mu.Lock()
	for pid := range c.partitionConsumers {
		log.Printf("Starting consumer for partition %d", pid)
	}
	c.mu.Unlock()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.heartbeatLoop()
	}()

	for pid, pc := range c.partitionConsumers {
		c.wg.Add(1)
		go func(pid int, pc *PartitionConsumer) {
			defer c.wg.Done()

			ticker := time.NewTicker(c.config.PollInterval)
			defer ticker.Stop()
			for {
				select {
				case <-c.doneCh:
					return
				case <-ticker.C:
					if !c.ownsPartition(pid) {
						return
					}
					pc.pollAndProcess()
				}
			}
		}(pid, pc)
	}
	atomic.StoreInt32(&c.rebalancing, 0)
}

func (c *Consumer) ownsPartition(pid int) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	pc, ok := c.partitionConsumers[pid]
	return ok && !pc.closed
}

func (c *Consumer) TriggerBenchmarkStop() {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()

	if c.closed {
		return
	}

	c.closed = true

	if c.metrics != nil {
		c.metrics.PrintSummary()
	}

	os.Exit(0)
}

func (c *Consumer) startCommitLoop() {
	go func() {
		ticker := time.NewTicker(c.config.AutoCommitInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if c.config.EnableAutoCommit {
					c.commitAllOffsets()
				}
			case <-c.doneCh:
				c.commitAllOffsets()
				return
			}
		}
	}()
}

func (c *Consumer) joinGroup() (generation int64, memberID string, assignments []int, err error) {
	log.Println("Joining group")
	if c.memberID != "" {
		c.mu.RLock()
		assignments = make([]int, 0, len(c.partitionConsumers))
		for pid := range c.partitionConsumers {
			assignments = append(assignments, pid)
		}
		c.mu.RUnlock()

		return c.generation, c.memberID, assignments, nil
	}

	conn, err := c.client.Connect(c.config.BrokerAddr)
	if err != nil {
		return 0, "", nil, fmt.Errorf("connect failed: %w", err)
	}
	defer conn.Close()

	joinCmd := fmt.Sprintf("JOIN_GROUP topic=%s group=%s member=%s",
		c.config.Topic, c.config.GroupID, c.config.ConsumerID)
	if err := util.WriteWithLength(conn, util.EncodeMessage("", joinCmd)); err != nil {
		return 0, "", nil, fmt.Errorf("send join command: %w", err)
	}

	resp, err := util.ReadWithLength(conn)
	if err != nil {
		return 0, "", nil, fmt.Errorf("read response: %w", err)
	}

	respStr := strings.TrimSpace(string(resp))
	if strings.HasPrefix(respStr, "ERROR:") {
		return 0, "", nil, fmt.Errorf("broker error: %s", respStr)
	}

	log.Printf("join-group: %s\n", respStr)
	// "OK generation=123 member=consumer-abc-1242 waiting"
	var gen int64 = 0
	var mid string
	var assigned []int

	if strings.Contains(respStr, "generation=") && strings.Contains(respStr, "member=") {
		parts := strings.Fields(respStr)
		for _, part := range parts {
			if strings.HasPrefix(part, "generation=") {
				fmt.Sscanf(part, "generation=%d", &gen)
			} else if strings.HasPrefix(part, "member=") {
				mid = strings.TrimPrefix(part, "member=")
			}
		}
	}

	if strings.Contains(respStr, "assignments=") {
		start := strings.Index(respStr, "[")
		end := strings.Index(respStr, "]")
		if start != -1 && end != -1 {
			partStr := respStr[start+1 : end]
			partStr = strings.ReplaceAll(partStr, ",", " ")
			parts := strings.Fields(partStr)

			for _, p := range parts {
				p = strings.TrimSpace(p)
				if p == "" {
					continue
				}

				pid, err := strconv.Atoi(p)
				if err == nil {
					assigned = append(assigned, pid)
				} else {
					log.Printf("⚠️ Error parsing partition ID '%s': %v", p, err)
				}
			}
		}
	}

	return gen, mid, assigned, nil
}

func (c *Consumer) syncGroup(generation int64, memberID string) ([]int, error) {
	conn, err := c.client.Connect(c.config.BrokerAddr)
	if err != nil {
		return nil, fmt.Errorf("connect failed: %w", err)
	}
	defer conn.Close()

	syncCmd := fmt.Sprintf("SYNC_GROUP topic=%s group=%s member=%s generation=%d",
		c.config.Topic, c.config.GroupID, memberID, generation)
	if err := util.WriteWithLength(conn, util.EncodeMessage("", syncCmd)); err != nil {
		return nil, fmt.Errorf("send sync command: %w", err)
	}

	resp, err := util.ReadWithLength(conn)
	if err != nil {
		return nil, fmt.Errorf("read sync response: %w", err)
	}

	respStr := strings.TrimSpace(string(resp))
	if strings.HasPrefix(respStr, "ERROR:") {
		return nil, fmt.Errorf("broker error: %s", respStr)
	}

	log.Printf("sync-group: %s\n", respStr)

	var assigned []int
	if strings.Contains(respStr, "[") && strings.Contains(respStr, "]") {
		start := strings.Index(respStr, "[")
		end := strings.Index(respStr, "]")
		partitionStr := respStr[start+1 : end]
		for _, p := range strings.Fields(partitionStr) {
			var pid int
			if _, err := fmt.Sscanf(p, "%d", &pid); err == nil {
				assigned = append(assigned, pid)
			}
		}
	}

	c.mu.Lock()
	c.generation = generation
	c.memberID = memberID
	c.mu.Unlock()

	return assigned, nil
}

func (c *Consumer) startStreaming() error {
	for pid, pc := range c.partitionConsumers {
		go func(pid int, pc *PartitionConsumer) {
			retryDelay := time.Duration(c.config.StreamingRetryIntervalMS) * time.Millisecond
			maxBatch := c.config.BatchSize
			if maxBatch > c.config.MaxPollRecords {
				maxBatch = c.config.MaxPollRecords
			}

			for {
				select {
				case <-c.doneCh:
					return
				default:
				}

				if atomic.LoadInt32(&c.rebalancing) == 1 {
					time.Sleep(100 * time.Millisecond)
					continue
				}

				if err := pc.ensureConnection(); err != nil {
					log.Printf("Partition [%d] streaming connection failed, retrying: %v", pid, err)
					time.Sleep(retryDelay)
					continue
				}

				pc.mu.Lock()
				conn := pc.conn
				pc.mu.Unlock()
				if conn == nil {
					time.Sleep(retryDelay)
					continue
				}

				streamCmd := fmt.Sprintf("STREAM topic=%s partition=%d group=%s gen=%d member=%s",
					c.config.Topic, pid, c.config.GroupID, c.generation, c.memberID)
				if err := util.WriteWithLength(conn, util.EncodeMessage("", streamCmd)); err != nil {
					log.Printf("Partition [%d] STREAM command send failed: %v", pid, err)
					pc.mu.Lock()
					pc.conn.Close()
					pc.conn = nil
					pc.mu.Unlock()
					time.Sleep(retryDelay)
					continue
				}

				msgs := make([]types.Message, 0, maxBatch)
				perReadTimeout := 100 * time.Millisecond
				deadline := time.Now().Add(perReadTimeout)

				for i := 0; i < maxBatch; i++ {
					conn.SetReadDeadline(deadline)

					// [8-byte offset][length-prefixed message]
					offsetBytes := make([]byte, 8)
					_, err := io.ReadFull(conn, offsetBytes)
					if err != nil {
						if ne, ok := err.(net.Error); ok && ne.Timeout() {
							break
						}
						log.Printf("Partition [%d] read stream offset error: %v", pid, err)
						pc.mu.Lock()
						pc.conn.Close()
						pc.conn = nil
						pc.mu.Unlock()
						break
					}

					msgBytes, err := util.ReadWithLength(conn)
					if err != nil {
						if ne, ok := err.(net.Error); ok && ne.Timeout() {
							break
						}
						if len(msgBytes) == 0 {
							break
						}
						log.Printf("Partition [%d] read streamed message error: %v", pid, err)
						pc.mu.Lock()
						pc.conn.Close()
						pc.conn = nil
						pc.mu.Unlock()
						break
					}

					offset := binary.BigEndian.Uint64(offsetBytes)
					msgs = append(msgs, types.Message{
						Offset:  offset,
						Payload: string(msgBytes),
					})

					deadline = time.Now().Add(perReadTimeout)
				}

				if len(msgs) > 0 {
					if err := pc.consumer.processBatchSync(msgs, pc.partitionID); err != nil {
						log.Printf("Partition [%d] batch processing error: %v", pid, err)
					}

					pc.mu.Lock()
					if len(msgs) > 0 {
						pc.offset = msgs[len(msgs)-1].Offset + 1
					}
					newOffset := pc.offset - 1
					pc.mu.Unlock()

					select {
					case c.commitCh <- commitEntry{
						partition: pid,
						offset:    newOffset,
					}:
					default:
						go func() {
							if atomic.LoadInt32(&c.rebalancing) == 1 {
								return
							}
							if err := c.directCommit(pid, newOffset); err != nil {
								log.Printf("Partition [%d] direct commit failed: %v", pid, err)
							}
						}()
					}
				}

				time.Sleep(50 * time.Millisecond)
			}
		}(pid, pc)
	}

	<-c.doneCh
	return nil
}

func (c *Consumer) commitAllOffsets() {
	c.mu.RLock()
	consumers := make([]*PartitionConsumer, 0, len(c.partitionConsumers))
	for _, pc := range c.partitionConsumers {
		consumers = append(consumers, pc)
	}
	c.mu.RUnlock()

	for _, pc := range consumers {
		pc.commitOffset()
	}
}

func (c *Consumer) processBatchSync(msgs []types.Message, partition int) error {
	var processedCountInBatch int64
	if c.batchHandler != nil {
		if idempotentHandler, ok := c.batchHandler.(*handler.MemoryIdempotentHandler); ok {
			_ = idempotentHandler.GetProcessedCount()

			if err := idempotentHandler.Handle(msgs); err != nil {
				return err
			}
			processedCountInBatch = idempotentHandler.GetProcessedCount()
		} else {
			if err := c.batchHandler.Handle(msgs); err != nil {
				return err
			}
			processedCountInBatch = int64(len(msgs))
		}
	}

	if c.metrics != nil {
		for range msgs {
			c.metrics.RecordMessage(partition)
		}

		if processedCountInBatch > 0 {
			c.metrics.RecordProcessed(processedCountInBatch)
		}

		if c.metrics.IsDone() {
			c.TriggerBenchmarkStop()
		}
	}

	return nil
}

func (c *Consumer) directCommit(partition int, offset uint64) error {
	conn, err := c.client.Connect(c.config.BrokerAddr)
	if err != nil {
		return fmt.Errorf("direct commit connect failed: %v", err)
	}
	defer conn.Close()

	commitCmd := fmt.Sprintf("COMMIT_OFFSET topic=%s partition=%d group=%s offset=%d generation=%d member=%s",
		c.config.Topic, partition, c.config.GroupID, offset, c.generation, c.memberID)

	if err := util.WriteWithLength(conn, util.EncodeMessage("", commitCmd)); err != nil {
		return fmt.Errorf("direct commit send failed: %v", err)
	}

	resp, err := util.ReadWithLength(conn)
	if err != nil {
		return fmt.Errorf("direct commit response failed: %v", err)
	}

	respStr := string(resp)
	if strings.Contains(respStr, "ERROR") {
		if strings.Contains(respStr, "GEN_MISMATCH") {
			go c.handleRebalanceSignal()
		}
		return fmt.Errorf("direct commit error: %s", respStr)
	} else {
		log.Printf("✅ COMMIT_SUCCESS [P%d, O%d]", partition, offset)
	}

	return nil
}

func (c *Consumer) Close() error {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true

	close(c.doneCh)
	c.sessionCancel()

	for _, pc := range c.partitionConsumers {
		pc.mu.Lock()
		pc.closed = true
		if pc.conn != nil {
			pc.conn.Close()
		}
		pc.mu.Unlock()
	}

	c.wg.Wait()
	return nil
}

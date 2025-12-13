package subscriber

import (
	"context"
	"fmt"
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
	"github.com/downfa11-org/go-broker/consumer/types"
	"github.com/downfa11-org/go-broker/util"
)

type Consumer struct {
	config             *config.ConsumerConfig
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

	hbConn net.Conn
	hbMu   sync.Mutex

	closed  bool
	closeMu sync.Mutex

	bmStartTime time.Time
	metrics     *bench.ConsumerMetrics
}

type commitEntry struct {
	partition int
	offset    uint64
}

func NewConsumer(cfg *config.ConsumerConfig) (*Consumer, error) {
	client := client.NewConsumerClient(cfg)
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

	util.Info("✅ Successfully joined topic '%s' for group '%s' with %d partitions: %v (generation=%d, member=%s)",
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
			c.hbMu.Lock()
			if c.hbConn == nil {
				var brokerAddr string
				if len(c.config.BrokerAddrs) > 0 {
					brokerAddr = c.config.BrokerAddrs[0]
				}
				c.hbConn, _ = c.client.Connect(brokerAddr)
			}
			conn := c.hbConn
			c.hbMu.Unlock()

			hb := fmt.Sprintf("HEARTBEAT topic=%s group=%s member=%s generation=%d",
				c.config.Topic, c.config.GroupID, c.memberID, c.generation)

			if err := util.WriteWithLength(conn, util.EncodeMessage("", hb)); err != nil {
				util.Error("heartbeat send failed: %v", err)
				c.hbMu.Lock()
				c.hbConn = nil
				c.hbMu.Unlock()
				continue
			}

			resp, err := util.ReadWithLength(conn)
			if err != nil {
				util.Error("heartbeat response failed: %v", err)
				c.hbMu.Lock()
				c.hbConn = nil
				c.hbMu.Unlock()
				continue
			}

			respStr := string(resp)
			if strings.Contains(respStr, "REBALANCE_REQUIRED") || strings.Contains(respStr, "GEN_MISMATCH") {
				util.Warn("heartbeat indicated rebalance/mismatch: %s", respStr)
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
		time.Sleep(1 * time.Second)

		gen, mid, assignments, err := c.joinGroup()
		if err != nil {
			util.Error("Rebalance join failed: %v", err)
			atomic.StoreInt32(&c.rebalancing, 0)
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
		atomic.StoreInt32(&c.rebalancing, 0)

		if c.config.Mode != config.ModePolling {
			c.startStreaming()
		} else {
			c.startConsuming()
		}
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
		util.Info("Starting consumer for partition %d", pid)
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
	if c.memberID != "" {
		c.mu.RLock()
		assignments = make([]int, 0, len(c.partitionConsumers))
		for pid := range c.partitionConsumers {
			assignments = append(assignments, pid)
		}
		c.mu.RUnlock()

		return c.generation, c.memberID, assignments, nil
	}

	var brokerAddr string
	if len(c.config.BrokerAddrs) > 0 {
		brokerAddr = c.config.BrokerAddrs[0]
	} else {
		return 0, "", nil, fmt.Errorf("no broker addresses configured")
	}

	conn, err := c.client.Connect(brokerAddr)
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

	util.Info("join-group: %s\n", respStr)
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
					util.Error("⚠️ Error parsing partition ID '%s': %v", p, err)
				}
			}
		}
	}

	return gen, mid, assigned, nil
}

func (c *Consumer) syncGroup(generation int64, memberID string) ([]int, error) {
	var brokerAddr string
	if len(c.config.BrokerAddrs) > 0 {
		brokerAddr = c.config.BrokerAddrs[0]
	} else {
		return nil, fmt.Errorf("no broker addresses configured")
	}

	conn, err := c.client.Connect(brokerAddr)
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

	util.Info("sync-group: %s\n", respStr)

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
	if c.config.EnableBenchmark {
		c.bmStartTime = time.Now()
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.heartbeatLoop()
	}()

	for _, pc := range c.partitionConsumers {
		c.wg.Add(1)
		go func(pc *PartitionConsumer) {
			defer c.wg.Done()
			pc.startStreamLoop()
		}(pc)
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
	if c.metrics != nil {
		for range msgs {
			c.metrics.RecordMessage(partition)
		}

		processedCount := int64(len(msgs))
		c.metrics.RecordProcessed(processedCount)
		util.Debug("recorded %d processed messages", processedCount)

		if c.metrics.IsDone() {
			util.Info("🎉 Benchmark completed successfully!")
			c.TriggerBenchmarkStop()
		}
	} else {
		util.Debug("⚠️ No metrics configured")
	}

	return nil
}

func (c *Consumer) directCommit(partition int, offset uint64) error {
	var brokerAddr string
	if len(c.config.BrokerAddrs) > 0 {
		brokerAddr = c.config.BrokerAddrs[0]
	} else {
		return fmt.Errorf("no broker addresses configured")
	}

	conn, err := c.client.Connect(brokerAddr)
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
		util.Debug("✅ COMMIT_SUCCESS [P%d, O%d]", partition, offset)
	}

	return nil
}

func (c *Consumer) isDistributedMode() bool {
	return len(c.config.BrokerAddrs) > 1
}

func (c *Consumer) Close() error {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true

	if c.memberID != "" {
		var brokerAddr string
		if len(c.config.BrokerAddrs) > 0 {
			brokerAddr = c.config.BrokerAddrs[0]
		} else {
			return fmt.Errorf("no broker addresses configured")
		}

		conn, err := c.client.Connect(brokerAddr)
		if err == nil {
			defer conn.Close()
			leaveCmd := fmt.Sprintf("LEAVE_GROUP topic=%s group=%s consumer=%s",
				c.config.Topic, c.config.GroupID, c.memberID)
			if err := util.WriteWithLength(conn, util.EncodeMessage("", leaveCmd)); err == nil {
				util.ReadWithLength(conn)
			}
		}
	}

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

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

	generation int64
	memberID   string

	commitConn     net.Conn
	commitCh       chan commitEntry
	commitMu       sync.Mutex
	commitRetryMap map[int]uint64

	currentOffsets map[int]uint64
	offsetsMu      sync.Mutex

	wg         sync.WaitGroup
	mainCtx    context.Context
	mainCancel context.CancelFunc

	rebalancing  int32
	rebalanceSig chan struct{}

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
	ctx, cancel := context.WithCancel(context.Background())

	c := &Consumer{
		config:             cfg,
		client:             client,
		partitionConsumers: make(map[int]*PartitionConsumer),
		offsets:            make(map[int]uint64),
		currentOffsets:     make(map[int]uint64),
		rebalanceSig:       make(chan struct{}, 1),
		doneCh:             make(chan struct{}),
		mainCtx:            ctx,
		mainCancel:         cancel,
	}

	if cfg.EnableBenchmark {
		c.metrics = bench.NewConsumerMetrics(int64(cfg.NumMessages))
	}

	c.commitCh = make(chan commitEntry, 1024)
	return c, nil
}

func (c *Consumer) getLeaderConn() (net.Conn, error) {
	conn, addr, err := c.client.ConnectWithFailover()
	if err != nil {
		return nil, err
	}

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}

	util.Debug("Connected to broker: %s", addr)
	return conn, nil
}

func (c *Consumer) validateCommitConn() bool {
	c.commitMu.Lock()
	defer c.commitMu.Unlock()
	if c.commitConn == nil {
		return false
	}

	c.commitConn.SetReadDeadline(time.Now().Add(1 * time.Millisecond))
	_, err := c.commitConn.Read(make([]byte, 0))
	if err != nil && !os.IsTimeout(err) {
		c.commitConn.Close()
		c.commitConn = nil
		return false
	}
	c.commitConn.SetReadDeadline(time.Time{})
	return true
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

	c.startCommitWorker()
	go c.rebalanceMonitorLoop()

	if c.config.Mode != config.ModePolling {
		c.startStreaming()
	} else {
		c.startConsuming()
		<-c.doneCh
	}
	return nil
}

func (c *Consumer) startCommitWorker() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(c.config.AutoCommitInterval)
		defer ticker.Stop()

		for {
			select {
			case entry := <-c.commitCh:
				c.offsetsMu.Lock()
				if entry.offset > c.currentOffsets[entry.partition] {
					c.currentOffsets[entry.partition] = entry.offset
				}
				c.offsetsMu.Unlock()

			case <-ticker.C:
				c.flushOffsets()
				c.processRetryQueue()

			case <-c.mainCtx.Done():
				return

			case <-c.doneCh:
				c.flushOffsets()
				return
			}
		}
	}()
}

func (c *Consumer) rebalanceMonitorLoop() {
	for {
		select {
		case <-c.doneCh:
			return
		case <-c.rebalanceSig:
			c.handleRebalanceSignal()
		}
	}
}

func (c *Consumer) flushOffsets() {
	c.offsetsMu.Lock()
	if len(c.currentOffsets) == 0 {
		c.offsetsMu.Unlock()
		return
	}

	toCommit := make(map[int]uint64, len(c.currentOffsets))
	for k, v := range c.currentOffsets {
		toCommit[k] = v
	}
	c.currentOffsets = make(map[int]uint64, len(toCommit))
	c.offsetsMu.Unlock()

	c.enqueueCommit(toCommit)
}

func (c *Consumer) processRetryQueue() {
	c.commitMu.Lock()
	if len(c.commitRetryMap) == 0 {
		c.commitMu.Unlock()
		return
	}

	toRetry := make(map[int]uint64)
	for p, o := range c.commitRetryMap {
		toRetry[p] = o
	}
	c.commitRetryMap = make(map[int]uint64)
	c.commitMu.Unlock()

	util.Debug("🔄 Retrying failed commits for %d partitions...", len(toRetry))

	if !c.sendBatchCommit(toRetry) {
		util.Error("❌ Retry batch commit failed. Re-queueing...")
		c.commitMu.Lock()
		for p, o := range toRetry {
			if current, ok := c.commitRetryMap[p]; !ok || o > current {
				c.commitRetryMap[p] = o
			}
		}
		c.commitMu.Unlock()
	}
}

func (c *Consumer) enqueueCommit(offsets map[int]uint64) {
	go func() {
		retries := 0
		baseBackoff := c.config.CommitRetryBackoff
		maxBackoff := 10 * time.Second

		for {
			if c.sendBatchCommit(offsets) {
				return
			}
			retries++
			if retries >= c.config.MaxCommitRetries {
				util.Error("Failed to commit offsets after %d retries, enqueueing for later", retries)
				c.commitMu.Lock()
				for p, o := range offsets {
					if current, ok := c.commitRetryMap[p]; !ok || o > current {
						c.commitRetryMap[p] = o
					}
				}
				c.commitMu.Unlock()
				return
			}
			sleepTime := baseBackoff * time.Duration(1<<(uint(retries)-1))

			if sleepTime > maxBackoff {
				sleepTime = maxBackoff
			}

			util.Debug("Commit failed (%d/%d)), retrying in %v...", retries, c.config.MaxCommitRetries, sleepTime)
			time.Sleep(sleepTime)
		}
	}()
}

func (c *Consumer) handleLeaderRedirection(resp string) {
	if strings.Contains(resp, "LEADER_IS") {
		// "ERROR NOT_LEADER LEADER_IS 192.168.0.10:9000"
		parts := strings.Fields(resp)
		for i, part := range parts {
			if part == "LEADER_IS" && i+1 < len(parts) {
				newLeader := parts[i+1]
				c.client.UpdateLeader(newLeader)
				util.Debug("update leader: %s", newLeader)
				break
			}
		}
	}
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
				conn, err := c.getLeaderConn()
				if err != nil {
					c.hbMu.Unlock()
					util.Error("heartbeat could not get connection: %v", err)
					continue
				}
				c.hbConn = conn
			}
			conn := c.hbConn
			conn.SetDeadline(time.Now().Add(5 * time.Second))
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
			conn.SetDeadline(time.Time{})
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
				select {
				case c.rebalanceSig <- struct{}{}:
				default:
				}
				return
			}
		}
	}
}

func (c *Consumer) resetHeartbeatConn() {
	c.hbMu.Lock()
	if c.hbConn != nil {
		c.hbConn.Close()
		c.hbConn = nil
	}
	c.hbMu.Unlock()
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

	c.mainCancel()
	c.wg.Wait()

	c.mainCtx, c.mainCancel = context.WithCancel(context.Background())

	go func() {
		defer atomic.StoreInt32(&c.rebalancing, 0)

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

	conn, err := c.getLeaderConn()
	if err != nil {
		return 0, "", nil, err
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
	conn, err := c.getLeaderConn()
	if err != nil {
		return nil, err
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

func (c *Consumer) startStreaming() {
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
}

func (c *Consumer) sendBatchCommit(offsets map[int]uint64) bool {
	c.commitMu.Lock()
	if c.commitConn == nil || !c.validateCommitConn() {
		conn, err := c.getLeaderConn()
		if err != nil {
			c.commitMu.Unlock()
			util.Error("Batch commit failed to get connection: %v", err)
			return false
		}

		if tcpConn, ok := conn.(*net.TCPConn); ok {
			tcpConn.SetKeepAlive(true)
			tcpConn.SetKeepAlivePeriod(30 * time.Second)
		}
		c.commitConn = conn
	}
	conn := c.commitConn
	c.commitMu.Unlock()

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("BATCH_COMMIT topic=%s group=%s generation=%d member=%s ", c.config.Topic, c.config.GroupID, c.generation, c.memberID))
	parts := []string{}
	for pid, off := range offsets {
		parts = append(parts, fmt.Sprintf("%d:%d", pid, off))
	}
	sb.WriteString(strings.Join(parts, ","))

	if err := util.WriteWithLength(conn, util.EncodeMessage("", sb.String())); err != nil {
		util.Error("Batch commit send failed: %v", err)
		c.commitMu.Lock()
		conn.Close()
		c.commitConn = nil
		c.commitMu.Unlock()
		return false
	}

	resp, err := util.ReadWithLength(conn)
	if err != nil {
		util.Error("Batch commit response read failed: %v", err)
		c.commitMu.Lock()
		conn.Close()
		c.commitConn = nil
		c.commitMu.Unlock()
		return false
	}

	respStr := string(resp)
	if strings.HasPrefix(respStr, "OK") {
		util.Debug("✅ Batch commit success: %s", respStr)
		return true
	}

	if strings.Contains(respStr, "ERROR") || strings.Contains(respStr, "STALE_METADATA") {
		util.Error("❌ Batch commit rejected: %s", respStr)

		if strings.Contains(respStr, "NOT_OWNER") ||
			strings.Contains(respStr, "GEN_MISMATCH") ||
			strings.Contains(respStr, "AUTHORIZED") {
			select {
			case c.rebalanceSig <- struct{}{}:
			default:
			}
		}
		return false
	}
	return false
}

func (c *Consumer) processBatchSync(msgs []types.Message, partition int) error {
	if c.metrics == nil {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for range msgs {
		if c.metrics.IsDone() {
			return nil
		}

		c.metrics.RecordMessage(partition)
		c.metrics.RecordProcessed(1)

		if c.metrics.IsDone() {
			util.Info("🎯 Exact target reached: %d. Terminating...", c.metrics.TargetMessages)
			c.TriggerBenchmarkStop()
			return nil
		}
	}

	return nil
}

func (c *Consumer) directCommit(partition int, offset uint64) error {
	conn, err := c.getLeaderConn()
	if err != nil {
		return err
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

func (c *Consumer) Close() error {
	c.closeMu.Lock()
	if c.closed {
		c.closeMu.Unlock()
		return nil
	}
	c.closed = true
	c.closeMu.Unlock()

	close(c.doneCh)
	c.mainCancel()

	c.resetHeartbeatConn()

	if c.memberID != "" {
		if conn, err := c.getLeaderConn(); err == nil {
			leaveCmd := fmt.Sprintf("LEAVE_GROUP topic=%s group=%s member=%s",
				c.config.Topic, c.config.GroupID, c.memberID)
			_ = util.WriteWithLength(conn, util.EncodeMessage("", leaveCmd))
			conn.Close()
		}
	}

	c.mu.Lock()
	for _, pc := range c.partitionConsumers {
		pc.mu.Lock()
		pc.closed = true
		if pc.conn != nil {
			pc.conn.Close()
		}
		pc.mu.Unlock()
	}
	c.mu.Unlock()

	c.wg.Wait()
	return nil
}

package producer

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/downfa11-org/go-broker/publisher/bench"
	"github.com/downfa11-org/go-broker/publisher/config"
	"github.com/downfa11-org/go-broker/publisher/types"
	"github.com/downfa11-org/go-broker/util"
)

type BatchState struct {
	BatchID     string
	StartSeqNum uint64
	EndSeqNum   uint64
	Partition   int
	SentTime    time.Time
	Acked       bool
}

type partitionBuffer struct {
	mu     sync.Mutex
	msgs   []types.Message
	cond   *sync.Cond
	closed bool
}

func newPartitionBuffer() *partitionBuffer {
	p := &partitionBuffer{
		msgs: make([]types.Message, 0),
	}
	p.cond = sync.NewCond(&p.mu)
	return p
}

type Publisher struct {
	config   *config.PublisherConfig
	producer *ProducerClient

	partitions int
	buffers    []*partitionBuffer

	sendersWG sync.WaitGroup

	rr       uint32
	inFlight []int32

	sentMu   sync.Mutex
	sentSeqs map[uint64]struct{}

	batchStates map[string]*BatchState
	batchMu     sync.Mutex
	gcTicker    *time.Ticker

	done    chan struct{}
	closed  int32
	closeMu sync.Mutex

	bmMu         sync.Mutex
	bmTotalTime  map[int]time.Duration
	bmTotalCount map[int]int
}

func NewPublisher(cfg *config.PublisherConfig) (*Publisher, error) {
	p := &Publisher{
		config:       cfg,
		producer:     NewProducerClient(cfg.Partitions),
		partitions:   cfg.Partitions,
		buffers:      make([]*partitionBuffer, cfg.Partitions),
		sentSeqs:     make(map[uint64]struct{}),
		done:         make(chan struct{}),
		bmTotalTime:  make(map[int]time.Duration),
		bmTotalCount: make(map[int]int),
		inFlight:     make([]int32, cfg.Partitions),
		batchStates:  make(map[string]*BatchState),
		gcTicker:     time.NewTicker(1 * time.Minute),
	}

	if err := p.CreateTopic(cfg.Topic, cfg.Partitions); err != nil {
		return nil, fmt.Errorf("failed to create topic '%s': %w", cfg.Topic, err)
	}

	connectedCount := 0
	for i := 0; i < cfg.Partitions; i++ {
		p.buffers[i] = newPartitionBuffer()
		if err := p.producer.ConnectPartition(i, cfg.BrokerAddr, cfg.UseTLS, cfg.TLSCertPath, cfg.TLSKeyPath); err != nil {
			util.Error("Failed to connect partition %d: %v", i, err)
		} else {
			connectedCount++
		}
		p.sendersWG.Add(1)
		go p.partitionSender(i)
	}
	if connectedCount == 0 {
		return nil, fmt.Errorf("failed to connect to any partition")
	}

	go p.batchStateGC()
	return p, nil
}

func (p *Publisher) nextPartition() int {
	idx := int((atomic.AddUint32(&p.rr, 1) - 1) % uint32(p.partitions))
	return idx
}

func (p *Publisher) CreateTopic(topic string, partitions int) error {
	conn, err := net.Dial("tcp", p.config.BrokerAddr)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()

	createCmd := fmt.Sprintf("CREATE topic=%s partitions=%d", topic, partitions)
	cmdBytes := util.EncodeMessage("admin", createCmd)

	if err := util.WriteWithLength(conn, cmdBytes); err != nil {
		return fmt.Errorf("send command: %w", err)
	}

	resp, err := util.ReadWithLength(conn)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if strings.Contains(string(resp), "ERROR:") {
		return fmt.Errorf("broker error: %s", string(resp))
	}

	return nil
}

func (p *Publisher) PublishMessage(message string) (uint64, error) {
	if atomic.LoadInt32(&p.closed) == 1 {
		return 0, fmt.Errorf("publisher closed")
	}

	part := p.nextPartition()
	buf := p.buffers[part]

	buf.mu.Lock()
	if len(buf.msgs) >= p.config.BufferSize {
		buf.mu.Unlock()
		return 0, fmt.Errorf("partition %d buffer full", part)
	}

	seqNum := p.producer.NextSeqNum(part)
	bm := types.Message{
		SeqNum:     seqNum,
		Payload:    message,
		ProducerID: p.producer.ID,
		Epoch:      p.producer.Epoch,
	}

	buf.msgs = append(buf.msgs, bm)
	buf.cond.Signal()
	buf.mu.Unlock()

	return seqNum, nil
}

func (p *Publisher) partitionSender(part int) {
	defer p.sendersWG.Done()

	buf := p.buffers[part]
	linger := time.Duration(p.config.LingerMS) * time.Millisecond

	for {
		buf.mu.Lock()

		for len(buf.msgs) == 0 && !buf.closed && atomic.LoadInt32(&p.closed) == 0 {
			buf.cond.Wait()
		}

		if buf.closed && len(buf.msgs) == 0 {
			buf.mu.Unlock()
			return
		}

		if len(buf.msgs) >= p.config.BatchSize {
			batch := p.extract(buf)
			buf.mu.Unlock()
			go p.sendBatch(part, batch)
			continue
		}

		buf.mu.Unlock()
		time.Sleep(linger)
		buf.mu.Lock()

		if len(buf.msgs) > 0 {
			util.Debug("Linger timeout. Sending partial batch from partition %d (size=%d)", part, len(buf.msgs))
			batch := p.extractAny(buf)
			buf.mu.Unlock()
			go p.sendBatch(part, batch)
		} else {
			buf.mu.Unlock()
		}
	}
}

func (p *Publisher) extract(buf *partitionBuffer) []types.Message {
	if len(buf.msgs) == 0 {
		return nil
	}

	n := p.config.BatchSize
	if len(buf.msgs) < n {
		n = len(buf.msgs)
	}

	batch := make([]types.Message, n)
	copy(batch, buf.msgs[:n])
	buf.msgs = buf.msgs[n:]

	return batch
}

func (p *Publisher) extractAny(buf *partitionBuffer) []types.Message {
	if len(buf.msgs) == 0 {
		util.Warn("extractAny: buffer empty")
		return nil
	}

	n := len(buf.msgs)
	batch := make([]types.Message, n)
	copy(batch, buf.msgs[:n])
	buf.msgs = buf.msgs[:0]

	return batch
}

func (p *Publisher) sendBatch(part int, batch []types.Message) {
	if len(batch) == 0 {
		return
	}

	atomic.AddInt32(&p.inFlight[part], 1)

	var batchStart, batchEnd uint64
	if len(batch) > 0 {
		batchStart = batch[0].SeqNum
		batchEnd = batch[len(batch)-1].SeqNum
	}

	shortID := p.producer.ID[:8]
	if len(shortID) > 8 {
		shortID = shortID[:8]
	}

	shortEpoch := p.producer.Epoch % 1000
	batchID := fmt.Sprintf("%s-%03d-%d-%d", shortID, shortEpoch, batchStart, batchEnd)
	util.Info("Sending batch %s: partition=%d, messages=%d, seqRange=%d-%d", batchID, part, len(batch), batchStart, batchEnd)

	p.batchMu.Lock()
	p.batchStates[batchID] = &BatchState{
		BatchID:     batchID,
		StartSeqNum: batchStart,
		EndSeqNum:   batchEnd,
		Partition:   part,
		SentTime:    time.Now(),
		Acked:       false,
	}
	p.batchMu.Unlock()

	data, err := types.EncodeBatchMessages(p.config.Topic, part, batch)
	if err != nil {
		util.Error("encode batch failed: %v", err)
		p.handleSendFailure(part, batch)
		return
	}

	payload, err := types.CompressMessage(data, p.config.EnableGzip)
	if err != nil {
		util.Error("compress batch failed: %v", err)
		p.handleSendFailure(part, batch)
		return
	}

	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(payload)))
	payload = append(lenBuf, payload...)

	if err := p.sendWithRetry(payload, batch, part, batchID); err != nil {
		util.Error("send failed: %v", err)
		atomic.AddInt32(&p.inFlight[part], -1)
		p.handleSendFailure(part, batch)
		return
	}

	p.sentMu.Lock()
	for _, m := range batch {
		p.sentSeqs[m.SeqNum] = struct{}{}
	}
	p.sentMu.Unlock()

	atomic.AddInt32(&p.inFlight[part], -1)
}

func (p *Publisher) handleSendFailure(part int, batch []types.Message) {
	if len(batch) == 0 {
		return
	}

	buf := p.buffers[part]
	buf.mu.Lock()
	defer buf.mu.Unlock()

	p.sentMu.Lock()
	var retryBatch []types.Message
	for _, msg := range batch {
		if _, exists := p.sentSeqs[msg.SeqNum]; !exists {
			msg.Retry = true
			retryBatch = append(retryBatch, msg)
		}
	}
	p.sentMu.Unlock()

	allMsgs := append(buf.msgs, retryBatch...)
	sort.Slice(allMsgs, func(i, j int) bool {
		if allMsgs[i].Retry && !allMsgs[j].Retry {
			return true
		}
		if !allMsgs[i].Retry && allMsgs[j].Retry {
			return false
		}
		return allMsgs[i].SeqNum < allMsgs[j].SeqNum
	})

	buf.msgs = allMsgs
	buf.cond.Signal()
}

func (p *Publisher) handlePartialFailure(part int, batch []types.Message, ackResp *types.AckResponse) {
	lastSuccessSeq := ackResp.SeqEnd

	buf := p.buffers[part]
	buf.mu.Lock()
	defer buf.mu.Unlock()

	var retryBatch []types.Message
	for _, msg := range batch {
		if msg.SeqNum > lastSuccessSeq {
			retryBatch = append(retryBatch, msg)
		}
	}

	if len(retryBatch) > 0 {
		allMsgs := append(buf.msgs, retryBatch...)
		sort.Slice(allMsgs, func(i, j int) bool {
			return allMsgs[i].SeqNum < allMsgs[j].SeqNum
		})
		buf.msgs = allMsgs
		buf.cond.Signal()
	}
}

func (p *Publisher) sendWithRetry(payload []byte, batch []types.Message, part int, batchID string) error {
	maxAttempts := p.config.MaxRetries + 1
	backoff := p.config.RetryBackoffMS

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		conn := p.producer.GetConn(part)
		if conn == nil {
			if err := p.producer.ReconnectPartition(part, p.config.BrokerAddr, p.config.UseTLS, p.config.TLSCertPath, p.config.TLSKeyPath); err != nil {
				lastErr = fmt.Errorf("reconnect failed: %w", err)
				time.Sleep(time.Duration(backoff) * time.Millisecond)
				backoff = min(backoff*2, p.config.MaxBackoffMS)
				continue
			}
			conn = p.producer.GetConn(part)
			if conn == nil {
				lastErr = fmt.Errorf("no connection after reconnect")
				time.Sleep(time.Duration(backoff) * time.Millisecond)
				backoff = min(backoff*2, p.config.MaxBackoffMS)
				continue
			}
		}

		_ = conn.SetWriteDeadline(time.Now().Add(time.Duration(p.config.WriteTimeoutMS) * time.Millisecond))
		if _, err := conn.Write(payload); err != nil {
			lastErr = fmt.Errorf("write failed: %w", err)
			_ = p.producer.ReconnectPartition(part, p.config.BrokerAddr, p.config.UseTLS, p.config.TLSCertPath, p.config.TLSKeyPath)
			time.Sleep(time.Duration(backoff) * time.Millisecond)
			backoff = min(backoff*2, p.config.MaxBackoffMS)
			continue
		}

		if p.config.Acks == "0" {
			return nil
		}

		_ = conn.SetReadDeadline(time.Now().Add(time.Duration(p.config.AckTimeoutMS) * time.Millisecond))
		resp, err := util.ReadWithLength(conn)
		_ = conn.SetReadDeadline(time.Time{})
		if err != nil {
			lastErr = fmt.Errorf("read ack failed: %w", err)
			time.Sleep(time.Duration(backoff) * time.Millisecond)
			backoff = min(backoff*2, p.config.MaxBackoffMS)
			continue
		}

		if p.config.EnableGzip {
			resp, err = types.DecompressMessage(resp)
			if err != nil {
				lastErr = fmt.Errorf("decompress ack failed: %w", err)
				continue
			}
		}

		ackResp, err := p.parseAckResponse(resp)
		if err != nil {
			lastErr = err
			continue
		}

		if ackResp.Status != "OK" {
			if ackResp.Status == "PARTIAL" {
				p.handlePartialFailure(part, batch, ackResp)
				return fmt.Errorf("partial failure handled")
			}
			lastErr = fmt.Errorf("broker error: %s", ackResp.ErrorMsg)
			continue
		}
		p.markBatchAckedByID(batchID)
		return nil
	}
	return lastErr
}

func (p *Publisher) markBatchAckedByID(batchID string) {
	p.batchMu.Lock()
	defer p.batchMu.Unlock()

	state, ok := p.batchStates[batchID]
	if !ok {
		util.Debug("Batch State %s not found. Already processed or GCed.", batchID)
		return
	}

	if state.Acked {
		delete(p.batchStates, batchID)
		return
	}

	state.Acked = true
	sentTime := state.SentTime

	p.sentMu.Lock()
	for seq := state.StartSeqNum; seq <= state.EndSeqNum; seq++ {
		p.sentSeqs[seq] = struct{}{}
	}
	p.sentMu.Unlock()

	part := state.Partition
	p.producer.CommitSeqRange(part, state.EndSeqNum)
	p.producer.SaveState()

	elapsed := time.Since(sentTime)
	p.bmMu.Lock()
	p.bmTotalCount[part] += 1
	p.bmTotalTime[part] += elapsed
	p.bmMu.Unlock()

	delete(p.batchStates, batchID)
}

func (p *Publisher) parseAckResponse(resp []byte) (*types.AckResponse, error) {
	var ackResp types.AckResponse
	if err := json.Unmarshal(resp, &ackResp); err != nil {
		return nil, fmt.Errorf("invalid ack format: %v, %w", string(resp), err)
	}

	if ackResp.ProducerID == "" {
		ackStr, _ := json.Marshal(ackResp)
		util.Warn("producerID mismatch: expected %d, got %d, ack=%s", p.producer.Epoch, ackResp.ProducerEpoch, ackStr)
		return nil, fmt.Errorf("incomplete ack response: missing required fields")
	}

	if ackResp.ProducerEpoch != p.producer.Epoch {
		ackStr, _ := json.Marshal(ackResp)
		util.Warn("epoch mismatch: expected %d, got %d, ack=%s", p.producer.Epoch, ackResp.ProducerEpoch, ackStr)
		return nil, fmt.Errorf("epoch mismatch: expected %d, got %d", p.producer.Epoch, ackResp.ProducerEpoch)
	}

	return &ackResp, nil
}

func (p *Publisher) VerifySentSequences(expectedCount int) error {
	p.sentMu.Lock()
	defer p.sentMu.Unlock()

	if len(p.sentSeqs) != expectedCount {
		return fmt.Errorf("expected %d messages sent, got %d", expectedCount, len(p.sentSeqs))
	}

	util.Info("All %d sequences sent successfully", expectedCount)
	return nil
}

func (p *Publisher) Flush() {
	for _, buf := range p.buffers {
		buf.mu.Lock()
		buf.cond.Broadcast()
		buf.mu.Unlock()
	}

	timeout := time.Duration(p.config.FlushTimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allClear := true
		for part := 0; part < p.partitions; part++ {
			inFlight := atomic.LoadInt32(&p.inFlight[part])
			if inFlight > 0 {
				allClear = false
				break
			}
		}

		if allClear {
			util.Info("Flush completed - all pending batches sent")
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	util.Warn("Flush timeout after %v", timeout)
}

func (p *Publisher) FlushBenchmark(expectedTotal int) {
	for _, buf := range p.buffers {
		buf.mu.Lock()
		buf.cond.Broadcast()
		buf.mu.Unlock()
	}

	timeout := time.Duration(p.config.FlushTimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		allInFlightClear := true
		for part := 0; part < p.partitions; part++ {
			if atomic.LoadInt32(&p.inFlight[part]) > 0 {
				allInFlightClear = false
				break
			}
		}

		if allInFlightClear {
			sentCount := p.GetSentMessageCount()
			if sentCount >= expectedTotal {
				p.batchMu.Lock()
				isBatchStatesEmpty := len(p.batchStates) == 0
				p.batchMu.Unlock()

				if isBatchStatesEmpty {
					util.Info("Flush completed - all expected messages (%d) have been acknowledged. (Elapsed: %.3fs)", expectedTotal, time.Since(deadline.Add(-timeout)).Seconds())
					return
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	util.Warn("Flush timeout after %v. Only %d/%d messages acknowledged.", timeout, p.GetSentMessageCount(), expectedTotal)
}

func (p *Publisher) batchStateGC() {
	for range p.gcTicker.C {
		cutoff := time.Now().Add(-1 * time.Minute)
		p.batchMu.Lock()
		for id, st := range p.batchStates {
			if st.Acked && st.SentTime.Before(cutoff) {
				delete(p.batchStates, id)
			}
		}
		p.batchMu.Unlock()
	}
}

func (p *Publisher) Close() {
	p.closeMu.Lock()
	if atomic.LoadInt32(&p.closed) == 1 {
		p.closeMu.Unlock()
		return
	}
	atomic.StoreInt32(&p.closed, 1)
	close(p.done)

	for _, buf := range p.buffers {
		buf.mu.Lock()
		buf.closed = true
		buf.cond.Broadcast()
		buf.mu.Unlock()
	}
	p.closeMu.Unlock()

	p.gcTicker.Stop()
	p.sendersWG.Wait()
	p.producer.Close()
}

// GetPartitionStats returns benchmark statistics for all partitions
func (p *Publisher) GetPartitionStats() []bench.PartitionStat {
	p.bmMu.Lock()
	defer p.bmMu.Unlock()

	stats := make([]bench.PartitionStat, 0, p.partitions)
	for part := 0; part < p.partitions; part++ {
		count := p.bmTotalCount[part]
		totalTime := p.bmTotalTime[part]
		var avg time.Duration
		if count > 0 {
			avg = totalTime / time.Duration(count)
		}
		stats = append(stats, bench.PartitionStat{
			PartitionID: part,
			BatchCount:  count,
			AvgDuration: avg,
		})
	}
	return stats
}

// GetSentMessageCount returns the number of successfully sent messages
func (p *Publisher) GetSentMessageCount() int {
	p.sentMu.Lock()
	defer p.sentMu.Unlock()
	return len(p.sentSeqs)
}

// GetPartitionCount returns the number of partitions
func (p *Publisher) GetPartitionCount() int {
	return p.partitions
}

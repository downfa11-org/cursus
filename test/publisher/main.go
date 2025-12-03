package main

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"gopkg.in/yaml.v3"

	"github.com/downfa11-org/go-broker/pkg/types"
	"github.com/downfa11-org/go-broker/util"
)

type PublisherConfig struct {
	BrokerAddr     string `yaml:"broker_addr" json:"broker_addr"`
	MaxRetries     int    `yaml:"max_retries" json:"max_retries"`
	RetryBackoffMS int    `yaml:"retry_backoff_ms" json:"retry_backoff_ms"`
	AckTimeoutMS   int    `yaml:"ack_timeout_ms" json:"ack_timeout_ms"`
	Topic          string `yaml:"topic" json:"topic"`
	Partitions     int    `yaml:"partitions" json:"partitions"`

	PublishDelayMS      int `yaml:"publish_delay_ms" json:"publish_delay_ms"`
	MaxInflightRequests int `yaml:"max_inflight_requests" json:"max_inflight_requests"`
	MaxBackoffMS        int `yaml:"max_backoff_ms" json:"max_backoff_ms"`
	WriteTimeoutMS      int `yaml:"write_timeout_ms" json:"write_timeout_ms"`

	Acks              string `yaml:"acks" json:"acks"`
	EnableIdempotence bool   `yaml:"enable_idempotence" json:"enable_idempotence"`
	BatchSize         int    `yaml:"batch_size" json:"batch_size"`
	BufferSize        int    `yaml:"buffer_size" json:"buffer_size"`
	LingerMS          int    `yaml:"linger_ms" json:"linger_ms"`

	UseTLS      bool   `yaml:"use_tls" json:"use_tls"`
	TLSCertPath string `yaml:"tls_cert_path" json:"tls_cert_path"`
	TLSKeyPath  string `yaml:"tls_key_path" json:"tls_key_path"`

	EnableGzip bool `yaml:"enable_gzip" json:"enable_gzip"`

	EnableBenchmark bool   `yaml:"enable_benchmark" json:"enable_benchmark"`
	BenchTopicName  string `yaml:"bench_topic_name" json:"bench_topic_name"`
	MessageSize     int    `yaml:"benchmark_message_size" json:"benchmark_message_size"`
	NumMessages     int    `yaml:"num_messages" json:"num_messages"`

	FlushWaitMS int `yaml:"flush_wait_ms" json:"flush_wait_ms"`
}

func LoadPublisherConfig() (*PublisherConfig, error) {
	cfg := &PublisherConfig{}

	flag.StringVar(&cfg.BrokerAddr, "broker", "localhost:9000", "Broker address")
	flag.IntVar(&cfg.MaxRetries, "max-retries", 3, "Maximum retry attempts")
	flag.IntVar(&cfg.RetryBackoffMS, "retry-backoff-ms", 100, "Initial backoff time in milliseconds")
	flag.IntVar(&cfg.AckTimeoutMS, "ack-timeout-ms", 5000, "ACK timeout in milliseconds")
	flag.StringVar(&cfg.Topic, "topic", "my-topic", "Topic name")
	flag.IntVar(&cfg.Partitions, "partitions", 4, "Number of partitions")
	flag.IntVar(&cfg.NumMessages, "num-messages", 10, "Number of messages to publish")

	flag.IntVar(&cfg.PublishDelayMS, "publish-delay-ms", 100, "Delay between messages in milliseconds")
	flag.IntVar(&cfg.MaxInflightRequests, "max-inflight", 5, "Max inflight requests")
	flag.IntVar(&cfg.BatchSize, "batch-size", 100, "Batch size")
	flag.IntVar(&cfg.BufferSize, "buffer-size", 1024, "Buffer size")
	flag.IntVar(&cfg.LingerMS, "linger-ms", 50, "Linger time in milliseconds")
	flag.IntVar(&cfg.MaxBackoffMS, "max-backoff-ms", 2000, "Maximum backoff time in ms")
	flag.IntVar(&cfg.WriteTimeoutMS, "write-timeout-ms", 5000, "Write timeout in ms")
	flag.IntVar(&cfg.FlushWaitMS, "flush-wait-ms", 500, "Wait after flush in ms")

	flag.BoolVar(&cfg.UseTLS, "use-tls", false, "Enable TLS")
	flag.StringVar(&cfg.TLSCertPath, "tls-cert", "", "TLS cert path")
	flag.StringVar(&cfg.TLSKeyPath, "tls-key", "", "TLS key path")

	flag.BoolVar(&cfg.EnableGzip, "enable-gzip", false, "Enable gzip")

	benchmarkFlag := flag.Bool("benchmark", false, "Enable benchmark mode with detailed metrics")
	flag.StringVar(&cfg.BenchTopicName, "bench-topic-name", "bench-topic", "Topic used in benchmark mode")
	flag.IntVar(&cfg.MessageSize, "benchmark_message_size", 100, "Message size in bytes (benchmark mode only)")

	configPath := flag.String("config", "/config.yaml", "Path to YAML/JSON config file")
	flag.Parse()

	if *configPath != "" {
		data, err := os.ReadFile(*configPath)
		if err != nil {
			if os.IsNotExist(err) {
				log.Printf("Config file %s not found, using flag defaults", *configPath)
				return cfg, nil
			}
			return nil, fmt.Errorf("failed to read config file %s: %w", *configPath, err)
		} else {
			if strings.HasSuffix(*configPath, ".json") {
				if err := json.Unmarshal(data, cfg); err != nil {
					log.Printf("[WARN] Failed to parse JSON config: %v", err)
				}
			} else {
				if err := yaml.Unmarshal(data, cfg); err != nil {
					log.Printf("[WARN] Failed to parse YAML config: %v", err)
				}
			}
		}
	}

	if cfg.MaxRetries < 0 {
		cfg.MaxRetries = 0
	}
	if cfg.RetryBackoffMS <= 0 {
		cfg.RetryBackoffMS = 100
	}
	if cfg.AckTimeoutMS <= 0 {
		cfg.AckTimeoutMS = 5000
	}
	if cfg.MaxInflightRequests <= 0 {
		cfg.MaxInflightRequests = 5
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 1024
	}
	if cfg.LingerMS <= 0 {
		cfg.LingerMS = 50
	}
	if cfg.Partitions <= 0 {
		cfg.Partitions = 1
	}
	if cfg.NumMessages < 0 {
		cfg.NumMessages = 0
	}
	if cfg.PublishDelayMS < 0 {
		cfg.PublishDelayMS = 0
	}
	if cfg.MaxBackoffMS <= 0 {
		cfg.MaxBackoffMS = 2000
	}
	if cfg.WriteTimeoutMS <= 0 {
		cfg.WriteTimeoutMS = 5000
	}
	if cfg.FlushWaitMS < 0 {
		cfg.FlushWaitMS = 0
	}
	if cfg.Acks != "0" && cfg.Acks != "1" && cfg.Acks != "all" {
		cfg.Acks = "1"
	}
	if cfg.Acks == "all" {
		cfg.Acks = "-1"
	}
	if cfg.EnableIdempotence && cfg.Acks == "0" {
		log.Printf("[WARN] Idempotence requires acks >= 1, setting acks=1")
		cfg.Acks = "1"
	}
	if strings.TrimSpace(cfg.Topic) == "" {
		cfg.Topic = "default-topic"
	}

	if cfg.UseTLS && (cfg.TLSCertPath == "" || cfg.TLSKeyPath == "") {
		return nil, fmt.Errorf("TLS enabled but cert/key paths not provided")
	}

	if *benchmarkFlag {
		cfg.EnableBenchmark = true
	}

	if cfg.EnableBenchmark {
		if cfg.Topic == "my-topic" {
			cfg.Topic = cfg.BenchTopicName
		}
		if cfg.MessageSize <= 0 {
			cfg.MessageSize = 1024
		}
	}

	return cfg, nil
}

type ProducerClient struct {
	ID     string
	seqNum atomic.Uint64
	epoch  int64
	mu     sync.Mutex
	conns  []net.Conn
}

func NewProducerClient() *ProducerClient {
	return &ProducerClient{
		ID:    uuid.New().String(),
		epoch: time.Now().UnixNano(),
	}
}

func (pc *ProducerClient) NextSeqNum() uint64 {
	return pc.seqNum.Add(1)
}

func (pc *ProducerClient) ConnectPartition(idx int, addr string, useTLS bool, certPath, keyPath string) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	return pc.connectPartitionLocked(idx, addr, useTLS, certPath, keyPath)
}

func (pc *ProducerClient) connectPartitionLocked(idx int, addr string, useTLS bool, certPath, keyPath string) error {
	var conn net.Conn
	var err error

	if useTLS {
		var cert tls.Certificate
		cert, err = tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return fmt.Errorf("load TLS cert: %w", err)
		}
		conn, err = tls.Dial("tcp", addr, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
		if err != nil {
			return fmt.Errorf("TLS dial to %s failed: %w", addr, err)
		}
	} else {
		conn, err = net.Dial("tcp", addr)
		if err != nil {
			return fmt.Errorf("TCP dial to %s failed: %w", addr, err)
		}
	}

	if len(pc.conns) <= idx {
		tmp := make([]net.Conn, idx+1)
		copy(tmp, pc.conns)
		pc.conns = tmp
	}
	pc.conns[idx] = conn
	return nil
}

func (pc *ProducerClient) ReconnectPartition(idx int, addr string, useTLS bool, certPath, keyPath string) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if idx < len(pc.conns) && pc.conns[idx] != nil {
		_ = pc.conns[idx].Close()
		pc.conns[idx] = nil
	}

	return pc.connectPartitionLocked(idx, addr, useTLS, certPath, keyPath)
}

func (pc *ProducerClient) GetConn(part int) net.Conn {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if part < len(pc.conns) {
		return pc.conns[part]
	}
	return nil
}

func (pc *ProducerClient) Close() error {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	for i, c := range pc.conns {
		if c != nil {
			_ = c.Close()
			pc.conns[i] = nil
		}
	}
	return nil
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
	config   *PublisherConfig
	producer *ProducerClient

	partitions int
	buffers    []*partitionBuffer

	sendersWG sync.WaitGroup

	rr       uint32
	inFlight []int32

	sentMu   sync.Mutex
	sentSeqs map[uint64]struct{}

	done    chan struct{}
	closed  int32
	closeMu sync.Mutex

	bmMu         sync.Mutex
	bmTotalTime  map[int]time.Duration
	bmTotalCount map[int]int
}

func NewPublisher(cfg *PublisherConfig) *Publisher {
	p := &Publisher{
		config:       cfg,
		producer:     NewProducerClient(),
		partitions:   cfg.Partitions,
		buffers:      make([]*partitionBuffer, cfg.Partitions),
		sentSeqs:     make(map[uint64]struct{}),
		done:         make(chan struct{}),
		bmTotalTime:  make(map[int]time.Duration),
		bmTotalCount: make(map[int]int),
		inFlight:     make([]int32, cfg.Partitions),
	}

	for i := 0; i < cfg.Partitions; i++ {
		p.buffers[i] = newPartitionBuffer()
		if err := p.producer.ConnectPartition(i, cfg.BrokerAddr, cfg.UseTLS, cfg.TLSCertPath, cfg.TLSKeyPath); err != nil {
			log.Printf("Failed to connect partition %d: %v", i, err)
		}
		p.sendersWG.Add(1)
		go p.partitionSender(i)
	}

	return p
}

func (p *Publisher) nextPartition() int {
	idx := int((atomic.AddUint32(&p.rr, 1) - 1) % uint32(p.partitions))
	return idx
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
	seqNum := p.producer.NextSeqNum()
	bm := types.Message{
		SeqNum:  seqNum,
		Payload: message,
	}

	buf.msgs = append(buf.msgs, bm)
	buf.mu.Unlock()
	buf.cond.Signal()

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

		batch := p.extract(buf, false)
		buf.mu.Unlock()

		if len(batch) == 0 {
			continue
		}

		p.sendBatch(part, batch)

		select {
		case <-time.After(linger):
			continue
		case <-p.done:
			return
		}
	}
}

func (p *Publisher) extract(buf *partitionBuffer, lock bool) []types.Message {
	if lock {
		buf.mu.Lock()
		defer buf.mu.Unlock()
	}

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

func (p *Publisher) sendBatch(part int, batch []types.Message) {
	if len(batch) == 0 {
		return
	}

	atomic.AddInt32(&p.inFlight[part], 1)
	defer atomic.AddInt32(&p.inFlight[part], -1)

	start := time.Now()

	data, err := EncodeBatchMessages(p.config.Topic, part, batch, p.config.Acks)
	if err != nil {
		log.Printf("[ERROR] encode batch failed: %v", err)
		p.handleSendFailure(part, batch)
		return
	}

	payload, err := CompressMessage(data, p.config.EnableGzip)
	if err != nil {
		log.Printf("[ERROR] compress batch failed: %v", err)
		p.handleSendFailure(part, batch)
		return
	}

	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(payload)))
	payload = append(lenBuf, payload...)

	if err := p.sendWithRetry(payload, batch, part); err != nil {
		log.Printf("[ERROR] send failed: %v", err)
		p.handleSendFailure(part, batch)
		return
	}

	p.sentMu.Lock()
	for _, m := range batch {
		p.sentSeqs[m.SeqNum] = struct{}{}
	}
	p.sentMu.Unlock()

	elapsed := time.Since(start)
	p.bmMu.Lock()
	p.bmTotalCount[part] += 1
	p.bmTotalTime[part] += elapsed
	p.bmMu.Unlock()
}

// todo. need to migrated util/serialize.go EncodeBathMessage
func EncodeBatchMessages(topic string, partition int, msgs []types.Message, acks string) ([]byte, error) {
	var buf bytes.Buffer
	buf.Write([]byte{0xBA, 0x7C})

	write := func(v any) error {
		if err := binary.Write(&buf, binary.BigEndian, v); err != nil {
			return fmt.Errorf("encode value failed: %w", err)
		}
		return nil
	}

	// topic
	topicBytes := []byte(topic)
	if err := write(uint16(len(topicBytes))); err != nil {
		return nil, err
	}
	if _, err := buf.Write(topicBytes); err != nil {
		return nil, fmt.Errorf("write topic bytes failed: %w", err)
	}

	// partition
	if err := write(int32(partition)); err != nil {
		return nil, err
	}

	// acks
	acksBytes := []byte(acks)
	if len(acksBytes) > 255 {
		acksBytes = acksBytes[:255]
	}
	if err := write(uint8(len(acksBytes))); err != nil {
		return nil, err
	}
	if _, err := buf.Write(acksBytes); err != nil {
		return nil, fmt.Errorf("write acks failed: %w", err)
	}

	// batch start/end seqNum
	var batchStart, batchEnd uint64
	if len(msgs) > 0 {
		batchStart = msgs[0].SeqNum
		batchEnd = msgs[len(msgs)-1].SeqNum
	}
	if err := write(batchStart); err != nil {
		return nil, err
	}
	if err := write(batchEnd); err != nil {
		return nil, err
	}

	if err := write(int32(len(msgs))); err != nil {
		return nil, err
	}

	for _, m := range msgs {
		if err := write(m.SeqNum); err != nil {
			return nil, err
		}
		payloadBytes := []byte(m.Payload)
		if err := write(uint32(len(payloadBytes))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(payloadBytes); err != nil {
			return nil, fmt.Errorf("write payload bytes failed: %w", err)
		}
	}

	return buf.Bytes(), nil
}

func (p *Publisher) handleSendFailure(part int, batch []types.Message) {
	if len(batch) == 0 {
		return
	}

	buf := p.buffers[part]
	buf.mu.Lock()
	defer buf.mu.Unlock()

	buf.msgs = append(buf.msgs, batch...)
	buf.cond.Signal()
}

func (p *Publisher) sendWithRetry(payload []byte, _ []types.Message, part int) error {
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
			resp, err = p.decompressMessage(resp)
			if err != nil {
				lastErr = fmt.Errorf("decompress ack failed: %w", err)
				time.Sleep(time.Duration(backoff) * time.Millisecond)
				backoff = min(backoff*2, p.config.MaxBackoffMS)
				continue
			}
		}

		respStr := strings.TrimSpace(string(resp))
		if strings.HasPrefix(respStr, "ERROR:") {
			lastErr = fmt.Errorf("broker error: %s", respStr)
			return lastErr
		}

		return nil
	}
	return lastErr
}

func (p *Publisher) decompressMessage(data []byte) ([]byte, error) {
	if !p.config.EnableGzip {
		return data, nil
	}
	gr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer gr.Close()
	return io.ReadAll(gr)
}

func CompressMessage(data []byte, enableGzip bool) ([]byte, error) {
	if !enableGzip {
		return data, nil
	}

	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	if _, err := gw.Write(data); err != nil {
		return nil, err
	}
	if err := gw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (p *Publisher) VerifySentSequences(expectedCount int) error {
	p.sentMu.Lock()
	defer p.sentMu.Unlock()

	if len(p.sentSeqs) != expectedCount {
		return fmt.Errorf("expected %d messages sent, got %d", expectedCount, len(p.sentSeqs))
	}

	for i := uint64(1); i <= uint64(expectedCount); i++ {
		if _, ok := p.sentSeqs[i]; !ok {
			return fmt.Errorf("missing seqNum=%d", i)
		}
	}

	log.Printf("All %d sequences sent successfully", expectedCount)
	return nil
}

func (p *Publisher) Flush() {
	for _, buf := range p.buffers {
		buf.mu.Lock()
		buf.cond.Broadcast()
		buf.mu.Unlock()
	}

	for part := 0; part < p.partitions; part++ {
		buf := p.buffers[part]
		for {
			buf.mu.Lock()
			inFlight := atomic.LoadInt32(&p.inFlight[part])
			remaining := len(buf.msgs)
			buf.mu.Unlock()

			if remaining == 0 && inFlight == 0 {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
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

	p.sendersWG.Wait()
	p.producer.Close()
}

type PartitionStat struct {
	PartitionID int
	BatchCount  int
	AvgDuration time.Duration
}

func printBenchmarkSummaryFixed(
	partitionStats []PartitionStat,
	sentMessages int,
	totalDuration time.Duration,
) {
	totalBatches := 0
	for _, ps := range partitionStats {
		totalBatches += ps.BatchCount
	}

	seconds := totalDuration.Seconds()
	if seconds <= 0 {
		seconds = 0.001
	}
	batchesPerSec := float64(totalBatches) / seconds
	messagesPerSec := float64(sentMessages) / seconds

	fmt.Println("=== BENCHMARK SUMMARY ===")
	fmt.Printf("Partitions                 : %d\n", len(partitionStats))
	fmt.Printf("Total Batches              : %d\n", totalBatches)
	fmt.Printf("Total messages published   : %d\n", sentMessages)
	fmt.Printf("Publish elapsed Time       : %.3fs\n", totalDuration.Seconds())
	fmt.Printf("Publish Batch Throughput   : %.2f batches/s\n", batchesPerSec)
	fmt.Printf("Publish Message Throughput : %.2f msg/s\n", messagesPerSec)
	fmt.Println()

	fmt.Println("Partition Breakdown:")
	for _, ps := range partitionStats {
		fmt.Printf(
			"  #%d  batches=%d  avg_batch=%.3fms\n",
			ps.PartitionID,
			ps.BatchCount,
			float64(ps.AvgDuration.Microseconds())/1000.0,
		)
	}
	fmt.Println("========================================")
}

func main() {
	cfg, err := LoadPublisherConfig()
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		os.Exit(1)
	}

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		log.Printf("Failed to marshal config: %v", err)
	} else {
		log.Printf("Configuration:\n%s", string(data))
	}

	pub := NewPublisher(cfg)
	defer pub.Close()

	total := cfg.NumMessages
	msgs := make([]string, 0, total)
	for i := 0; i < total; i++ {
		var msg string
		if cfg.EnableBenchmark {
			msg = generateMessage(cfg.MessageSize, i)
		} else {
			msg = fmt.Sprintf("hello-%d", i)
		}
		msgs = append(msgs, msg)
	}

	start := time.Now()

	batchSize := cfg.BatchSize
	for i := 0; i < len(msgs); i += batchSize {
		end := i + batchSize
		if end > len(msgs) {
			end = len(msgs)
		}
		batch := msgs[i:end]

		for _, m := range batch {
			if _, err := pub.PublishMessage(m); err != nil {
				log.Printf("[ERROR] publish failed: %v", err)
			}
		}

		if cfg.PublishDelayMS > 0 {
			time.Sleep(time.Duration(cfg.PublishDelayMS) * time.Millisecond)
		}
	}

	log.Println("All messages queued, flushing...")

	pub.Flush()
	time.Sleep(time.Duration(cfg.FlushWaitMS) * time.Millisecond)

	duration := time.Since(start)

	partitionStats := make([]PartitionStat, 0, pub.partitions)
	totalBatches := 0
	pub.bmMu.Lock()
	for part := 0; part < pub.partitions; part++ {
		count := pub.bmTotalCount[part]
		totalTime := pub.bmTotalTime[part]
		var avg time.Duration
		if count > 0 {
			avg = totalTime / time.Duration(count)
		}
		partitionStats = append(partitionStats, PartitionStat{
			PartitionID: part,
			BatchCount:  count,
			AvgDuration: avg,
		})
		totalBatches += count
	}
	pub.bmMu.Unlock()

	if err := pub.VerifySentSequences(total); err != nil {
		log.Printf("verify: %v", err)
	}

	pub.sentMu.Lock()
	sentMessages := len(pub.sentSeqs)
	pub.sentMu.Unlock()
	printBenchmarkSummaryFixed(partitionStats, sentMessages, duration)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
}

func generateMessage(size int, seqNum int) string {
	if size <= 0 {
		return fmt.Sprintf("%s #%d", "Hello World!", seqNum)
	}

	header := fmt.Sprintf("msg-%d-", seqNum)
	paddingSize := size - len(header)
	if paddingSize < 0 {
		paddingSize = 0
	}

	padding := strings.Repeat("x", paddingSize)
	return header + padding
}

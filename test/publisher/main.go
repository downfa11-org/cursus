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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
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
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}

		if strings.HasSuffix(*configPath, ".json") {
			if err := json.Unmarshal(data, cfg); err != nil {
				return nil, fmt.Errorf("failed to parse JSON config: %w", err)
			}
		} else {
			if err := yaml.Unmarshal(data, cfg); err != nil {
				return nil, fmt.Errorf("failed to parse YAML config: %w", err)
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
	if cfg.Acks != "0" && cfg.Acks != "1" && cfg.Acks != "all" {
		cfg.Acks = "1"
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

	var conn net.Conn
	var err error

	if useTLS {
		cert, err := tls.LoadX509KeyPair(certPath, keyPath)
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

	return pc.ConnectPartition(idx, addr, useTLS, certPath, keyPath)
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

type BatchMessage struct {
	SeqNum  uint64
	Payload string
}

type partitionBuffer struct {
	mu     sync.Mutex
	msgs   []BatchMessage
	cond   *sync.Cond
	closed bool
}

func newPartitionBuffer() *partitionBuffer {
	p := &partitionBuffer{
		msgs: make([]BatchMessage, 0),
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

	rr uint32

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
	}

	for i := 0; i < cfg.Partitions; i++ {
		p.buffers[i] = newPartitionBuffer()
		if err := p.producer.ConnectPartition(i, cfg.BrokerAddr, cfg.UseTLS, cfg.TLSCertPath, cfg.TLSKeyPath); err != nil {
			log.Fatalf("Failed to connect partition %d: %v", i, err)
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

	seqNum := p.producer.NextSeqNum()
	part := p.nextPartition()
	buf := p.buffers[part]

	buf.mu.Lock()
	defer buf.mu.Unlock()

	if len(buf.msgs) >= p.config.BufferSize {
		return 0, fmt.Errorf("partition %d buffer full", part)
	}
	bm := BatchMessage{
		SeqNum:  seqNum,
		Payload: message,
	}

	buf.msgs = append(buf.msgs, bm)
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

		var batch []BatchMessage
		if len(buf.msgs) >= p.config.BatchSize {
			batch = buf.msgs[:p.config.BatchSize]
			buf.msgs = buf.msgs[p.config.BatchSize:]
			buf.mu.Unlock()
		} else {
			batch = append([]BatchMessage(nil), buf.msgs...)
			buf.mu.Unlock()

			timer := time.NewTimer(linger)
			select {
			case <-timer.C:
				buf.mu.Lock()
				if len(buf.msgs) > 0 {
					batch = append(batch, buf.msgs...)
					buf.msgs = buf.msgs[:0]
				}
				buf.mu.Unlock()
			case <-p.done:
				timer.Stop()
				return
			}
			timer.Stop()
		}

		if len(batch) == 0 {
			continue
		}

		start := time.Now()

		buf.mu.Lock()
		log.Printf("[DEBUG] Partition %d sending batch of %d messages. Current buffer size: %d", part, len(batch), len(buf.msgs))
		for _, m := range batch {
			log.Printf("[DEBUG] Partition %d message SeqNum=%d Payload=%s", part, m.SeqNum, m.Payload)
		}
		buf.mu.Unlock()

		data := EncodeBatchMessages(p.config.Topic, part, batch)
		payload, err := CompressMessage(data, p.config.EnableGzip)
		if err != nil {
			log.Printf("[ERROR] compress batch failed: %v", err)
			p.handleSendFailure(part, batch)
			continue
		}

		lenBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(lenBuf, uint32(len(payload)))
		payload = append(lenBuf, payload...)

		if err := p.sendWithRetry(payload, batch, part); err != nil {
			log.Printf("[ERROR] partition %d send failed: %v", part, err)
			p.handleSendFailure(part, batch)
		} else {
			p.sentMu.Lock()
			for _, m := range batch {
				p.sentSeqs[m.SeqNum] = struct{}{}
			}
			p.sentMu.Unlock()
		}

		elapsed := time.Since(start)
		p.bmMu.Lock()
		p.bmTotalCount[part] += 1
		p.bmTotalTime[part] += elapsed
		p.bmMu.Unlock()
	}
}

func (p *Publisher) handleSendFailure(part int, batch []BatchMessage) {
	buf := p.buffers[part]
	buf.mu.Lock()
	buf.msgs = append(buf.msgs, batch...)
	buf.mu.Unlock()
	buf.cond.Signal()
}

func (p *Publisher) sendWithRetry(payload []byte, _ []BatchMessage, part int) error {
	maxAttempts := p.config.MaxRetries + 1
	backoff := p.config.RetryBackoffMS
	const maxBackoff = 2000

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		conn := p.producer.GetConn(part)
		if conn == nil {
			if err := p.producer.ReconnectPartition(part, p.config.BrokerAddr, p.config.UseTLS, p.config.TLSCertPath, p.config.TLSKeyPath); err != nil {
				lastErr = fmt.Errorf("reconnect failed: %w", err)
				time.Sleep(time.Duration(backoff) * time.Millisecond)
				backoff = min(backoff*2, maxBackoff)
				continue
			}
			conn = p.producer.GetConn(part)
			if conn == nil {
				lastErr = fmt.Errorf("no connection after reconnect")
				time.Sleep(time.Duration(backoff) * time.Millisecond)
				backoff = min(backoff*2, maxBackoff)
				continue
			}
		}

		_ = conn.SetWriteDeadline(time.Now().Add(time.Duration(p.config.WriteTimeoutMS) * time.Millisecond))
		backoff = min(backoff*2, p.config.MaxBackoffMS)
		if _, err := conn.Write(payload); err != nil {
			lastErr = fmt.Errorf("write failed: %w", err)
			_ = p.producer.ReconnectPartition(part, p.config.BrokerAddr, p.config.UseTLS, p.config.TLSCertPath, p.config.TLSKeyPath)
			time.Sleep(time.Duration(backoff) * time.Millisecond)
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		if p.config.Acks == "0" {
			return nil
		}

		_ = conn.SetReadDeadline(time.Now().Add(time.Duration(p.config.AckTimeoutMS) * time.Millisecond))
		resp, err := p.readWithLength(conn)
		_ = conn.SetReadDeadline(time.Time{})
		if err != nil {
			lastErr = fmt.Errorf("read ack failed: %w", err)
			time.Sleep(time.Duration(backoff) * time.Millisecond)
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		if p.config.EnableGzip {
			resp, err = p.decompressMessage(resp)
			if err != nil {
				lastErr = fmt.Errorf("decompress ack failed: %w", err)
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

func (p *Publisher) readWithLength(conn net.Conn) ([]byte, error) {
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, lenBuf); err != nil {
		return nil, fmt.Errorf("read length: %w", err)
	}

	msgLen := binary.BigEndian.Uint32(lenBuf)
	msgBuf := make([]byte, msgLen)

	if _, err := io.ReadFull(conn, msgBuf); err != nil {
		return nil, fmt.Errorf("read message: %w", err)
	}
	// EOF, unexpected header -> ACK is not gzipped
	return msgBuf, nil
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

func EncodeBatchMessages(topic string, partition int, msgs []BatchMessage) []byte {
	var buf bytes.Buffer
	topicBytes := []byte(topic)
	binary.Write(&buf, binary.BigEndian, uint16(len(topicBytes)))
	buf.Write(topicBytes)
	binary.Write(&buf, binary.BigEndian, int32(partition))

	var batchStart uint64 = 0
	var batchEnd uint64 = 0
	if len(msgs) > 0 {
		batchStart = msgs[0].SeqNum
		batchEnd = msgs[len(msgs)-1].SeqNum
	}
	binary.Write(&buf, binary.BigEndian, batchStart)
	binary.Write(&buf, binary.BigEndian, batchEnd)
	binary.Write(&buf, binary.BigEndian, int32(len(msgs)))

	for _, m := range msgs {
		binary.Write(&buf, binary.BigEndian, m.SeqNum)
		payloadBytes := []byte(m.Payload)
		binary.Write(&buf, binary.BigEndian, uint32(len(payloadBytes)))
		buf.Write(payloadBytes)
	}

	return buf.Bytes()
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

	timeout := time.After(time.Duration(p.config.LingerMS*2) * time.Millisecond)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		allEmpty := true
		for _, buf := range p.buffers {
			buf.mu.Lock()
			if len(buf.msgs) > 0 {
				allEmpty = false
			}
			buf.mu.Unlock()
		}

		if allEmpty {
			break
		}

		select {
		case <-timeout:
			log.Println("[WARN] flush timeout, some messages may be unsent")
			return
		case <-ticker.C:
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

	batchesPerSec := float64(totalBatches) / totalDuration.Seconds()
	messagesPerSec := float64(sentMessages) / totalDuration.Seconds()

	fmt.Println("=== BENCHMARK SUMMARY ===")
	fmt.Printf("Partitions            : %d\n", len(partitionStats))
	fmt.Printf("Total Batches         : %d\n", totalBatches)
	fmt.Printf("Total Messages        : %d\n", sentMessages)
	fmt.Printf("Total Time            : %.3fs\n", totalDuration.Seconds())
	fmt.Printf("Batch Throughput      : %.2f batches/s\n", batchesPerSec)
	fmt.Printf("Message Throughput    : %.2f msg/s\n", messagesPerSec)
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
		total := pub.bmTotalTime[part]
		var avg time.Duration
		if count > 0 {
			avg = total / time.Duration(count)
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

	sentMessages := len(pub.sentSeqs)
	printBenchmarkSummaryFixed(partitionStats, sentMessages, duration)
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
	return header + padding[:paddingSize]
}

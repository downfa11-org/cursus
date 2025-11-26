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

// ProducerClient manages producer ID and sequence numbers for exactly-once semantics
type ProducerClient struct {
	ID     string
	seqNum atomic.Uint64
	epoch  int64
	mu     sync.Mutex
	conn   net.Conn
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

func (pc *ProducerClient) Connect(addr string, useTLS bool, certPath, keyPath string) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.conn != nil {
		return nil
	}

	var conn net.Conn
	var err error

	if useTLS {
		if certPath == "" || keyPath == "" {
			return fmt.Errorf("TLS enabled but cert/key paths not provided")
		}

		var cert tls.Certificate
		cert, err = tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return fmt.Errorf("load TLS cert: %w", err)
		}

		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}

		conn, err = tls.Dial("tcp", addr, tlsConfig)
	} else {
		conn, err = net.Dial("tcp", addr)
	}

	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}

	pc.conn = conn
	return nil
}

// Reconnect closes the current connection and establishes a new one
func (pc *ProducerClient) Reconnect(addr string, useTLS bool, certPath, keyPath string) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	// Close existing connection
	if pc.conn != nil {
		_ = pc.conn.Close()
		pc.conn = nil
	}

	// Establish new connection
	var conn net.Conn
	var err error

	if useTLS {
		if certPath == "" || keyPath == "" {
			return fmt.Errorf("TLS enabled but cert/key paths not provided")
		}

		cert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return fmt.Errorf("load TLS cert: %w", err)
		}

		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}

		conn, err = tls.Dial("tcp", addr, tlsConfig)
		if err != nil {
			log.Printf("Failed to establish TLS connection to %s: %v", addr, err)
		}
	} else {
		conn, err = net.Dial("tcp", addr)
	}

	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}

	pc.conn = conn
	return nil
}

// GetConn returns the current connection (thread-safe)
func (pc *ProducerClient) GetConn() net.Conn {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.conn
}

func (pc *ProducerClient) Close() error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.conn != nil {
		err := pc.conn.Close()
		pc.conn = nil
		return err
	}
	return nil
}

type PublisherConfig struct {
	BrokerAddr     string `yaml:"broker_addr" json:"broker_addr"`
	MaxRetries     int    `yaml:"max_retries" json:"max_retries"`
	RetryBackoffMS int    `yaml:"retry_backoff_ms" json:"retry_backoff_ms"`
	AckTimeoutMS   int    `yaml:"ack_timeout_ms" json:"ack_timeout_ms"`
	Topic          string `yaml:"topic" json:"topic"`
	Partitions     int    `yaml:"partitions" json:"partitions"`
	NumMessages    int    `yaml:"num_messages" json:"num_messages"`

	PublishDelayMS      int `yaml:"publish_delay_ms" json:"publish_delay_ms"`
	MaxInflightRequests int `yaml:"max_inflight_requests" json:"max_inflight_requests"`

	Acks              string `yaml:"acks" json:"acks"` // "0", "1", "all"
	EnableIdempotence bool   `yaml:"enable_idempotence" json:"enable_idempotence"`
	BatchSize         int    `yaml:"batch_size" json:"batch_size"`
	BufferSize        int    `yaml:"buffer_size" json:"buffer_size"`
	LingerMS          int    `yaml:"linger_ms" json:"linger_ms"`

	// TLS
	UseTLS      bool   `yaml:"use_tls" json:"use_tls"`
	TLSCertPath string `yaml:"tls_cert_path" json:"tls_cert_path"`
	TLSKeyPath  string `yaml:"tls_key_path" json:"tls_key_path"`

	// compression
	EnableGzip bool `yaml:"enable_gzip" json:"enable_gzip"`
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

	flag.BoolVar(&cfg.UseTLS, "use-tls", false, "Enable TLS")
	flag.StringVar(&cfg.TLSCertPath, "tls-cert", "", "TLS cert path")
	flag.StringVar(&cfg.TLSKeyPath, "tls-key", "", "TLS key path")

	flag.BoolVar(&cfg.EnableGzip, "enable-gzip", false, "Enable gzip")

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
		cfg.AckTimeoutMS = 1
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
		log.Printf("[WARN] TLS enabled but cert/key paths not provided, disabling TLS")
		cfg.UseTLS = false
	}

	return cfg, nil
}

type BatchMessage struct {
	SeqNum  uint64
	Payload string
}

type Publisher struct {
	config      *PublisherConfig
	producer    *ProducerClient
	inflightSem chan struct{}

	batchMu sync.Mutex
	batch   []BatchMessage
	batchCh chan BatchMessage
	done    chan struct{}

	sentMu   sync.Mutex
	sentSeqs map[uint64]time.Time
}

func NewPublisher(cfg *PublisherConfig) *Publisher {
	p := &Publisher{
		config:   cfg,
		producer: NewProducerClient(),

		inflightSem: make(chan struct{}, cfg.MaxInflightRequests),
		batch:       make([]BatchMessage, 0, cfg.BatchSize),
		batchCh:     make(chan BatchMessage, cfg.BufferSize),
		done:        make(chan struct{}),
		sentSeqs:    make(map[uint64]time.Time),
	}

	go p.batchLoop()
	return p
}

func (p *Publisher) batchLoop() {
	lingerDuration := time.Duration(p.config.LingerMS) * time.Millisecond
	ticker := time.NewTicker(lingerDuration)
	defer ticker.Stop()

	for {
		select {
		case msg := <-p.batchCh:
			p.batchMu.Lock()
			p.batch = append(p.batch, msg)

			// immediately send when BatchSize is reached
			if len(p.batch) >= p.config.BatchSize {
				p.flushBatch()
			}
			p.batchMu.Unlock()

		case <-ticker.C:
			// send batches periodically
			p.batchMu.Lock()
			if len(p.batch) > 0 {
				p.flushBatch()
			}
			p.batchMu.Unlock()

		case <-p.done:
			// send remaining batches on shutdown
			p.batchMu.Lock()
			if len(p.batch) > 0 {
				p.flushBatch()
			}
			p.batchMu.Unlock()
			return
		}
	}
}

func (p *Publisher) flushBatch() {
	if len(p.batch) == 0 {
		return
	}

	batchCopy := make([]BatchMessage, len(p.batch))
	copy(batchCopy, p.batch)
	p.batch = p.batch[:0]

	for _, batchMsg := range batchCopy {
		p.inflightSem <- struct{}{}

		go func(msg BatchMessage) {
			defer func() { <-p.inflightSem }()

			msgBytes := EncodeMessage(p.config.Topic, msg.Payload)
			if err := p.SendWithRetry(msgBytes, msg.SeqNum); err != nil {
				log.Printf("[ERROR] Failed to send seqNum=%d: %v", msg.SeqNum, err)
			} else {
				p.sentMu.Lock()
				p.sentSeqs[msg.SeqNum] = time.Now()
				p.sentMu.Unlock()
				log.Printf("[SUCCESS] Sent seqNum=%d", msg.SeqNum)
			}
		}(batchMsg)
	}
}

func (p *Publisher) WriteWithLength(conn net.Conn, data []byte) error {
	compressed, err := CompressMessage(data, p.config.EnableGzip)
	if err != nil {
		return fmt.Errorf("compress: %w", err)
	}

	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(compressed)))

	if _, err := conn.Write(lenBuf); err != nil {
		return fmt.Errorf("write length: %w", err)
	}

	if _, err := conn.Write(compressed); err != nil {
		return fmt.Errorf("write data: %w", err)
	}

	return nil
}

func (p *Publisher) ReadWithLength(conn net.Conn) ([]byte, error) {
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, lenBuf); err != nil {
		return nil, fmt.Errorf("read length: %w", err)
	}

	msgLen := binary.BigEndian.Uint32(lenBuf)
	msgBuf := make([]byte, msgLen)

	if _, err := io.ReadFull(conn, msgBuf); err != nil {
		return nil, fmt.Errorf("read message: %w", err)
	}

	return DecompressMessage(msgBuf, p.config.EnableGzip)
}

func EncodeMessage(topic, payload string) []byte {
	topicBytes := []byte(topic)
	payloadBytes := []byte(payload)
	data := make([]byte, 2+len(topicBytes)+len(payloadBytes))
	binary.BigEndian.PutUint16(data[:2], uint16(len(topicBytes)))
	copy(data[2:2+len(topicBytes)], topicBytes)
	copy(data[2+len(topicBytes):], payloadBytes)
	return data
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

func DecompressMessage(data []byte, enableGzip bool) ([]byte, error) {
	if !enableGzip {
		return data, nil
	}

	gr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer gr.Close()

	return io.ReadAll(gr)
}

func (p *Publisher) SendWithRetry(data []byte, seqNum uint64) error {
	ackTimeout := time.Duration(p.config.AckTimeoutMS) * time.Millisecond

	for attempt := 0; attempt <= p.config.MaxRetries; attempt++ {
		err := p.tryOnce(data, ackTimeout)
		if err == nil {
			return nil
		}

		if attempt == p.config.MaxRetries {
			return fmt.Errorf("max retries exceeded: %w", err)
		}

		backoff := time.Duration(1<<attempt) * time.Duration(p.config.RetryBackoffMS) * time.Millisecond
		fmt.Printf("Attempt %d/%d failed for seqNum=%d (%v), retrying in %v\n",
			attempt+1, p.config.MaxRetries+1, seqNum, err, backoff)

		time.Sleep(backoff)
	}

	return fmt.Errorf("unexpected retry loop exit")
}

func (p *Publisher) PublishMessage(message string) (uint64, error) {
	seqNum := p.producer.NextSeqNum()

	var payload string
	if p.config.EnableIdempotence {
		payload = fmt.Sprintf("PUBLISH topic=%s acks=%s producerId=%s seqNum=%d epoch=%d message=%s",
			p.config.Topic,
			p.config.Acks,
			p.producer.ID,
			seqNum,
			p.producer.epoch,
			message)
	} else {
		payload = fmt.Sprintf("PUBLISH topic=%s acks=%s message=%s",
			p.config.Topic,
			p.config.Acks,
			message)
	}

	batchMsg := BatchMessage{
		SeqNum:  seqNum,
		Payload: payload,
	}

	select {
	case p.batchCh <- batchMsg:
		return seqNum, nil
	case <-time.After(5 * time.Second):
		return 0, fmt.Errorf("batch channel full, timeout")
	}
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

	log.Printf("[VERIFY] All %d sequences sent successfully", expectedCount)
	return nil
}

func (p *Publisher) Flush() {
	p.batchMu.Lock()
	defer p.batchMu.Unlock()
	p.flushBatch()
}

func (p *Publisher) Close() {
	close(p.done)
	for i := 0; i < cap(p.inflightSem); i++ {
		p.inflightSem <- struct{}{}
	}
	p.producer.Close()
}

func (p *Publisher) CreateTopic() error {
	conn := p.producer.GetConn()
	if conn == nil {
		return fmt.Errorf("connection not established")
	}

	createCmd := EncodeMessage(p.config.Topic, fmt.Sprintf("CREATE topic=%s partitions=%d", p.config.Topic, p.config.Partitions))

	if err := p.WriteWithLength(conn, createCmd); err != nil {
		return fmt.Errorf("send create command: %w", err)
	}

	resp, err := p.ReadWithLength(conn)
	if err != nil {
		return fmt.Errorf("read create response: %w", err)
	}

	respMsg := strings.TrimSpace(string(resp))
	if strings.Contains(respMsg, "topic exists") || strings.Contains(respMsg, "already exists") {
		fmt.Printf("Topic '%s' already exists\n", p.config.Topic)
		return nil
	}

	if strings.HasPrefix(respMsg, "ERROR:") {
		return fmt.Errorf("topic creation failed: %s", respMsg)
	}

	fmt.Printf("Topic '%s' created with %d partitions\n", p.config.Topic, p.config.Partitions)
	return nil
}

func (p *Publisher) tryOnce(data []byte, ackTimeout time.Duration) error {
	conn := p.producer.GetConn()
	if conn == nil {
		return fmt.Errorf("connection not established")
	}

	log.Printf("Sending %d bytes to broker", len(data))

	if err := p.WriteWithLength(conn, data); err != nil {
		log.Printf("[WARN] Write failed: %v, attempting reconnect", err)

		if reconnectErr := p.producer.Reconnect(p.config.BrokerAddr, p.config.UseTLS, p.config.TLSCertPath, p.config.TLSKeyPath); reconnectErr != nil {
			return fmt.Errorf("reconnect failed: %w", reconnectErr)
		}

		log.Printf("[INFO] Reconnected successfully")
		return fmt.Errorf("write failed, reconnected: %w", err)
	}

	if p.config.Acks == "0" {
		return nil
	}

	if err := conn.SetReadDeadline(time.Now().Add(ackTimeout)); err != nil {
		return fmt.Errorf("set read deadline: %w", err)
	}

	resp, err := p.ReadWithLength(conn)
	if err != nil {
		return fmt.Errorf("read ack: %w", err)
	}

	_ = conn.SetReadDeadline(time.Time{})
	msg := strings.TrimSpace(string(resp))
	if strings.HasPrefix(msg, "ERROR:") {
		return fmt.Errorf("broker error: %s", msg)
	}

	return nil
}

func main() {
	cfg, err := LoadPublisherConfig()
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("📋 Configuration:\n")
	fmt.Printf("  Broker: %s\n", cfg.BrokerAddr)
	fmt.Printf("  Topic: %s (partitions: %d)\n", cfg.Topic, cfg.Partitions)
	fmt.Printf("  Max Retries: %d\n", cfg.MaxRetries)
	fmt.Printf("  Retry Backoff: %dms\n", cfg.RetryBackoffMS)
	fmt.Printf("  ACK Timeout: %dms\n\n", cfg.AckTimeoutMS)

	publisher := NewPublisher(cfg)
	if err := publisher.producer.Connect(cfg.BrokerAddr, cfg.UseTLS, cfg.TLSCertPath, cfg.TLSKeyPath); err != nil {
		log.Fatalf("Failed to connect to broker: %v", err)
	}
	defer publisher.producer.Close()

	if err := publisher.CreateTopic(); err != nil {
		fmt.Printf("Failed to create topic: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("\nProducer ID: %s\n", publisher.producer.ID)
	fmt.Printf("Epoch: %d\n\n", publisher.producer.epoch)

	fmt.Println("Publishing messages...")
	for i := 0; i < cfg.NumMessages; i++ {
		message := fmt.Sprintf("Hello from Go client! Message #%d", i)

		seqNum, err := publisher.PublishMessage(message)
		if err != nil {
			fmt.Printf("Failed to publish message %d: %v\n", i, err)
			continue
		}

		fmt.Printf("Message %d published successfully (seqNum=%d)\n", i, seqNum)
		time.Sleep(time.Duration(cfg.PublishDelayMS) * time.Millisecond)
	}

	publisher.Flush()
	time.Sleep(2 * time.Second)

	if err := publisher.VerifySentSequences(cfg.NumMessages); err != nil {
		fmt.Printf("Sequence verification failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\nAll messages published successfully!")
}

package main

import (
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
)

type ConsumerMode string

const (
	ModePolling   ConsumerMode = "polling"
	ModeStreaming ConsumerMode = "streaming"
)

type ConsumerConfig struct {
	BrokerAddr string `yaml:"broker_addr" json:"broker_addr"`
	Topic      string `yaml:"topic" json:"topic"`
	GroupID    string `yaml:"group_id" json:"group_id"`
	ConsumerID string `yaml:"consumer_id" json:"consumer_id"`

	EnableBenchmark bool         `yaml:"enable_benchmark" json:"enable_benchmark"`
	Mode            ConsumerMode `yaml:"mode" json:"mode"`

	PollInterval       time.Duration `yaml:"poll_interval" json:"poll_interval"`
	BatchSize          int           `yaml:"batch_size" json:"batch_size"`
	AutoCommitInterval time.Duration `yaml:"auto_commit_interval" json:"auto_commit_interval"`

	SessionTimeoutMS         int `yaml:"session_timeout_ms" json:"session_timeout_ms"`
	MaxPollRecords           int `yaml:"max_poll_records" json:"max_poll_records"`
	MaxConnectRetries        int `yaml:"max_connect_retries" json:"max_connect_retries"`
	ConnectRetryBackoffMS    int `yaml:"connect_retry_backoff_ms" json:"connect_retry_backoff_ms"`
	HeartbeatIntervalMS      int `yaml:"heartbeat_interval_ms" json:"heartbeat_interval_ms"`
	StreamingReadDeadlineMS  int `yaml:"streaming_read_deadline_ms" json:"streaming_read_deadline_ms"`
	StreamingRetryIntervalMS int `yaml:"streaming_retry_interval_ms" json:"streaming_retry_interval_ms"`

	EnableAutoCommit bool `yaml:"enable_auto_commit" json:"enable_auto_commit"`
	EnableGzip       bool `yaml:"enable_gzip" json:"enable_gzip"`
}

func LoadConfig() (*ConsumerConfig, error) {
	cfg := &ConsumerConfig{}
	flag.StringVar(&cfg.ConsumerID, "consumer-id", "consumer-1", "Consumer ID")
	flag.StringVar(&cfg.GroupID, "group-id", "default-group", "Consumer group ID")
	flag.StringVar(&cfg.Topic, "topic", "", "Topic to consume")
	flag.DurationVar(&cfg.PollInterval, "poll-interval", 1*time.Second, "Poll interval")
	flag.IntVar(&cfg.BatchSize, "batch-size", 100, "Batch size for consuming")
	flag.DurationVar(&cfg.AutoCommitInterval, "auto-commit-interval", 5*time.Second, "Auto commit interval")

	flag.IntVar(&cfg.MaxPollRecords, "max-poll-records", 500, "Max records per poll")
	flag.BoolVar(&cfg.EnableAutoCommit, "enable-auto-commit", true, "Enable auto commit")
	flag.IntVar(&cfg.SessionTimeoutMS, "session-timeout-ms", 30000, "Session timeout in milliseconds")
	flag.BoolVar(&cfg.EnableGzip, "enable-gzip", false, "Enable gzip compression")

	configPath := flag.String("config", "/config.yaml", "Path to YAML/JSON config file")
	flag.Parse()

	if *configPath != "" {
		data, err := os.ReadFile(*configPath)
		if err != nil {
			return nil, err
		}
		if strings.HasSuffix(*configPath, ".json") {
			if err := json.Unmarshal(data, cfg); err != nil {
				return nil, err
			}
		} else {
			if err := yaml.Unmarshal(data, cfg); err != nil {
				return nil, err
			}
		}
	}

	if len(cfg.BrokerAddr) == 0 {
		cfg.BrokerAddr = "localhost:9000"
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 1 * time.Second
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 100
	}
	if cfg.AutoCommitInterval == 0 {
		cfg.AutoCommitInterval = 5 * time.Second
	}
	if cfg.MaxPollRecords == 0 {
		cfg.MaxPollRecords = 500
	}
	if cfg.Mode == "" {
		cfg.Mode = ModePolling
	}
	if cfg.MaxConnectRetries == 0 {
		cfg.MaxConnectRetries = 5
	}
	if cfg.ConnectRetryBackoffMS == 0 {
		cfg.ConnectRetryBackoffMS = 1000
	}
	if cfg.HeartbeatIntervalMS == 0 {
		cfg.HeartbeatIntervalMS = cfg.SessionTimeoutMS / 2
	}
	if cfg.StreamingReadDeadlineMS == 0 {
		cfg.StreamingReadDeadlineMS = 5 * 60 * 1000 // 5min
	}
	if cfg.StreamingRetryIntervalMS == 0 {
		cfg.StreamingRetryIntervalMS = 50
	}

	return cfg, nil
}

type ConsumerClient struct {
	ID string
}

func NewConsumerClient() *ConsumerClient {
	return &ConsumerClient{ID: uuid.New().String()}
}

func (c *ConsumerClient) Connect(addr string) (net.Conn, error) {
	return net.Dial("tcp", addr)
}

type Message struct {
	Topic     string
	Partition int
	Offset    uint64
	Value     []byte
}

type PartitionConsumer struct {
	partitionID int
	consumer    *Consumer
	offset      uint64
	conn        net.Conn
	mu          sync.Mutex
	closed      bool
}

func (pc *PartitionConsumer) ensureConnection() error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.closed {
		return fmt.Errorf("partition consumer closed")
	}
	if pc.conn != nil {
		return nil
	}

	var err error
	for i := 0; i < pc.consumer.config.MaxConnectRetries; i++ {
		pc.conn, err = pc.consumer.client.Connect(pc.consumer.config.BrokerAddr)
		if err == nil {
			return nil
		}
		log.Printf("Partition %d: connect retry %d failed: %v", pc.partitionID, i+1, err)
		duration := time.Duration(pc.consumer.config.ConnectRetryBackoffMS) * time.Millisecond
		time.Sleep(duration)
	}
	return fmt.Errorf("failed to connect after retries: %w", err)
}

func (pc *PartitionConsumer) pollAndProcess() {
	if err := pc.ensureConnection(); err != nil {
		log.Printf("Partition %d: cannot poll: %v", pc.partitionID, err)
		return
	}

	pc.mu.Lock()
	currentOffset := pc.offset
	pc.mu.Unlock()

	consumeCmd := fmt.Sprintf("CONSUME topic=%s partition=%d offset=%d",
		pc.consumer.config.Topic, pc.partitionID, currentOffset)
	if err := sendBinaryMessage(pc.conn, encodeMessage(pc.consumer.config.Topic, consumeCmd)); err != nil {
		log.Printf("Partition %d: send command failed, reconnecting: %v", pc.partitionID, err)
		pc.closeConn()
		return
	}

	batchSize := pc.consumer.config.BatchSize
	if batchSize > pc.consumer.config.MaxPollRecords {
		batchSize = pc.consumer.config.MaxPollRecords
	}

	for i := 0; i < batchSize; i++ {
		duration := time.Duration(pc.consumer.config.StreamingReadDeadlineMS) * time.Millisecond
		pc.conn.SetReadDeadline(time.Now().Add(duration))
		msgBytes, err := readBinaryMessage(pc.conn)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				time.Sleep(pc.consumer.config.PollInterval)
				continue
			}
			if err == io.EOF {
				pc.closeConn()
				return
			}
			log.Printf("Partition %d: read message error: %v", pc.partitionID, err)
			pc.closeConn()
			break
		}
		if len(msgBytes) == 0 {
			break
		}

		pc.processMessage(msgBytes)
		pc.mu.Lock()
		pc.offset++
		pc.mu.Unlock()
	}
}

func (pc *PartitionConsumer) processMessage(msgBytes []byte) {
	if !pc.consumer.config.EnableBenchmark {
		log.Printf("Partition %d: %s", pc.partitionID, string(msgBytes))
	}

	if pc.consumer.config.EnableBenchmark {
		atomic.AddInt64(&pc.consumer.bmMsgCount, 1)
	}
}

func (pc *PartitionConsumer) commitOffset() {
	pc.mu.Lock()
	currentOffset := pc.offset
	pc.mu.Unlock()

	conn, err := pc.consumer.client.Connect(pc.consumer.config.BrokerAddr)
	if err != nil {
		log.Printf("Partition %d: commit connect failed: %v", pc.partitionID, err)
		return
	}
	defer conn.Close()

	commitCmd := fmt.Sprintf("COMMIT_OFFSET topic=%s partition=%d group=%s offset=%d",
		pc.consumer.config.Topic, pc.partitionID, pc.consumer.config.GroupID, currentOffset)
	if err := sendBinaryMessage(conn, encodeMessage("", commitCmd)); err != nil {
		log.Printf("Partition %d: commit send failed: %v", pc.partitionID, err)
		return
	}

	resp, err := readBinaryMessage(conn)
	if err != nil {
		log.Printf("Partition %d: commit response failed: %v", pc.partitionID, err)
		return
	}

	if strings.Contains(string(resp), "ERROR:") {
		log.Printf("Partition %d: commit error: %s", pc.partitionID, string(resp))
	}

	pc.consumer.mu.Lock()
	pc.consumer.offsets[pc.partitionID] = currentOffset
	pc.consumer.mu.Unlock()
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
}

func (pc *PartitionConsumer) closeConn() {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.conn != nil {
		pc.conn.Close()
		pc.conn = nil
	}
}

type GroupCoordinator struct {
	consumer *Consumer
	assigned []int
	doneCh   chan struct{}
}

func (gc *GroupCoordinator) start() {
	duration := time.Duration(gc.consumer.config.HeartbeatIntervalMS) * time.Millisecond
	heartbeatTicker := time.NewTicker(duration)
	defer heartbeatTicker.Stop()

	for {
		select {
		case <-heartbeatTicker.C:
			gc.sendHeartbeat()
		case <-gc.doneCh:
			return
		}
	}
}

func (gc *GroupCoordinator) sendHeartbeat() {
	conn, err := gc.consumer.client.Connect(gc.consumer.config.BrokerAddr)
	if err != nil {
		log.Printf("Heartbeat failed: %v", err)
		return
	}
	defer conn.Close()

	heartbeatCmd := fmt.Sprintf("HEARTBEAT topic=%s group=%s consumer=%s",
		gc.consumer.config.Topic, gc.consumer.config.GroupID, gc.consumer.config.ConsumerID)
	cmdBytes := encodeMessage("", heartbeatCmd)

	if err := sendBinaryMessage(conn, cmdBytes); err != nil {
		log.Printf("Heartbeat send failed: %v", err)
		return
	}

	_, err = readBinaryMessage(conn)
	if err != nil {
		log.Printf("Heartbeat response failed: %v", err)
	}
}

type Consumer struct {
	config             ConsumerConfig
	client             *ConsumerClient
	partitionConsumers map[int]*PartitionConsumer
	coordinator        *GroupCoordinator

	offsets map[int]uint64
	doneCh  chan struct{}
	mu      sync.RWMutex

	closed  bool
	closeMu sync.Mutex

	bmMu        sync.Mutex
	bmMsgCount  int64
	bmStartTime time.Time
}

func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	client := NewConsumerClient()
	c := &Consumer{
		config:             cfg,
		client:             client,
		partitionConsumers: make(map[int]*PartitionConsumer),
		offsets:            make(map[int]uint64),
		doneCh:             make(chan struct{}),
	}
	return c, nil
}

func (c *Consumer) Start() error {
	c.coordinator = &GroupCoordinator{
		consumer: c,
		assigned: make([]int, 0),
		doneCh:   make(chan struct{}),
	}
	go c.coordinator.start()
	log.Println("coordinator started.")

	assigned, err := c.joinGroup()
	if err != nil {
		log.Printf("❌ Failed to join consumer group: %v", err)
		log.Printf("   Topic: %s, Group: %s, Consumer: %s",
			c.config.Topic, c.config.GroupID, c.config.ConsumerID)
		return fmt.Errorf("join group failed: %w", err)
	}

	log.Printf("✅ Successfully joined group '%s' with %d partitions: %v",
		c.config.GroupID, len(assigned), assigned)

	c.partitionConsumers = make(map[int]*PartitionConsumer)
	for _, pid := range assigned {
		pc := &PartitionConsumer{partitionID: pid, consumer: c, offset: 0}
		c.partitionConsumers[pid] = pc
	}

	c.startCommitLoop()
	go c.startConsuming()

	return nil
}

func (c *Consumer) startConsuming() {
	if c.config.EnableBenchmark {
		c.bmStartTime = time.Now()
	}

	for pid, pc := range c.partitionConsumers {
		go func(pid int, pc *PartitionConsumer) {
			ticker := time.NewTicker(c.config.PollInterval)
			defer ticker.Stop()
			for {
				select {
				case <-c.doneCh:
					pc.close()
					return
				case <-ticker.C:
					if c.config.Mode == ModePolling {
						pc.pollAndProcess()
					}
				}
			}
		}(pid, pc)
	}
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
				return
			}
		}
	}()
}

func (c *Consumer) joinGroup() ([]int, error) {
	log.Println("Joining group")
	conn, err := c.client.Connect(c.config.BrokerAddr)
	if err != nil {
		return nil, fmt.Errorf("connect failed: %w", err)
	}
	defer conn.Close()

	joinCmd := fmt.Sprintf("JOIN_GROUP topic=%s group=%s consumer=%s",
		c.config.Topic, c.config.GroupID, c.config.ConsumerID)
	cmdBytes := encodeMessage("", joinCmd)

	if err := sendBinaryMessage(conn, cmdBytes); err != nil {
		return nil, fmt.Errorf("send join command: %w", err)
	}

	resp, err := readBinaryMessage(conn)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	respStr := strings.TrimSpace(string(resp))
	if strings.HasPrefix(respStr, "ERROR:") {
		return nil, fmt.Errorf("broker error: %s", respStr)
	}

	var assigned []int
	if strings.Contains(respStr, "with partitions:") {
		start := strings.Index(respStr, "[")
		end := strings.Index(respStr, "]")
		if start != -1 && end != -1 {
			partitionStr := respStr[start+1 : end]
			parts := strings.Fields(partitionStr)
			for _, p := range parts {
				var id int
				if _, err := fmt.Sscanf(p, "%d", &id); err == nil {
					assigned = append(assigned, id)
				}
			}
		}
	}

	return assigned, nil
}

func (c *Consumer) startStreaming() error {
	for pid, pc := range c.partitionConsumers {
		go func(pid int, pc *PartitionConsumer) {
			for {
				select {
				case <-c.doneCh:
					return
				default:
				}

				duration := time.Duration(pc.consumer.config.StreamingRetryIntervalMS) * time.Millisecond

				if err := pc.ensureConnection(); err != nil {
					log.Printf("Partition %d: streaming connection failed, retrying: %v", pid, err)
					time.Sleep(duration)
					continue
				}

				conn := pc.conn
				streamCmd := fmt.Sprintf("STREAM topic=%s partition=%d group=%s",
					c.config.Topic, pid, c.config.GroupID)
				if err := sendBinaryMessage(conn, encodeMessage("", streamCmd)); err != nil {
					log.Printf("Partition %d: STREAM command send failed: %v", pid, err)
					pc.mu.Lock()
					pc.conn.Close()
					pc.conn = nil
					pc.mu.Unlock()
					time.Sleep(duration)
					continue
				}

				for {
					select {
					case <-c.doneCh:
						return
					default:
					}

					conn.SetReadDeadline(time.Now().Add(5 * time.Minute))
					msgBytes, err := readBinaryMessage(conn)
					if err != nil {
						if ne, ok := err.(net.Error); ok && ne.Timeout() {
							time.Sleep(50 * time.Millisecond)
							continue
						}
						log.Printf("Partition %d: streaming read error: %v", pid, err)
						pc.mu.Lock()
						pc.conn.Close()
						pc.conn = nil
						pc.mu.Unlock()
						break
					}
					if len(msgBytes) == 0 {
						time.Sleep(50 * time.Millisecond)
						continue
					}

					if c.config.EnableBenchmark {
						atomic.AddInt64(&pc.consumer.bmMsgCount, 1)
					}

					c.mu.Lock()
					c.offsets[pid]++
					c.mu.Unlock()
				}
			}
		}(pid, pc)
	}

	go c.startCommitLoop()
	<-c.doneCh
	return nil
}

func (c *Consumer) commitAllOffsets() {
	c.mu.RLock()
	offsetsCopy := make(map[int]uint64)
	for k, v := range c.offsets {
		offsetsCopy[k] = v
	}
	c.mu.RUnlock()

	for partitionID := range offsetsCopy {
		if pc, exists := c.partitionConsumers[partitionID]; exists {
			pc.commitOffset()
		}
	}
}

func (c *Consumer) startPolling() error {
	<-c.coordinator.doneCh
	return nil
}

func (c *Consumer) Close() error {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true

	for _, pc := range c.partitionConsumers {
		pc.mu.Lock()
		pc.closed = true
		if pc.conn != nil {
			pc.conn.Close()
		}
		pc.mu.Unlock()
	}

	close(c.doneCh)
	close(c.coordinator.doneCh)
	return nil
}

func (c *Consumer) PrintBenchmarkSummary() {
	if !c.config.EnableBenchmark {
		return
	}
	c.bmMu.Lock()
	defer c.bmMu.Unlock()
	duration := time.Since(c.bmStartTime)
	tps := float64(atomic.LoadInt64(&c.bmMsgCount)) / duration.Seconds()

	fmt.Println("=== CONSUMER BENCHMARK SUMMARY ===")
	fmt.Printf("Total messages consumed: %d\n", c.bmMsgCount)
	fmt.Printf("Elapsed time: %v\n", duration)
	fmt.Printf("Approx TPS: %.2f\n", tps)
	fmt.Println("==================================")
	os.Exit(0)
}

func sendBinaryMessage(conn net.Conn, msg []byte) error {
	data := []byte(msg)
	buf := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(data)))
	copy(buf[4:], data)
	_, err := conn.Write(buf)
	return err
}

func readBinaryMessage(conn net.Conn) ([]byte, error) {
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, lenBuf); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		log.Printf("❌ Read length error: %v, raw: % x", err, lenBuf)
		return nil, err
	}
	msgLen := binary.BigEndian.Uint32(lenBuf)
	data := make([]byte, msgLen)
	if _, err := io.ReadFull(conn, data); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		log.Printf("❌ Read message error: %v, expected %d bytes, received %d bytes, raw: % x", err, msgLen, len(data), data)
		return nil, err
	}

	dataStr := string(data)
	if !strings.HasPrefix(dataStr, "OK") {
		if len(data) > 64 {
			log.Printf("✅ Read message (truncated to 64 bytes): %s ...", string(data[:64]))
		} else {
			log.Printf("✅ Read message: %s", string(data))
		}
	}

	return data, nil
}

func encodeMessage(topic, payload string) []byte {
	topicBytes := []byte(topic)
	payloadBytes := []byte(payload)
	data := make([]byte, 2+len(topicBytes)+len(payloadBytes))
	binary.BigEndian.PutUint16(data[:2], uint16(len(topicBytes)))
	copy(data[2:2+len(topicBytes)], topicBytes)
	copy(data[2+len(topicBytes):], payloadBytes)
	return data
}

func main() {
	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		log.Printf("Failed to marshal config: %v", err)
	} else {
		log.Printf("Configuration:\n%s", string(data))
	}

	c, err := NewConsumer(*cfg)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}

	c.Start()
	log.Println("consumer started.")

	switch c.config.Mode {
	case ModePolling:
		go c.startPolling()
	case ModeStreaming:
		go c.startStreaming()
	default:
		go c.startPolling()
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	c.PrintBenchmarkSummary()
	if err := c.Close(); err != nil {
		log.Printf("Error closing consumer: %v", err)
	}
}

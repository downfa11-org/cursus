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
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// ConsumerConfig holds all consumer configuration
type ConsumerConfig struct {
	BrokerAddr string `yaml:"broker_addr" json:"broker_addr"`
	GroupID    string `yaml:"group_id" json:"group_id"`
	ConsumerID string `yaml:"consumer_id" json:"consumer_id"`
	Topic      string `yaml:"topic" json:"topic"`
	Partitions []int  `yaml:"partitions" json:"partitions"`

	// Heartbeat settings
	HeartbeatIntervalMS int `yaml:"heartbeat_interval_ms" json:"heartbeat_interval_ms"`
	SessionTimeoutMS    int `yaml:"session_timeout_ms" json:"session_timeout_ms"`

	// Fetcher settings
	MaxPollRecords int `yaml:"max_poll_records" json:"max_poll_records"`
	PollIntervalMS int `yaml:"poll_interval_ms" json:"poll_interval_ms"`

	// Offset settings
	AutoCommit           bool `yaml:"auto_commit" json:"auto_commit"`
	AutoCommitIntervalMS int  `yaml:"auto_commit_interval_ms" json:"auto_commit_interval_ms"`

	// Retry settings
	MaxRetries     int `yaml:"max_retries" json:"max_retries"`
	RetryBackoffMS int `yaml:"retry_backoff_ms" json:"retry_backoff_ms"`

	// Join group retry
	JoinGroupMaxRetries   int `yaml:"join_group_max_retries" json:"join_group_max_retries"`
	JoinGroupRetryDelayMS int `yaml:"join_group_retry_delay_ms" json:"join_group_retry_delay_ms"`

	DefaultPartitions []int `yaml:"default_partitions" json:"default_partitions"`
	EmptyPollDelayMS  int   `yaml:"empty_poll_delay_ms" json:"empty_poll_delay_ms"`
	PollTimeoutMS     int   `yaml:"poll_timeout_ms" json:"poll_timeout_ms"`
}

// Message represents a consumed message
type Message struct {
	Partition int
	Offset    int
	Payload   string
}

// OffsetManager tracks offsets per partition
type OffsetManager struct {
	offsets map[int]int
	mu      sync.RWMutex
}

func NewOffsetManager() *OffsetManager {
	return &OffsetManager{
		offsets: make(map[int]int),
	}
}

func (om *OffsetManager) Commit(partition, offset int) {
	om.mu.Lock()
	defer om.mu.Unlock()
	om.offsets[partition] = offset
}

func (om *OffsetManager) Get(partition int) int {
	om.mu.RLock()
	defer om.mu.RUnlock()
	return om.offsets[partition]
}

// Consumer represents a message consumer
type Consumer struct {
	config         *ConsumerConfig
	offsetManager  *OffsetManager
	heartbeatDone  chan struct{}
	pollDone       chan struct{}
	lastCommitTime time.Time
	ready          chan struct{}
}

func LoadConsumerConfig() (*ConsumerConfig, error) {
	cfg := &ConsumerConfig{}

	// defaults
	flag.StringVar(&cfg.BrokerAddr, "broker", "localhost:9000", "Broker address")
	flag.StringVar(&cfg.GroupID, "group-id", "default-group", "Consumer group ID")
	flag.StringVar(&cfg.ConsumerID, "consumer-id", "", "Consumer ID (auto-generated if empty)")
	flag.StringVar(&cfg.Topic, "topic", "my-topic", "Topic name")
	flag.IntVar(&cfg.HeartbeatIntervalMS, "heartbeat-interval-ms", 3000, "Heartbeat interval in milliseconds")
	flag.IntVar(&cfg.SessionTimeoutMS, "session-timeout-ms", 10000, "Session timeout in milliseconds")
	flag.IntVar(&cfg.MaxPollRecords, "max-poll-records", 100, "Maximum records per poll")
	flag.IntVar(&cfg.PollIntervalMS, "poll-interval-ms", 1000, "Poll interval in milliseconds")
	flag.BoolVar(&cfg.AutoCommit, "auto-commit", true, "Enable auto commit")
	flag.IntVar(&cfg.AutoCommitIntervalMS, "auto-commit-interval-ms", 5000, "Auto commit interval in milliseconds")
	flag.IntVar(&cfg.MaxRetries, "max-retries", 3, "Maximum retry attempts")
	flag.IntVar(&cfg.RetryBackoffMS, "retry-backoff-ms", 100, "Initial backoff time in milliseconds")
	flag.IntVar(&cfg.JoinGroupMaxRetries, "join-group-max-retries", 10, "Maximum retry attempts for JOIN_GROUP")
	flag.IntVar(&cfg.JoinGroupRetryDelayMS, "join-group-retry-delay-ms", 500, "Retry delay in milliseconds for JOIN_GROUP")
	flag.IntVar(&cfg.EmptyPollDelayMS, "empty-poll-delay-ms", 50, "Delay when no messages available in milliseconds")
	flag.IntVar(&cfg.PollTimeoutMS, "poll-timeout-ms", 5000, "Poll timeout in milliseconds")

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

	if strings.TrimSpace(cfg.BrokerAddr) == "" {
		cfg.BrokerAddr = "localhost:9000"
	}

	if strings.TrimSpace(cfg.GroupID) == "" {
		cfg.GroupID = "default-group"
	}

	if strings.TrimSpace(cfg.Topic) == "" {
		cfg.Topic = "default-topic"
	}

	if cfg.HeartbeatIntervalMS <= 0 {
		cfg.HeartbeatIntervalMS = 1000
	}

	if cfg.SessionTimeoutMS <= 0 {
		cfg.SessionTimeoutMS = 5000
	}

	if cfg.MaxPollRecords <= 0 {
		cfg.MaxPollRecords = 1
	}

	if cfg.PollIntervalMS <= 0 {
		cfg.PollIntervalMS = 200
	}

	if cfg.AutoCommitIntervalMS <= 0 {
		cfg.AutoCommitIntervalMS = 1000
	}

	if cfg.MaxRetries < 0 {
		cfg.MaxRetries = 0
	}

	if cfg.RetryBackoffMS <= 0 {
		cfg.RetryBackoffMS = 50
	}

	if cfg.JoinGroupMaxRetries <= 0 {
		cfg.JoinGroupMaxRetries = 5
	}

	if cfg.JoinGroupRetryDelayMS <= 0 {
		cfg.JoinGroupRetryDelayMS = 200
	}

	if cfg.EmptyPollDelayMS < 0 {
		cfg.EmptyPollDelayMS = 0
	}

	if cfg.PollTimeoutMS <= 0 {
		cfg.PollTimeoutMS = 1000
	}

	if cfg.ConsumerID == "" {
		cfg.ConsumerID = fmt.Sprintf("consumer-%d", time.Now().UnixNano())
	}

	return cfg, nil
}

func NewConsumer(cfg *ConsumerConfig) (*Consumer, error) {
	consumer := &Consumer{
		config:         cfg,
		offsetManager:  NewOffsetManager(),
		heartbeatDone:  make(chan struct{}),
		pollDone:       make(chan struct{}),
		lastCommitTime: time.Now(),
		ready:          make(chan struct{}),
	}

	if len(cfg.Partitions) == 0 {
		if len(cfg.DefaultPartitions) > 0 {
			cfg.Partitions = cfg.DefaultPartitions
		} else {
			cfg.Partitions = []int{0, 1, 2, 3}
		}
	}

	if err := consumer.joinGroup(); err != nil {
		log.Printf("Failed to join group: %v", err)
		return nil, fmt.Errorf("failed to join group: %w", err)
	}

	for _, partition := range cfg.Partitions {
		consumer.offsetManager.Commit(partition, 0)
	}

	go consumer.startHeartbeat()

	close(consumer.ready)

	log.Printf("Consumer '%s' initialized for group '%s' on topic '%s'",
		cfg.ConsumerID, cfg.GroupID, cfg.Topic)

	return consumer, nil
}

func WriteWithLength(conn net.Conn, data []byte) error {
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))

	if _, err := conn.Write(lenBuf); err != nil {
		return fmt.Errorf("write length: %w", err)
	}

	if _, err := conn.Write(data); err != nil {
		return fmt.Errorf("write data: %w", err)
	}

	return nil
}

func ReadWithLength(conn net.Conn) ([]byte, error) {
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, lenBuf); err != nil {
		return nil, fmt.Errorf("read length: %w", err)
	}

	msgLen := binary.BigEndian.Uint32(lenBuf)
	msgBuf := make([]byte, msgLen)

	if _, err := io.ReadFull(conn, msgBuf); err != nil {
		return nil, fmt.Errorf("read message: %w", err)
	}

	return msgBuf, nil
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

func (c *Consumer) startHeartbeat() {
	ticker := time.NewTicker(time.Duration(c.config.HeartbeatIntervalMS) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-c.heartbeatDone:
			return
		case <-ticker.C:
			if err := c.sendHeartbeat(); err != nil {
				log.Printf("Heartbeat failed: %v", err)
			}
		}
	}
}

func (c *Consumer) sendHeartbeat() error {
	conn, err := net.Dial("tcp", c.config.BrokerAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	cmd := EncodeMessage("admin", fmt.Sprintf("HEARTBEAT %s %s", c.config.GroupID, c.config.ConsumerID))
	if err := WriteWithLength(conn, cmd); err != nil {
		return err
	}

	_, err = ReadWithLength(conn)
	if err != nil && err != io.EOF && !strings.Contains(err.Error(), "EOF") {
		return err
	}
	return nil
}

func (c *Consumer) Poll(timeout time.Duration) ([]Message, error) {
	var allMessages []Message

	for _, partition := range c.config.Partitions {
		offset := c.offsetManager.Get(partition)

		messages, err := c.fetchMessages(partition, offset)
		if err != nil {
			continue
		}

		if len(messages) > 0 {
			if c.config.AutoCommit {
				lastOffset := messages[len(messages)-1].Offset
				c.offsetManager.Commit(partition, lastOffset+1)
			}
		}

		allMessages = append(allMessages, messages...)

		// auto commit
		if c.config.AutoCommit && len(messages) > 0 {
			lastOffset := messages[len(messages)-1].Offset
			c.offsetManager.Commit(partition, lastOffset+1)
		}
	}

	if c.config.AutoCommit && time.Since(c.lastCommitTime) >
		time.Duration(c.config.AutoCommitIntervalMS)*time.Millisecond {
		c.commitOffsets()
		c.lastCommitTime = time.Now()
	}

	if len(allMessages) == 0 {
		time.Sleep(time.Duration(c.config.EmptyPollDelayMS) * time.Millisecond)
	}

	return allMessages, nil
}

func (c *Consumer) fetchMessages(partition, offset int) ([]Message, error) {
	conn, err := net.Dial("tcp", c.config.BrokerAddr)
	if err != nil {
		return nil, fmt.Errorf("connect to broker: %w", err)
	}
	defer conn.Close()

	timeout := time.Duration(c.config.PollTimeoutMS) * time.Millisecond
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return nil, fmt.Errorf("set read deadline: %w", err)
	}

	consumeCmd := fmt.Sprintf("CONSUME %s %d %d", c.config.Topic, partition, offset)
	cmdBytes := EncodeMessage(c.config.Topic, consumeCmd)

	if err := WriteWithLength(conn, cmdBytes); err != nil {
		return nil, fmt.Errorf("send consume command: %w", err)
	}

	var messages []Message
	for i := 0; i < c.config.MaxPollRecords; i++ {
		if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
			return messages, fmt.Errorf("reset read deadline: %w", err)
		}

		msgBytes, err := ReadWithLength(conn)
		if err != nil {
			if err == io.EOF || strings.Contains(err.Error(), "EOF") {
				break
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				break
			}
			return messages, err
		}

		messages = append(messages, Message{
			Partition: partition,
			Offset:    offset + i,
			Payload:   string(msgBytes),
		})
	}

	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		log.Printf("Warning: Failed to clear read deadline: %v", err)
	}

	if len(messages) == 0 {
		c.offsetManager.Commit(partition, offset+1)
	}

	return messages, nil
}

func (c *Consumer) commitOffsets() {
	for _, partition := range c.config.Partitions {
		conn, err := net.Dial("tcp", c.config.BrokerAddr)
		if err != nil {
			log.Printf("Failed to connect for offset commit partition %d: %v", partition, err)
			continue
		}

		offset := c.offsetManager.Get(partition)
		commitCmd := EncodeMessage("admin",
			fmt.Sprintf("COMMIT_OFFSET %s %d %d", c.config.Topic, partition, offset))

		if err := WriteWithLength(conn, commitCmd); err != nil {
			log.Printf("Failed to commit offset for partition %d: %v", partition, err)
			conn.Close()
			continue
		}

		_, err = ReadWithLength(conn)
		if err != nil && err != io.EOF && !strings.Contains(err.Error(), "EOF") {
			log.Printf("Failed to read commit response for partition %d: %v", partition, err)
		}

		conn.Close()
	}
}

func (c *Consumer) Close() error {
	close(c.heartbeatDone)
	close(c.pollDone)

	// Final commit
	if c.config.AutoCommit {
		c.commitOffsets()
	}

	return nil
}

func (c *Consumer) joinGroup() error {
	maxRetries := 10 // default
	if c.config.JoinGroupMaxRetries > 0 {
		maxRetries = c.config.JoinGroupMaxRetries
	}

	retryDelay := 500 * time.Millisecond // default
	if c.config.JoinGroupRetryDelayMS > 0 {
		retryDelay = time.Duration(c.config.JoinGroupRetryDelayMS) * time.Millisecond
	}

	conn1, err := net.Dial("tcp", c.config.BrokerAddr)
	if err != nil {
		return fmt.Errorf("connect to broker: %w", err)
	}
	defer conn1.Close()

	setGroupCmd := EncodeMessage(c.config.Topic, fmt.Sprintf("SETGROUP %s", c.config.GroupID))
	if err := WriteWithLength(conn1, setGroupCmd); err != nil {
		return fmt.Errorf("send setgroup command: %w", err)
	}

	resp1, err := ReadWithLength(conn1)
	if err != nil {
		return fmt.Errorf("read setgroup response: %w", err)
	}
	log.Printf("Consumer group set to '%s' '%s'", c.config.GroupID, resp1)
	// REGISTER_GROUP
	for attempt := 1; attempt <= maxRetries; attempt++ {
		conn2, err := net.Dial("tcp", c.config.BrokerAddr)
		if err != nil {
			if attempt < maxRetries {
				time.Sleep(retryDelay)
				retryDelay *= 2
				continue
			}
			return fmt.Errorf("connect to broker: %w", err)
		}

		registerCmd := EncodeMessage("admin", fmt.Sprintf("REGISTER_GROUP %s %s", c.config.Topic, c.config.GroupID))
		if err := WriteWithLength(conn2, registerCmd); err != nil {
			conn2.Close()
			if attempt < maxRetries {
				time.Sleep(retryDelay)
				retryDelay *= 2
				continue
			}
			return fmt.Errorf("send register_group command: %w", err)
		}

		resp2, err := ReadWithLength(conn2)
		conn2.Close()

		if err != nil {
			if attempt < maxRetries {
				log.Printf("Waiting for topic to be created... (attempt %d/%d)", attempt+1, maxRetries)
				time.Sleep(retryDelay)
				retryDelay *= 2
				continue
			}
			return fmt.Errorf("read register_group response: %w", err)
		}

		if strings.HasPrefix(string(resp2), "ERROR:") {
			if strings.Contains(string(resp2), "does not exist") {
				// Topic is not created, retry
				if attempt < maxRetries {
					log.Printf("Waiting for topic to be created... (attempt %d/%d)", attempt+1, maxRetries)
					time.Sleep(retryDelay)
					retryDelay *= 2
					continue
				}
			}
			return fmt.Errorf("register group failed: %s", string(resp2))
		}

		log.Printf("✅ Registered group '%s': %s", c.config.GroupID, string(resp2))
		break
	}

	// JOIN_GROUP
	for attempt := 1; attempt <= maxRetries; attempt++ {
		conn3, err := net.Dial("tcp", c.config.BrokerAddr)
		if err != nil {
			if attempt < maxRetries {
				time.Sleep(retryDelay)
				retryDelay *= 2
				continue
			}
			return fmt.Errorf("connect to broker: %w", err)
		}

		joinCmd := EncodeMessage("admin", fmt.Sprintf("JOIN_GROUP %s %s", c.config.GroupID, c.config.ConsumerID))
		if err := WriteWithLength(conn3, joinCmd); err != nil {
			conn3.Close()
			if attempt < maxRetries {
				time.Sleep(retryDelay)
				retryDelay *= 2
				continue
			}
			return fmt.Errorf("send join_group command: %w", err)
		}

		resp3, err := ReadWithLength(conn3)
		conn3.Close()

		if err != nil {
			if attempt < maxRetries {
				time.Sleep(retryDelay)
				retryDelay *= 2
				continue
			}
			return fmt.Errorf("read join_group response: %w", err)
		}

		if strings.HasPrefix(string(resp3), "ERROR:") {
			if strings.Contains(string(resp3), "group not found") {
				return fmt.Errorf("join group failed: %s", string(resp3))
			}
			return fmt.Errorf("join group failed: %s", string(resp3))
		}

		log.Printf("Joined group '%s': %s", c.config.GroupID, string(resp3))
		return nil
	}

	return fmt.Errorf("failed to join group after %d attempts", maxRetries)
}

func main() {
	cfg, err := LoadConsumerConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	fmt.Printf("📋 Consumer Configuration:\n")
	fmt.Printf("  Broker: %s\n", cfg.BrokerAddr)
	fmt.Printf("  Group ID: %s\n", cfg.GroupID)
	fmt.Printf("  Consumer ID: %s\n", cfg.ConsumerID)
	fmt.Printf("  Topic: %s\n", cfg.Topic)
	fmt.Printf("  Partitions: %v\n", cfg.Partitions)
	fmt.Printf("  Heartbeat Interval: %dms\n", cfg.HeartbeatIntervalMS)
	fmt.Printf("  Max Poll Records: %d\n", cfg.MaxPollRecords)
	fmt.Printf("  Auto Commit: %v\n\n", cfg.AutoCommit)

	consumer, err := NewConsumer(cfg)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	<-consumer.ready
	fmt.Println("Starting to consume messages...")

	pollInterval := time.Duration(cfg.PollIntervalMS) * time.Millisecond
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	messageCount := 0
	for range ticker.C {
		messages, err := consumer.Poll(5 * time.Second)
		if err != nil {
			log.Printf("Poll error: %v", err)
			continue
		}

		for _, msg := range messages {
			messageCount++
			fmt.Printf("[%d] Partition %d, Offset %d: %s\n",
				messageCount, msg.Partition, msg.Offset, msg.Payload)
		}

		if len(messages) == 0 {
			fmt.Printf("No new messages (total consumed: %d)\n", messageCount)
		}
	}
}

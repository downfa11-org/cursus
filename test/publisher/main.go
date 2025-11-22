package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type PublisherConfig struct {
	BrokerAddr     string `yaml:"broker_addr" json:"broker_addr"`
	MaxRetries     int    `yaml:"max_retries" json:"max_retries"`
	RetryBackoffMS int    `yaml:"retry_backoff_ms" json:"retry_backoff_ms"`
	AckTimeoutMS   int    `yaml:"ack_timeout_ms" json:"ack_timeout_ms"`
	Topic          string `yaml:"topic" json:"topic"`
	Partitions     int    `yaml:"partitions" json:"partitions"`
	NumMessages    int    `yaml:"num_messages" json:"num_messages"`
	PublishDelayMS int    `yaml:"publish_delay_ms" json:"publish_delay_ms"`
}

func LoadPublisherConfig() (*PublisherConfig, error) {
	cfg := &PublisherConfig{}

	// default
	flag.StringVar(&cfg.BrokerAddr, "broker", "localhost:9000", "Broker address")
	flag.IntVar(&cfg.MaxRetries, "max-retries", 3, "Maximum retry attempts")
	flag.IntVar(&cfg.RetryBackoffMS, "retry-backoff-ms", 100, "Initial backoff time in milliseconds")
	flag.IntVar(&cfg.AckTimeoutMS, "ack-timeout-ms", 5000, "ACK timeout in milliseconds")
	flag.StringVar(&cfg.Topic, "topic", "my-topic", "Topic name")
	flag.IntVar(&cfg.Partitions, "partitions", 4, "Number of partitions")
	flag.IntVar(&cfg.NumMessages, "num-messages", 10, "Number of messages to publish")
	flag.IntVar(&cfg.PublishDelayMS, "publish-delay-ms", 100, "Delay between messages in milliseconds")

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
	if cfg.Partitions <= 0 {
		cfg.Partitions = 1
	}
	if cfg.NumMessages < 0 {
		cfg.NumMessages = 0
	}
	if cfg.PublishDelayMS < 0 {
		cfg.PublishDelayMS = 0
	}
	if strings.TrimSpace(cfg.Topic) == "" {
		cfg.Topic = "default-topic"
	}

	return cfg, nil
}

type Publisher struct {
	config *PublisherConfig
}

func NewPublisher(cfg *PublisherConfig) *Publisher {
	return &Publisher{config: cfg}
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

func (p *Publisher) SendWithRetry(data []byte) error {
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
		fmt.Printf("Attempt %d/%d failed (%v), retrying in %v\n",
			attempt+1, p.config.MaxRetries+1, err, backoff)

		time.Sleep(backoff)
	}

	return fmt.Errorf("unexpected retry loop exit")
}

func (p *Publisher) tryOnce(data []byte, ackTimeout time.Duration) error {
	conn, err := net.Dial("tcp", p.config.BrokerAddr)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	if err := WriteWithLength(conn, data); err != nil {
		return fmt.Errorf("write: %w", err)
	}

	// ACK deadline
	if err := conn.SetReadDeadline(time.Now().Add(ackTimeout)); err != nil {
		return fmt.Errorf("set read deadline: %w", err)
	}

	// READ ACK
	resp, err := ReadWithLength(conn)
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

func (p *Publisher) CreateTopic() error {
	conn, err := net.Dial("tcp", p.config.BrokerAddr)
	if err != nil {
		return fmt.Errorf("connect to broker: %w", err)
	}
	defer conn.Close()

	createCmd := EncodeMessage(p.config.Topic, fmt.Sprintf("CREATE %s %d", p.config.Topic, p.config.Partitions))

	if err := p.SendWithRetry(createCmd); err != nil {
		if strings.Contains(err.Error(), "topic exists") || strings.Contains(err.Error(), "already exists") {
			fmt.Printf("Topic '%s' already exists\n", p.config.Topic)
			return nil
		}
		return fmt.Errorf("topic creation failed: %w", err)
	}

	fmt.Printf("Topic '%s' created with %d partitions\n", p.config.Topic, p.config.Partitions)
	return nil
}

func (p *Publisher) PublishMessage(message string) error {
	conn, err := net.Dial("tcp", p.config.BrokerAddr)
	if err != nil {
		return fmt.Errorf("connect to broker: %w", err)
	}
	defer conn.Close()

	msgBytes := EncodeMessage(p.config.Topic, message)
	return p.SendWithRetry(msgBytes)
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

	if err := publisher.CreateTopic(); err != nil {
		fmt.Printf("Failed to create topic: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\nPublishing messages...")
	for i := 0; i < cfg.NumMessages; i++ {
		message := fmt.Sprintf("Hello from Go client! Message #%d", i)

		if err := publisher.PublishMessage(message); err != nil {
			fmt.Printf("Failed to publish message %d: %v\n", i, err)
			continue
		}

		fmt.Printf("Message %d published successfully\n", i)
		time.Sleep(time.Duration(cfg.PublishDelayMS) * time.Millisecond)
	}

	fmt.Println("\n All messages published successfully!")
}

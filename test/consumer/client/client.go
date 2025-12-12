package client

import (
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/downfa11-org/go-broker/consumer/config"
	"github.com/google/uuid"
)

type ConsumerClient struct {
	ID       string
	config   *config.ConsumerConfig
	connPool map[string]net.Conn
	mu       sync.RWMutex
}

func NewConsumerClient(cfg *config.ConsumerConfig) *ConsumerClient {
	return &ConsumerClient{
		ID:       uuid.New().String(),
		config:   cfg,
		connPool: make(map[string]net.Conn),
	}
}

func (c *ConsumerClient) GetNextBroker() string {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.config.BrokerAddrs) == 0 {
		return ""
	}

	broker := c.config.BrokerAddrs[c.config.CurrentBrokerIndex]
	c.config.CurrentBrokerIndex = (c.config.CurrentBrokerIndex + 1) % len(c.config.BrokerAddrs)
	return broker
}

func (c *ConsumerClient) ConnectWithFailover() (net.Conn, string, error) {
	if len(c.config.BrokerAddrs) == 0 {
		return nil, "", fmt.Errorf("no broker addresses configured")
	}

	var lastErr error
	for i := 0; i < len(c.config.BrokerAddrs); i++ {
		broker := c.GetNextBroker()

		conn, err := net.Dial("tcp", broker)
		if err == nil {
			c.mu.Lock()
			c.connPool[broker] = conn
			c.mu.Unlock()
			return conn, broker, nil
		}
		lastErr = err
		log.Printf("Failed to connect to %s: %v", broker, err)
	}

	return nil, "", fmt.Errorf("failed to connect to all brokers: %w", lastErr)
}

func (c *ConsumerClient) Connect(addr string) (net.Conn, error) {
	c.mu.Lock()
	if conn, exists := c.connPool[addr]; exists && conn != nil {
		c.mu.Unlock()
		return conn, nil
	}
	c.mu.Unlock()

	conn, err := net.Dial("tcp", addr)
	if err == nil {
		c.mu.Lock()
		c.connPool[addr] = conn
		c.mu.Unlock()
	}
	return conn, err
}

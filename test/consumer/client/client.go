package client

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/downfa11-org/go-broker/consumer/config"
	"github.com/downfa11-org/go-broker/util"
	"github.com/google/uuid"
)

type ConsumerClient struct {
	ID     string
	config *config.ConsumerConfig
	mu     sync.RWMutex

	leaderAddr       string
	lastLeaderUpdate time.Time
}

func NewConsumerClient(cfg *config.ConsumerConfig) *ConsumerClient {
	return &ConsumerClient{
		ID:     uuid.New().String(),
		config: cfg,
	}
}

func (c *ConsumerClient) selectBroker() string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.leaderAddr != "" && time.Since(c.lastLeaderUpdate) < 30*time.Second {
		return c.leaderAddr
	}
	if len(c.config.BrokerAddrs) > 0 {
		return c.config.BrokerAddrs[0]
	}
	return ""
}

func (c *ConsumerClient) UpdateLeader(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leaderAddr != addr {
		c.leaderAddr = addr
		c.lastLeaderUpdate = time.Now()
		util.Info("📍 Leader updated to: %s", addr)
	}
}

func (c *ConsumerClient) Connect(addr string) (net.Conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	dialer := net.Dialer{Timeout: 5 * time.Second}
	conn, err := dialer.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial failed to %s: %w", addr, err)
	}

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}

	return conn, nil
}

func (c *ConsumerClient) ConnectWithFailover() (net.Conn, string, error) {
	addrs := c.config.BrokerAddrs
	if len(addrs) == 0 {
		return nil, "", fmt.Errorf("no broker addresses configured")
	}

	leader := c.selectBroker()
	if leader != "" {
		if conn, err := c.Connect(leader); err == nil {
			return conn, leader, nil
		}
		util.Warn("Leader %s failed, falling back to other brokers", leader)
	}

	var lastErr error
	for i := 0; i < len(addrs); i++ {
		addr := addrs[i]
		if addr == leader {
			continue
		}

		conn, err := c.Connect(addr)
		if err == nil {
			return conn, addr, nil
		}
		lastErr = err
		util.Warn("Failover: failed to connect to %s: %v", addr, err)
	}

	return nil, "", fmt.Errorf("all brokers unreachable: %w", lastErr)
}

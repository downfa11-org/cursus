package client

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/downfa11-org/go-broker/consumer/config"
	"github.com/downfa11-org/go-broker/util"
	"github.com/google/uuid"
)

type leaderInfo struct {
	addr    string
	updated time.Time
}

type ConsumerClient struct {
	ID     string
	config *config.ConsumerConfig
	mu     sync.RWMutex

	leader atomic.Value
}

func NewConsumerClient(cfg *config.ConsumerConfig) *ConsumerClient {
	c := &ConsumerClient{
		ID:     uuid.New().String(),
		config: cfg,
	}
	c.leader.Store(&leaderInfo{addr: "", updated: time.Time{}})
	return c
}

func (c *ConsumerClient) selectBroker() string {
	info := c.leader.Load().(*leaderInfo)
	if info.addr != "" && time.Since(info.updated) < 30*time.Second {
		return info.addr
	}

	if len(c.config.BrokerAddrs) > 0 {
		return c.config.BrokerAddrs[0]
	}
	return ""
}

func (c *ConsumerClient) UpdateLeader(addr string) {
	oldInfo := c.leader.Load().(*leaderInfo)
	if oldInfo.addr != addr {
		c.leader.Store(&leaderInfo{
			addr:    addr,
			updated: time.Now(),
		})
		util.Info("📍 Leader updated to: %s (Lock-free update)", addr)
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
		tcpConn.SetNoDelay(true)
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)

		tcpConn.SetReadBuffer(2 * 1024 * 1024)  // 2MB
		tcpConn.SetWriteBuffer(2 * 1024 * 1024) // 2MB
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
			c.UpdateLeader(addr)
			return conn, addr, nil
		}
		lastErr = err
		util.Warn("Failover: failed to connect to %s: %v", addr, err)
	}

	return nil, "", fmt.Errorf("all brokers unreachable: %w", lastErr)
}

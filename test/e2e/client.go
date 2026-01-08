package e2e

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/downfa11-org/go-broker/util"
)

// BrokerClient wraps low-level broker communication
type BrokerClient struct {
	addrs         []string
	conn          net.Conn
	mu            sync.Mutex
	closed        bool
	topic         string
	consumerGroup string
	memberID      string // consumerID + uuid
	generation    int
}

// ConsumerGroupStatus represents consumer group metadata
type ConsumerGroupStatus struct {
	GroupName      string       `json:"group_name"`
	TopicName      string       `json:"topic_name"`
	State          string       `json:"state"`
	MemberCount    int          `json:"member_count"`
	PartitionCount int          `json:"partition_count"`
	Members        []MemberInfo `json:"members"`
	LastRebalance  time.Time    `json:"last_rebalance"`
}

type MemberInfo struct {
	MemberID      string    `json:"member_id"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	Assignments   []int     `json:"assignments"`
}

func NewBrokerClient(addrs []string) *BrokerClient {
	return &BrokerClient{
		addrs: addrs,
	}
}

func (bc *BrokerClient) connect() error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	if bc.conn != nil && !bc.closed {
		return nil
	}

	var lastErr error
	for _, addr := range bc.addrs {
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err == nil {
			if bc.conn != nil {
				bc.conn.Close()
			}
			bc.conn = conn
			bc.closed = false
			return nil
		}
		lastErr = err
	}

	return fmt.Errorf("failed to connect to any broker in %v: %w", bc.addrs, lastErr)
}

func (bc *BrokerClient) Close() {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	if bc.conn != nil {
		bc.conn.Close()
		bc.conn = nil
	}
	bc.closed = true
}

// sendCommandAndGetResponse executes a command on an existing connection and returns the response string.
func (bc *BrokerClient) sendCommandAndGetResponse(cmdTopic, cmdPayload string, readTimeout time.Duration) (string, error) {
	if err := bc.connect(); err != nil {
		return "", err
	}

	bc.mu.Lock()
	defer bc.mu.Unlock()

	if bc.conn == nil || bc.closed {
		return "", fmt.Errorf("connection is closed")
	}

	cmdBytes := util.EncodeMessage(cmdTopic, cmdPayload)
	if err := util.WriteWithLength(bc.conn, cmdBytes); err != nil {
		return "", fmt.Errorf("send command: %w", err)
	}

	if err := bc.conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
		return "", fmt.Errorf("set read deadline: %w", err)
	}

	respBuf, err := util.ReadWithLength(bc.conn)

	if err != nil {
		return "", fmt.Errorf("read response: %w", err)
	}
	return strings.TrimSpace(string(respBuf)), nil
}

// executeCommand is a simplified wrapper for commands expected to return only "OK" or "ERROR:".
func (bc *BrokerClient) executeCommand(topic, payload string) error {
	resp, err := bc.sendCommandAndGetResponse(topic, payload, 2*time.Second)
	if err != nil {
		return err
	}
	if strings.HasPrefix(resp, "ERROR:") {
		return fmt.Errorf("broker error: %s", resp)
	}
	return nil
}

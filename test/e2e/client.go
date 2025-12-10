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
	addr          string
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

func NewBrokerClient(addr string) *BrokerClient {
	return &BrokerClient{addr: addr}
}

func (bc *BrokerClient) connect() error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	if bc.conn != nil && !bc.closed {
		return nil
	}

	if bc.conn != nil {
		bc.conn.Close()
	}

	conn, err := net.Dial("tcp", bc.addr)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}

	bc.conn = conn
	bc.closed = false
	return nil
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
	var conn net.Conn
	shouldClose := false

	bc.mu.Lock()
	if bc.conn == nil {
		bc.mu.Unlock()
		var err error
		conn, err = net.Dial("tcp", bc.addr)
		if err != nil {
			return "", fmt.Errorf("connect: %w", err)
		}
		shouldClose = true
	} else {
		conn = bc.conn
		bc.mu.Unlock()
	}

	defer func() {
		if shouldClose {
			conn.Close()
		} else {
			if err := conn.SetReadDeadline(time.Time{}); err != nil {
				util.Warn("failed to reset read deadline: %v", err)
			}
		}
	}()

	cmdBytes := util.EncodeMessage(cmdTopic, cmdPayload)

	if err := conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
		return "", fmt.Errorf("set read deadline: %w", err)
	}

	if err := util.WriteWithLength(conn, cmdBytes); err != nil {
		return "", fmt.Errorf("send command: %w", err)
	}

	respBuf, err := util.ReadWithLength(conn)

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

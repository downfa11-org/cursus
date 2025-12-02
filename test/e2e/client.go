package e2e

import (
	"encoding/json"
	"fmt"
	"io"
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
	registered    bool
	topic         string
	consumerGroup string
	consumerID    string
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

func (bc *BrokerClient) sendCommand(topic, payload string) error {
	conn, err := net.Dial("tcp", bc.addr)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()

	cmdBytes := util.EncodeMessage(topic, payload)
	if err := util.WriteWithLength(conn, cmdBytes); err != nil {
		return fmt.Errorf("send command: %w", err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return fmt.Errorf("set read deadline: %w", err)
	}
	defer func() {
		if err := conn.SetReadDeadline(time.Time{}); err != nil {
			util.Warn("Failed to clear read deadline: %v", err)
		}
	}()

	respBuf, err := util.ReadWithLength(conn)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	resp := strings.TrimSpace(string(respBuf))
	if strings.HasPrefix(resp, "ERROR:") {
		return fmt.Errorf("broker error: %s", resp)
	}

	return nil
}

// CreateTopic sends CREATE command to broker
func (bc *BrokerClient) CreateTopic(topic string, partitions int) error {
	createCmd := fmt.Sprintf("CREATE topic=%s partitions=%d", topic, partitions)

	err := bc.sendCommand("admin", createCmd)
	if err != nil && strings.Contains(err.Error(), "topic exists") {
		return nil
	}
	return err
}

// PublishIdempotent sends a message with idempotence metadata
func (bc *BrokerClient) PublishIdempotent(topic, producerID string, seqNum uint64, epoch int64, payload, acks string) error {
	publishCmd := fmt.Sprintf("PUBLISH topic=%s acks=%s producerId=%s seqNum=%d epoch=%d message=%s",
		topic, acks, producerID, seqNum, epoch, payload)

	if acks == "0" {
		conn, err := net.Dial("tcp", bc.addr)
		if err != nil {
			return fmt.Errorf("connect: %w", err)
		}
		defer conn.Close()

		cmdBytes := util.EncodeMessage("admin", publishCmd)
		return util.WriteWithLength(conn, cmdBytes)
	}

	return bc.sendCommand("admin", publishCmd)
}

// PublishSimple sends a message without idempotence
func (bc *BrokerClient) PublishSimple(topic, payload, acks string) error {
	publishCmd := fmt.Sprintf("PUBLISH topic=%s acks=%s message=%s", topic, acks, payload)

	if acks == "0" {
		conn, err := net.Dial("tcp", bc.addr)
		if err != nil {
			return fmt.Errorf("connect: %w", err)
		}
		defer conn.Close()

		cmdBytes := util.EncodeMessage("admin", publishCmd)
		return util.WriteWithLength(conn, cmdBytes)
	}

	return bc.sendCommand("admin", publishCmd)
}

// ConsumeMessages reads messages from a partition
func (bc *BrokerClient) ConsumeMessages(topic string, partition int, consumerGroup string, timeout time.Duration) ([]string, error) {
	conn, err := net.Dial("tcp", bc.addr)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()

	registerCmd := fmt.Sprintf("REGISTER_GROUP topic=%s group=%s", topic, consumerGroup)
	cmdBytes := util.EncodeMessage("", registerCmd)

	if err := util.WriteWithLength(conn, cmdBytes); err != nil {
		return nil, fmt.Errorf("register group: %w", err)
	}

	resp, err := util.ReadWithLength(conn)
	if err != nil {
		return nil, fmt.Errorf("register response: %w", err)
	}

	if strings.Contains(string(resp), "ERROR:") {
		return nil, fmt.Errorf("register failed: %s", string(resp))
	}

	startOffset := 0
	consumeCmd := fmt.Sprintf("CONSUME topic=%s partition=%d offset=%d group=%s autoOffsetReset=earliest",
		topic, partition, startOffset, consumerGroup)
	cmdBytes = util.EncodeMessage(topic, consumeCmd)

	if err := util.WriteWithLength(conn, cmdBytes); err != nil {
		return nil, fmt.Errorf("send consume command: %w", err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return nil, fmt.Errorf("set read deadline: %w", err)
	}
	defer func() {
		if err := conn.SetReadDeadline(time.Time{}); err != nil {
			util.Error("failed to reset read deadline: %v", err)
		}
	}()

	var messages []string
	for {
		msgBytes, err := util.ReadWithLength(conn)
		if err != nil {
			if err == io.EOF || strings.Contains(err.Error(), "EOF") {
				break
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				break
			}
			return messages, fmt.Errorf("read message: %w", err)
		}

		if len(msgBytes) == 0 {
			break
		}

		messages = append(messages, string(msgBytes))
	}

	return messages, nil
}

// GetConsumerGroupStatus retrieves consumer group metadata from broker
func (bc *BrokerClient) GetConsumerGroupStatus(groupID string) (*ConsumerGroupStatus, error) {
	bc.mu.Lock()
	if bc.conn == nil || bc.closed {
		bc.mu.Unlock()
		if err := bc.connect(); err != nil {
			return nil, err
		}
		bc.mu.Lock()
	}
	conn := bc.conn
	bc.mu.Unlock()

	if conn == nil {
		return nil, fmt.Errorf("connection not available")
	}

	statusCmd := fmt.Sprintf("GROUP_STATUS group=%s", groupID)
	cmdBytes := util.EncodeMessage("admin", statusCmd)

	if err := util.WriteWithLength(conn, cmdBytes); err != nil {
		return nil, fmt.Errorf("send command: %w", err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return nil, fmt.Errorf("set read deadline: %w", err)
	}
	defer func() {
		if err := conn.SetReadDeadline(time.Time{}); err != nil {
			util.Error("failed to reset read deadline: %v", err)
		}
	}()

	respBuf, err := util.ReadWithLength(conn)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	respStr := strings.TrimSpace(string(respBuf))
	if strings.HasPrefix(respStr, "ERROR:") {
		return nil, fmt.Errorf("broker error: %s", respStr)
	}

	var status ConsumerGroupStatus
	if err := json.Unmarshal(respBuf, &status); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	return &status, nil
}

// RegisterConsumerGroup registers a consumer group with a topic
func (bc *BrokerClient) RegisterConsumerGroup(topic, groupName string) error {
	if err := bc.connect(); err != nil {
		return err
	}

	consumerID := fmt.Sprintf("e2e-%d", time.Now().UnixNano())
	registerCmd := fmt.Sprintf("REGISTER_GROUP topic=%s group=%s", topic, groupName)
	cmdBytes := util.EncodeMessage("", registerCmd)

	if err := util.WriteWithLength(bc.conn, cmdBytes); err != nil {
		return fmt.Errorf("send add consumer command: %w", err)
	}

	resp, err := util.ReadWithLength(bc.conn)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	respStr := strings.TrimSpace(string(resp))
	if strings.HasPrefix(respStr, "ERROR:") {
		return fmt.Errorf("broker error: %s", respStr)
	}

	bc.mu.Lock()
	bc.registered = true
	bc.topic = topic
	bc.consumerGroup = groupName
	bc.consumerID = consumerID
	bc.mu.Unlock()

	return nil
}

func (bc *BrokerClient) SendHeartbeat() error {
	bc.mu.Lock()
	if !bc.registered {
		bc.mu.Unlock()
		return fmt.Errorf("consumer not registered")
	}

	conn := bc.conn
	topic := bc.topic
	consumerGroup := bc.consumerGroup
	consumerID := bc.consumerID
	bc.mu.Unlock()

	if conn == nil {
		return fmt.Errorf("connection closed")
	}

	heartbeatCmd := fmt.Sprintf("HEARTBEAT topic=%s group=%s consumer=%s", topic, consumerGroup, consumerID)
	cmdBytes := util.EncodeMessage("", heartbeatCmd)

	if err := util.WriteWithLength(conn, cmdBytes); err != nil {
		return fmt.Errorf("send heartbeat: %w", err)
	}

	resp, err := util.ReadWithLength(conn)
	if err != nil {
		return fmt.Errorf("read heartbeat response: %w", err)
	}

	respStr := strings.TrimSpace(string(resp))
	if strings.HasPrefix(respStr, "ERROR:") {
		return fmt.Errorf("heartbeat error: %s", respStr)
	}

	return nil
}

func (bc *BrokerClient) DeleteTopic(topic string) error {
	deleteCmd := fmt.Sprintf("DELETE topic=%s", topic)
	return bc.sendCommand("admin", deleteCmd)
}

// CommitOffset commits an offset for a consumer group
func (bc *BrokerClient) CommitOffset(topic string, partition int, groupID string, offset uint64) error {
	commitCmd := fmt.Sprintf("COMMIT_OFFSET topic=%s partition=%d group=%s offset=%d", topic, partition, groupID, offset)
	return bc.sendCommand(topic, commitCmd)
}

// FetchCommittedOffset retrieves the committed offset for a consumer group
func (bc *BrokerClient) FetchCommittedOffset(topic string, partition int, groupID string) (uint64, error) {
	conn, err := net.Dial("tcp", bc.addr)
	if err != nil {
		return 0, fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()

	cmd := fmt.Sprintf("FETCH_OFFSET topic=%s partition=%d group=%s", topic, partition, groupID)
	cmdBytes := util.EncodeMessage("admin", cmd)

	if err := util.WriteWithLength(conn, cmdBytes); err != nil {
		return 0, fmt.Errorf("send command: %w", err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return 0, fmt.Errorf("set read deadline: %w", err)
	}
	defer func() {
		if err := conn.SetReadDeadline(time.Time{}); err != nil {
			util.Warn("Failed to clear read deadline: %v", err)
		}
	}()

	resp, err := util.ReadWithLength(conn)
	if err != nil {
		return 0, fmt.Errorf("read response: %w", err)
	}

	respStr := strings.TrimSpace(string(resp))
	if strings.HasPrefix(respStr, "ERROR:") {
		return 0, fmt.Errorf("broker error: %s", respStr)
	}

	var offset uint64
	if n, err := fmt.Sscanf(respStr, "%d", &offset); err != nil || n != 1 {
		return 0, fmt.Errorf("expected integer, got: %s", respStr)
	}
	return offset, nil
}

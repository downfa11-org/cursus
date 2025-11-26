package e2e

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/downfa11-org/go-broker/util"
)

// BrokerClient wraps low-level broker communication
type BrokerClient struct {
	addr string
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

// CreateTopic sends CREATE command to broker
func (bc *BrokerClient) CreateTopic(topic string, partitions int) error {
	conn, err := net.Dial("tcp", bc.addr)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()

	createCmd := fmt.Sprintf("CREATE topic=%s partitions=%d", topic, partitions)
	cmdBytes := util.EncodeMessage("admin", createCmd)

	if err := util.WriteWithLength(conn, cmdBytes); err != nil {
		return fmt.Errorf("send create command: %w", err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return fmt.Errorf("set read deadline: %w", err)
	}
	defer func() {
		if err := conn.SetReadDeadline(time.Time{}); err != nil {
			util.Warn("[WARN] Failed to clear read deadline: %v", err)
		}
	}()

	respBuf, err := util.ReadWithLength(conn)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	resp := strings.TrimSpace(string(respBuf))
	if strings.Contains(resp, "TOPIC EXISTS") || strings.Contains(resp, "already exists") {
		return nil // Topic already exists, not an error
	}
	if strings.HasPrefix(resp, "ERROR:") {
		return fmt.Errorf("broker error: %s", resp)
	}

	return nil
}

// PublishIdempotent sends a message with idempotence metadata
func (bc *BrokerClient) PublishIdempotent(topic, producerID string, seqNum uint64, epoch int64, payload, acks string) error {
	conn, err := net.Dial("tcp", bc.addr)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()

	publishCmd := fmt.Sprintf("PUBLISH topic=%s acks=%s producerId=%s seqNum=%d epoch=%d message=%s", topic, acks, producerID, seqNum, epoch, payload)
	cmdBytes := util.EncodeMessage("admin", publishCmd)

	if err := util.WriteWithLength(conn, cmdBytes); err != nil {
		return fmt.Errorf("send message: %w", err)
	}

	if acks == "0" {
		return nil
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
		return fmt.Errorf("read ack: %w", err)
	}

	resp := strings.TrimSpace(string(respBuf))
	if strings.HasPrefix(resp, "ERROR:") {
		return fmt.Errorf("broker error: %s", resp)
	}

	return nil
}

// PublishSimple sends a message without idempotence
func (bc *BrokerClient) PublishSimple(topic, payload, acks string) error {
	conn, err := net.Dial("tcp", bc.addr)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()

	publishCmd := fmt.Sprintf("PUBLISH topic=%s acks=%s message=%s", topic, acks, payload)
	cmdBytes := util.EncodeMessage("admin", publishCmd)

	if err := util.WriteWithLength(conn, cmdBytes); err != nil {
		return fmt.Errorf("send message: %w", err)
	}

	if acks == "0" {
		return nil
	}

	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return fmt.Errorf("set read deadline: %w", err)
	}
	defer func() {
		if err := conn.SetReadDeadline(time.Time{}); err != nil {
			util.Error("Failed to clear read deadline: %v", err)
		}
	}()

	respBuf, err := util.ReadWithLength(conn)
	if err != nil {
		return fmt.Errorf("read ack: %w", err)
	}

	resp := strings.TrimSpace(string(respBuf))
	if strings.HasPrefix(resp, "ERROR:") {
		return fmt.Errorf("broker error: %s", resp)
	}

	if acks == "1" && !strings.Contains(resp, "acks=1") {
		return fmt.Errorf("unexpected response: %s", resp)
	}

	return nil
}

// ConsumeMessages reads messages from a partition
func (bc *BrokerClient) ConsumeMessages(topic string, partition int, consumerGroup string, timeout time.Duration) ([]string, error) {
	conn, err := net.Dial("tcp", bc.addr)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()

	startOffset := 0
	autoOffsetReset := "earliest"

	consumeCmd := fmt.Sprintf("CONSUME topic=%s partition=%d offset=%d group=%s autoOffsetReset=%s",
		topic, partition, startOffset, consumerGroup, autoOffsetReset)
	cmdBytes := util.EncodeMessage(topic, consumeCmd)

	if err := util.WriteWithLength(conn, cmdBytes); err != nil {
		return nil, fmt.Errorf("send consume command: %w", err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return nil, fmt.Errorf("set read deadline: %w", err)
	}
	defer func() {
		if err := conn.SetReadDeadline(time.Time{}); err != nil {
			util.Warn("Failed to clear read deadline: %v", err)
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
	conn, err := net.Dial("tcp", bc.addr)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()

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
			util.Warn("Failed to clear read deadline: %v", err)
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

// CommitOffset commits an offset for a consumer group
func (bc *BrokerClient) CommitOffset(topic string, partition int, groupID string, offset uint64) error {
	conn, err := net.Dial("tcp", bc.addr)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()

	commitCmd := fmt.Sprintf("COMMIT_OFFSET topic=%s partition=%d offset=%d", topic, partition, offset)
	cmdBytes := util.EncodeMessage(topic, commitCmd)

	if err := util.WriteWithLength(conn, cmdBytes); err != nil {
		return fmt.Errorf("send command: %w", err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return fmt.Errorf("set deadline: %w", err)
	}

	resp, err := util.ReadWithLength(conn)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	defer func() {
		if err := conn.SetReadDeadline(time.Time{}); err != nil {
			util.Warn("Failed to clear read deadline: %v", err)
		}
	}()

	respStr := strings.TrimSpace(string(resp))
	if strings.HasPrefix(respStr, "ERROR:") {
		return fmt.Errorf("broker error: %s", respStr)
	}

	return nil
}

func (bc *BrokerClient) DeleteTopic(topic string) error {
	conn, err := net.Dial("tcp", bc.addr)
	if err != nil {
		return fmt.Errorf("connect failed: %w", err)
	}
	defer conn.Close()

	deleteCmd := fmt.Sprintf("DELETE topic=%s", topic)
	cmdBytes := util.EncodeMessage(topic, deleteCmd)

	if err := util.WriteWithLength(conn, cmdBytes); err != nil {
		return fmt.Errorf("send DELETE command failed: %w", err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return fmt.Errorf("set read deadline failed: %w", err)
	}
	defer func() {
		if err := conn.SetReadDeadline(time.Time{}); err != nil {
			util.Warn("Failed to clear read deadline: %v", err)
		}
	}()

	respBuf, err := util.ReadWithLength(conn)
	if err != nil {
		return fmt.Errorf("read response failed: %w", err)
	}

	resp := strings.TrimSpace(string(respBuf))
	if strings.HasPrefix(resp, "ERROR:") {
		return fmt.Errorf("broker error: %s", resp)
	}

	return nil
}

// RegisterConsumerGroup registers a consumer group with a topic
func (bc *BrokerClient) RegisterConsumerGroup(topic, groupName string, consumerCount int) error {
	conn, err := net.Dial("tcp", bc.addr)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()

	subscribeCmd := fmt.Sprintf("SUBSCRIBE topic=%s group=%s", topic, groupName)
	cmdBytes := util.EncodeMessage("admin", subscribeCmd)

	if err := util.WriteWithLength(conn, cmdBytes); err != nil {
		return fmt.Errorf("send subscribe command: %w", err)
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

	respStr := strings.TrimSpace(string(respBuf))
	if strings.HasPrefix(respStr, "ERROR:") {
		return fmt.Errorf("broker error: %s", respStr)
	}

	return nil
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

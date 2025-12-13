package e2e

import (
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/downfa11-org/go-broker/util"
)

// CreateTopic sends CREATE command to broker
func (bc *BrokerClient) CreateTopic(topic string, partitions int) error {
	createCmd := fmt.Sprintf("CREATE topic=%s partitions=%d", topic, partitions)

	err := bc.executeCommand("admin", createCmd)
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

	return bc.executeCommand("admin", publishCmd)
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

	return bc.executeCommand("admin", publishCmd)
}

// GetConsumerGroupStatus retrieves consumer group metadata from broker
func (bc *BrokerClient) GetConsumerGroupStatus(groupID string) (*ConsumerGroupStatus, error) {
	statusCmd := fmt.Sprintf("GROUP_STATUS group=%s", groupID)

	respStr, err := bc.sendCommandAndGetResponse("admin", statusCmd, 2*time.Second)
	if err != nil {
		return nil, err
	}

	var status ConsumerGroupStatus
	if err := json.Unmarshal([]byte(respStr), &status); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	return &status, nil
}

func (bc *BrokerClient) SendHeartbeat() error {
	bc.mu.Lock()
	memberID := bc.memberID
	topic := bc.topic
	consumerGroup := bc.consumerGroup
	generation := bc.generation
	bc.mu.Unlock()

	if memberID == "" {
		return fmt.Errorf("consumer not joined to group")
	}

	heartbeatCmd := fmt.Sprintf("HEARTBEAT topic=%s group=%s member=%s generation=%d", topic, consumerGroup, memberID, generation)
	return bc.executeCommand("", heartbeatCmd)
}

// DeleteTopic
func (bc *BrokerClient) DeleteTopic(topic string) error {
	deleteCmd := fmt.Sprintf("DELETE topic=%s", topic)
	return bc.executeCommand("admin", deleteCmd)
}

// CommitOffset commits an offset for a consumer group
func (bc *BrokerClient) CommitOffset(topic string, partition int, groupID string, offset uint64) error {
	bc.mu.Lock()
	generation := bc.generation
	memberID := bc.memberID
	bc.mu.Unlock()

	commitCmd := fmt.Sprintf("COMMIT_OFFSET topic=%s partition=%d group=%s offset=%d generation=%d member=%s",
		topic, partition, groupID, offset, generation, memberID)
	return bc.executeCommand(topic, commitCmd)
}

// FetchCommittedOffset retrieves the committed offset for a consumer group
func (bc *BrokerClient) FetchCommittedOffset(topic string, partition int, groupID string) (uint64, error) {
	cmd := fmt.Sprintf("FETCH_OFFSET topic=%s partition=%d group=%s", topic, partition, groupID)

	respStr, err := bc.sendCommandAndGetResponse("admin", cmd, 2*time.Second)
	if err != nil {
		return 0, err
	}

	var offset uint64
	if n, err := fmt.Sscanf(respStr, "%d", &offset); err != nil || n != 1 {
		return 0, fmt.Errorf("expected integer offset, got: %s", respStr)
	}
	return offset, nil
}

// joinGroup executes the JOIN_GROUP command and extracts generation and memberID.
func (bc *BrokerClient) joinGroup(topic, group string) (int, string, error) {
	bc.mu.Lock()
	if bc.memberID == "" {
		bc.memberID = fmt.Sprintf("e2e-consumer-%d", time.Now().UnixNano())
	}
	initialMemberID := bc.memberID
	bc.mu.Unlock()

	joinCmd := fmt.Sprintf("JOIN_GROUP topic=%s group=%s member=%s", topic, group, initialMemberID)

	resp, err := bc.sendCommandAndGetResponse("", joinCmd, 2*time.Second)
	if err != nil {
		return 0, "", fmt.Errorf("join group failed: %w", err)
	}

	if strings.HasPrefix(resp, "ERROR:") {
		return 0, "", fmt.Errorf("broker error: %s", resp)
	}

	var gen int
	var newMemberID string

	if strings.Contains(resp, "generation=") {
		parts := strings.Fields(resp)
		for _, part := range parts {
			if strings.HasPrefix(part, "generation=") {
				if n, scanErr := fmt.Sscanf(part, "generation=%d", &gen); scanErr != nil || n != 1 {
					util.Warn("JOIN_GROUP response did not contain valid generation info: %s", resp)
				}
			}
			if strings.HasPrefix(part, "member=") {
				// member=e2e-consumer-1765285832171409200-6241
				newMemberID = strings.TrimPrefix(part, "member=")
				if newMemberID == "" {
					util.Warn("JOIN_GROUP response contained empty member info: %s", resp)
				}
			}
		}
	}

	if newMemberID == "" {
		newMemberID = initialMemberID
	}

	bc.mu.Lock()
	bc.generation = gen
	bc.memberID = newMemberID
	bc.topic = topic
	bc.consumerGroup = group
	bc.mu.Unlock()

	return gen, newMemberID, nil
}

// syncGroup executes the SYNC_GROUP command to finalize partition assignment.
func (bc *BrokerClient) syncGroup(topic, group string, generation int, memberID string) ([]int, error) {
	syncCmd := fmt.Sprintf("SYNC_GROUP topic=%s group=%s member=%s generation=%d", topic, group, memberID, generation)

	resp, err := bc.sendCommandAndGetResponse("", syncCmd, 2*time.Second)
	if err != nil {
		return nil, fmt.Errorf("sync group failed: %w", err)
	}

	if strings.HasPrefix(resp, "ERROR:") {
		return nil, fmt.Errorf("broker error: %s", resp)
	}

	assignedPartitions := []int{}

	const assignmentPrefix = "assignments="
	assignmentIndex := strings.Index(resp, assignmentPrefix)

	if assignmentIndex != -1 {
		assignmentsStr := resp[assignmentIndex+len(assignmentPrefix):]
		start := strings.Index(assignmentsStr, "[")
		end := strings.Index(assignmentsStr, "]")

		if start != -1 && end != -1 && end > start {
			listStr := assignmentsStr[start+1 : end]
			listStr = strings.ReplaceAll(listStr, ",", " ")
			parts := strings.Fields(listStr)

			for _, pStr := range parts {
				pStr = strings.TrimSpace(pStr)
				if pStr != "" {
					var partitionID int
					if _, scanErr := fmt.Sscanf(pStr, "%d", &partitionID); scanErr == nil {
						assignedPartitions = append(assignedPartitions, partitionID)
					} else {
						return nil, fmt.Errorf("invalid partition ID format '%s': %w", pStr, scanErr)
					}
				}
			}
		}
	}

	return assignedPartitions, nil
}

// ConsumeMessages reads messages from a partition
func (bc *BrokerClient) ConsumeMessages(topic string, partition int, consumerGroup string, memberID string, generation int, timeout time.Duration) ([]string, error) {
	if err := bc.connect(); err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	bc.mu.Lock()
	conn := bc.conn
	bc.mu.Unlock()

	if conn == nil {
		return nil, fmt.Errorf("connection not available after connect")
	}

	startOffset := 0
	consumeCmd := fmt.Sprintf("CONSUME topic=%s partition=%d offset=%d group=%s autoOffsetReset=earliest member=%s generation=%d",
		topic, partition, startOffset, consumerGroup, memberID, generation)
	cmdBytes := util.EncodeMessage(topic, consumeCmd)

	if err := util.WriteWithLength(conn, cmdBytes); err != nil {
		if resetErr := conn.SetReadDeadline(time.Time{}); resetErr != nil {
			util.Warn("failed to reset read deadline after send failure: %v", resetErr)
		}
		return nil, fmt.Errorf("send consume command: %w", err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return nil, fmt.Errorf("set read deadline: %w", err)
	}

	batchBytes, err := util.ReadWithLength(conn)

	if resetErr := conn.SetReadDeadline(time.Time{}); resetErr != nil {
		util.Warn("failed to reset read deadline: %v", resetErr)
	}

	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return []string{}, nil
		}
		return nil, fmt.Errorf("read batch message: %w", err)
	}

	if len(batchBytes) == 0 {
		return []string{}, nil
	}

	respStr := strings.TrimSpace(string(batchBytes))
	if strings.HasPrefix(respStr, "ERROR:") {
		return nil, fmt.Errorf("broker error during consume: %s", respStr)
	}

	batch, err := util.DecodeBatchMessages(batchBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to decode batch: %w", err)
	}

	var messages []string
	for _, msg := range batch.Messages {
		messages = append(messages, msg.Payload)
	}

	return messages, nil
}

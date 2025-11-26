package bench

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/downfa11-org/go-broker/pkg/server"
	"github.com/downfa11-org/go-broker/util"
)

const AckTimeout = 5 * time.Second

type BenchClient struct {
	Addr        string
	EnableGzip  bool
	NumMessages int
	Topic       string
	Partitions  int

	MessageSize int
	BatchSize   int
	LingerMS    int
	MaxInflight int
	UseAsync    bool
}

func (c *BenchClient) RunTopicCreationPhase() error {
	conn, err := net.Dial("tcp", c.Addr)
	if err != nil {
		return fmt.Errorf("connect to broker for topic creation: %w", err)
	}
	defer conn.Close()

	createPayload := fmt.Sprintf("CREATE topic=%s partitions=%d", c.Topic, c.Partitions)
	createPayload = strings.TrimSpace(createPayload)

	if err := c.sendCommand(conn, c.Topic, createPayload); err != nil {
		if strings.Contains(err.Error(), "topic exists") {
			util.Info("Topic '%s' already exists, continuing...", c.Topic)
			return nil
		}
		return fmt.Errorf("topic creation failed: %w", err)
	}

	return nil
}

// sendCommand sends an encoded topic/payload command and validates the response.
func (c *BenchClient) sendCommand(conn net.Conn, topic, payload string) error {
	cmdBytes := util.EncodeMessage(topic, payload)
	if err := util.WriteWithLength(conn, cmdBytes); err != nil {
		return fmt.Errorf("send command: %w", err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(AckTimeout)); err != nil {
		return fmt.Errorf("set read deadline: %w", err)
	}
	defer func() {
		_ = conn.SetReadDeadline(time.Time{})
	}()

	respBuf, err := util.ReadWithLength(conn)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	resp := strings.TrimSpace(string(respBuf))
	if resp == "" {
		return fmt.Errorf("empty response from broker")
	}

	if strings.HasPrefix(resp, "ERROR:") {
		return fmt.Errorf("broker error: %s", resp)
	}

	if !strings.Contains(resp, "now has") && !strings.Contains(resp, "already exists") {
		return fmt.Errorf("unexpected response: %s", resp)
	}

	return nil
}

// sendMessagesToPartition sends benchmark messages for a specific partition.
func (c *BenchClient) sendMessagesToPartition(producerID int, partitionID int, count int) error {
	conn, err := net.Dial("tcp", c.Addr)
	if err != nil {
		return fmt.Errorf("[P%d/Part%d] connection failed: %v", producerID, partitionID, err)
	}
	defer conn.Close()

	for i := 0; i < count; i++ {
		payload := fmt.Sprintf("bench-msg-P%d-Part%d-Msg%d", producerID, partitionID, i)
		data := util.EncodeMessage(c.Topic, payload)

		msgBytes, err := server.CompressMessage(data, c.EnableGzip)
		if err != nil {
			return fmt.Errorf("[P%d/Part%d] compress failed: %v", producerID, partitionID, err)
		}

		if err := util.WriteWithLength(conn, msgBytes); err != nil {
			return fmt.Errorf("[P%d/Part%d] send failed: %v", producerID, partitionID, err)
		}

		if err := conn.SetReadDeadline(time.Now().Add(AckTimeout)); err != nil {
			return fmt.Errorf("[P%d/Part%d] set read deadline failed: %v", producerID, partitionID, err)
		}

		resp, err := util.ReadWithLength(conn)
		if err != nil {
			return fmt.Errorf("[Producer %d | Partition %d | Message %d] Read response failed:\n  %v",
				producerID, partitionID, i, err)
		}

		respStr := strings.TrimSpace(string(resp))
		if len(respStr) == 0 {
			return fmt.Errorf("[P%d/Part%d] empty response — possible connection issue", producerID, partitionID)
		}
		if strings.HasPrefix(respStr, "ERROR:") {
			return fmt.Errorf("[P%d/Part%d] broker error: %s", producerID, partitionID, respStr)
		}
		if respStr != "OK" {
			return fmt.Errorf("[P%d/Part%d] unexpected response: %s", producerID, partitionID, respStr)
		}

		_ = conn.SetReadDeadline(time.Time{})
	}
	return nil
}

func (c *BenchClient) RunMessageProductionPhase(producerID int) error {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error

	msgsPerPartition := c.NumMessages / c.Partitions
	remainder := c.NumMessages % c.Partitions

	for p := 0; p < c.Partitions; p++ {
		messagesToSend := msgsPerPartition
		if p < remainder {
			messagesToSend++
		}

		if messagesToSend > 0 {
			wg.Add(1)
			go func(partitionID, count int) {
				defer wg.Done()
				if err := c.sendMessagesToPartition(producerID, partitionID, count); err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
				}
			}(p, messagesToSend)
		}
	}

	wg.Wait()

	if len(errs) > 0 {
		return fmt.Errorf("%d partition(s) failed, error: %w", len(errs), errs[0])
	}
	return nil
}

// consumeMessagesFromPartition connects and sends a CONSUME command, then reads all expected messages.
func (c *BenchClient) consumeMessagesFromPartition(cid, partitionID, count int, wg *sync.WaitGroup) {
	defer wg.Done()

	conn, err := net.Dial("tcp", c.Addr)
	if err != nil {
		util.Error("[C%d] connection failed: %v", cid, err)
		return
	}
	defer conn.Close()

	startOffset := 0

	consumeCmd := fmt.Sprintf("CONSUME topic=%s partition=%d offset=%d group=- autoOffsetReset=earliest", c.Topic, partitionID, startOffset)
	consumeCmd = strings.TrimSpace(consumeCmd)
	cmdBytes := util.EncodeMessage(c.Topic, consumeCmd)

	if err := util.WriteWithLength(conn, cmdBytes); err != nil {
		util.Error("send command failed in Consumer-%d: %v", cid, err)
		return
	}

	if err := conn.SetReadDeadline(time.Now().Add(AckTimeout * 2)); err != nil {
		util.Error("set read deadline failed in Consumer-%d: %v. Continuing...", cid, err)
	}

	consumedCount := 0
	for i := 0; i < count; i++ {
		msgBytes, err := util.ReadWithLength(conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			util.Error("Read message %d failed in Consumer-%d: %v", i, cid, err)
			return
		}

		if err := conn.SetReadDeadline(time.Now().Add(AckTimeout * 2)); err != nil {
			util.Error("Warning: Failed to reset read deadline in Consumer-%d: %v", cid, err)
		}

		if len(msgBytes) == 0 {
			continue
		}

		payload := string(msgBytes)
		if !strings.HasPrefix(payload, "bench-msg-P") ||
			!strings.Contains(payload, "-Part") ||
			!strings.Contains(payload, "-Msg") {
			util.Warn("Invalid message format in Consumer-%d: %s", cid, payload)
		}

		consumedCount++
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		util.Error("Warning: Failed to clear read deadline: %v", err)
	}

	util.Debug("Consumer%d finished reading %d/%d messages.", cid, consumedCount, count)
}

// RunConsumerPhase executes the consumption benchmark workflow.
func (c *BenchClient) RunConsumerPhase(numConsumers int) error {
	var wg sync.WaitGroup

	total := c.NumMessages
	msgsPerConsumer := total / c.Partitions
	remainder := total % c.Partitions

	for partitionID := 0; partitionID < c.Partitions; partitionID++ {
		count := msgsPerConsumer
		if partitionID < remainder {
			count++
		}
		if count == 0 {
			continue
		}
		wg.Add(1)
		consumerID := partitionID % numConsumers
		go c.consumeMessagesFromPartition(consumerID, partitionID, count, &wg)
	}

	wg.Wait()
	return nil
}

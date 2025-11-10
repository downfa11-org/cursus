package bench

import (
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/downfa11-org/go-broker/pkg/server"
	"github.com/downfa11-org/go-broker/util"
)

type BenchClient struct {
	Addr        string
	EnableGzip  bool
	NumMessages int
	Topic       string
	Partitions  int
}

// sendCommand sends an encoded topic/payload command and validates the response.
func (c *BenchClient) sendCommand(conn net.Conn, topic, payload string) error {
	cmdBytes := util.EncodeMessage(topic, payload)
	if err := util.WriteWithLength(conn, cmdBytes); err != nil {
		return fmt.Errorf("send command: %w", err)
	}

	respBuf, err := util.ReadWithLength(conn)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	_, respPayload := util.DecodeMessage(respBuf)
	resp := strings.TrimSpace(respPayload)

	upperResp := strings.ToUpper(resp)

	if !(strings.Contains(upperResp, "OK") ||
		strings.Contains(upperResp, "TOPIC") ||
		strings.Contains(resp, "✅") ||
		strings.Contains(upperResp, "CREATED") ||
		strings.Contains(upperResp, "EXISTS")) {
		return fmt.Errorf("unexpected response: %s", resp)
	}
	return nil
}

// sendMessagesToPartition sends benchmark messages for a specific partition.
func (c *BenchClient) sendMessagesToPartition(pid, count int, wg *sync.WaitGroup) {
	defer wg.Done()

	conn, err := net.Dial("tcp", c.Addr)
	if err != nil {
		fmt.Printf("[P%d] connection failed: %v\n", pid, err)
		return
	}
	defer conn.Close()

	for i := 0; i < count; i++ {
		payload := fmt.Sprintf("bench-msg-%d-%d", pid, i)
		data := util.EncodeMessage(c.Topic, payload)

		msgBytes, err := server.CompressMessage(data, c.EnableGzip)
		if err != nil {
			fmt.Printf("[P%d] compress failed: %v\n", pid, err)
			return
		}

		if err := util.WriteWithLength(conn, msgBytes); err != nil {
			fmt.Printf("[P%d] send failed: %v\n", pid, err)
			return
		}

		if _, err := util.ReadWithLength(conn); err != nil {
			fmt.Printf("[P%d] read resp failed: %v\n", pid, err)
			return
		}
	}
}

// Run executes the full benchmark workflow.
func (c *BenchClient) Run() error {
	conn, err := net.Dial("tcp", c.Addr)
	if err != nil {
		return fmt.Errorf("connect to broker: %w", err)
	}
	defer conn.Close()

	createPayload := fmt.Sprintf("CREATE %s %d", c.Topic, c.Partitions)
	if err := c.sendCommand(conn, c.Topic, createPayload); err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("topic creation failed: %w", err)
		}
	}

	var wg sync.WaitGroup
	msgsPerPartition := c.NumMessages / c.Partitions

	for p := 0; p < c.Partitions; p++ {
		wg.Add(1)
		go c.sendMessagesToPartition(p, msgsPerPartition, &wg)
	}

	wg.Wait()
	return nil
}

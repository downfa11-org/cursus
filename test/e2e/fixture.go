package e2e

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/downfa11-org/go-broker/util"
	"github.com/google/uuid"
)

const (
	e2eComposeFile      = "test/e2e/docker-compose.yml"
	healthCheckURL      = "http://localhost:9080/health"
	defaultBrokerAddr   = "localhost:9000"
	healthCheckRetries  = 30
	healthCheckInterval = 1 * time.Second
)

type Context struct {
	t              *testing.T
	brokerAddr     string
	topic          string
	partitions     int
	numMessages    int
	publishDelayMS int
	startTime      time.Time
	publishedCount int
	consumedCount  int
	consumerGroup  string

	producerID       string
	seqNum           uint64
	conn             net.Conn
	publishedSeqNums []uint64
}

type Actions struct {
	ctx *Context
}

type Consequences struct {
	ctx *Context
}

func Given(t *testing.T) *Context {
	return &Context{
		t:              t,
		brokerAddr:     defaultBrokerAddr,
		topic:          "test-topic",
		partitions:     1,
		numMessages:    10,
		publishDelayMS: 100,
		startTime:      time.Now(),
		consumerGroup:  fmt.Sprintf("test-group-%d", time.Now().UnixNano()),
		producerID:     uuid.New().String(),
		seqNum:         0,
	}
}

func (c *Context) WithTopic(topic string) *Context {
	c.topic = topic
	return c
}

func (c *Context) WithPartitions(n int) *Context {
	c.partitions = n
	return c
}

func (c *Context) WithNumMessages(n int) *Context {
	c.numMessages = n
	return c
}

func (c *Context) WithConsumerGroup(group string) *Context {
	c.consumerGroup = group
	return c
}

func (c *Context) WithDefaultConsumerGroup() *Context {
	c.consumerGroup = "default-group"
	return c
}

func (c *Context) When() *Actions {
	return &Actions{ctx: c}
}

func (a *Actions) StartBroker() *Actions {
	a.ctx.t.Log("Verifying broker is already running...")

	if err := waitForHealth(); err != nil {
		a.ctx.t.Fatalf("Broker is not healthy: %v", err)
	}

	a.ctx.t.Log("Broker is healthy and ready")

	if err := a.createTopic(); err != nil {
		a.ctx.t.Fatalf("Failed to create topic: %v", err)
	}

	return a
}

func (a *Actions) createTopic() error {
	conn, err := net.Dial("tcp", a.ctx.brokerAddr)
	if err != nil {
		return fmt.Errorf("connect to broker: %w", err)
	}
	defer conn.Close()

	createCmd := fmt.Sprintf("CREATE %s %d", a.ctx.topic, a.ctx.partitions)
	cmdBytes := util.EncodeMessage("admin", createCmd)

	if err := util.WriteWithLength(conn, cmdBytes); err != nil {
		return fmt.Errorf("send CREATE command: %w", err)
	}

	respBytes, err := util.ReadWithLength(conn)
	if err != nil {
		return fmt.Errorf("read CREATE response: %w", err)
	}

	resp := string(respBytes)
	if strings.HasPrefix(resp, "ERROR:") {
		return fmt.Errorf("create topic failed: %s", resp)
	}

	a.ctx.t.Logf("Topic '%s' created with %d partitions", a.ctx.topic, a.ctx.partitions)
	return nil
}

func waitForHealth() error {
	for i := 0; i < healthCheckRetries; i++ {
		resp, err := http.Get(healthCheckURL)
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return nil
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(healthCheckInterval)
	}
	return fmt.Errorf("broker never became healthy")
}

func (a *Actions) StopBroker() *Actions {
	cmd := exec.Command("docker", "compose", "-f", e2eComposeFile, "stop", "broker")
	if err := cmd.Run(); err != nil {
		a.ctx.t.Logf("Warning: Failed to stop broker: %v", err)
	}
	time.Sleep(1 * time.Second)
	return a
}

func (c *Context) Cleanup() {
	c.t.Log("Cleanup will be handled by Makefile")

	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}

func (a *Actions) PublishMessages() *Actions {
	a.ctx.t.Logf("Publishing %d messages to topic '%s'...", a.ctx.numMessages, a.ctx.topic)

	if a.ctx.conn == nil {
		conn, err := net.Dial("tcp", a.ctx.brokerAddr)
		if err != nil {
			a.ctx.t.Fatalf("Failed to connect: %v", err)
		}
		a.ctx.conn = conn
	}

	timestamp := time.Now().UnixNano()
	a.ctx.publishedSeqNums = make([]uint64, 0, a.ctx.numMessages)
	for i := 0; i < a.ctx.numMessages; i++ {
		a.ctx.seqNum++
		a.ctx.publishedSeqNums = append(a.ctx.publishedSeqNums, a.ctx.seqNum)

		payload := fmt.Sprintf("test-message-%d-%d", i, timestamp)
		publishCmd := fmt.Sprintf("PUBLISH %s %s", a.ctx.topic, payload)
		cmdBytes := util.EncodeMessage(a.ctx.topic, publishCmd)

		if err := util.WriteWithLength(a.ctx.conn, cmdBytes); err != nil {
			a.ctx.t.Errorf("Failed to send message %d: %v", i, err)
			continue
		}

		respBytes, err := util.ReadWithLength(a.ctx.conn)
		if err != nil {
			a.ctx.t.Errorf("Failed to read ack for message %d: %v", i, err)
			continue
		}

		resp := strings.TrimSpace(string(respBytes))
		if !strings.Contains(resp, "Published") {
			a.ctx.t.Errorf("Unexpected response for message %d: %s", i, resp)
			continue
		}

		a.ctx.publishedCount++
		time.Sleep(time.Duration(a.ctx.publishDelayMS) * time.Millisecond)
	}

	a.ctx.t.Logf("Published %d/%d messages successfully", a.ctx.publishedCount, a.ctx.numMessages)
	time.Sleep(1 * time.Second) // disk_flush wait

	return a
}

func (a *Actions) RetryPublishMessages() *Actions {
	a.ctx.t.Log("Retrying messages with same ProducerID + SeqNum...")

	conn, err := net.Dial("tcp", a.ctx.brokerAddr)
	if err != nil {
		a.ctx.t.Fatalf("Failed to reconnect: %v", err)
	}
	a.ctx.conn = conn

	for i := 0; i < a.ctx.numMessages; i++ {
		a.ctx.seqNum++

		payload := fmt.Sprintf("test-message-%d", i)
		seqNum := a.ctx.publishedSeqNums[i]

		cmdBytes := util.EncodeIdempotentMessage(
			a.ctx.topic,
			payload,
			a.ctx.producerID,
			seqNum,
			time.Now().UnixNano(),
		)

		if err := util.WriteWithLength(a.ctx.conn, cmdBytes); err != nil {
			a.ctx.t.Fatalf("Failed to retry message %d: %v", i, err)
		}

		respBytes, err := util.ReadWithLength(a.ctx.conn)
		if err != nil {
			a.ctx.t.Errorf("Failed to read retry response for message %d: %v", i, err)
			continue
		}

		resp := strings.TrimSpace(string(respBytes))
		a.ctx.t.Logf("Retry response for message %d: %s", i, resp)
	}

	time.Sleep(1 * time.Second)
	return a
}

func (a *Actions) ConsumeMessages() *Actions {
	a.ctx.t.Logf("Consuming messages from topic '%s' (all partitions)...", a.ctx.topic)

	totalConsumed := 0
	for partition := 0; partition < a.ctx.partitions; partition++ {
		conn, err := net.Dial("tcp", a.ctx.brokerAddr)
		if err != nil {
			a.ctx.t.Errorf("Failed to connect for partition %d: %v", partition, err)
			continue
		}

		// setgroup
		setGroupCmd := fmt.Sprintf("SETGROUP %s", a.ctx.consumerGroup)
		setGroupBytes := util.EncodeMessage(a.ctx.topic, setGroupCmd)

		if err := util.WriteWithLength(conn, setGroupBytes); err != nil {
			a.ctx.t.Errorf("Failed to send SETGROUP for partition %d: %v", partition, err)
			conn.Close()
			continue
		}

		setGroupResp, err := util.ReadWithLength(conn)
		if err != nil {
			a.ctx.t.Errorf("Failed to read SETGROUP response for partition %d: %v", partition, err)
			conn.Close()
			continue
		}

		respStr := string(setGroupResp)
		if strings.HasPrefix(respStr, "ERROR:") {
			a.ctx.t.Errorf("SETGROUP failed for partition %d: %s", partition, respStr)
			conn.Close()
			continue
		}
		a.ctx.t.Logf("SETGROUP successful for partition %d: %s", partition, respStr)

		// consume
		consumeCmd := fmt.Sprintf("CONSUME %s %d 0", a.ctx.topic, partition)
		cmdBytes := util.EncodeMessage(a.ctx.topic, consumeCmd)

		if err := util.WriteWithLength(conn, cmdBytes); err != nil {
			a.ctx.t.Errorf("Failed to send CONSUME command for partition %d: %v", partition, err)
			conn.Close()
			continue
		}

		err = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		if err != nil {
			a.ctx.t.Errorf("Failed to set read deadline for partition %d: %v", partition, err)
		}

		partitionCount := 0
		for {
			msgBytes, err := util.ReadWithLength(conn)
			if err != nil {
				if err == io.EOF {
					break
				}
				a.ctx.t.Logf("Read error on partition %d: %v", partition, err)
				break
			}

			if len(msgBytes) > 0 {
				partitionCount++
				totalConsumed++
			}

			err = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			if err != nil {
				a.ctx.t.Errorf("Failed to set read deadline for partition %d: %v", partition, err)
			}
		}

		a.ctx.t.Logf("Consumed %d messages from partition %d", partitionCount, partition)
		conn.Close()
	}

	a.ctx.consumedCount = totalConsumed
	a.ctx.t.Logf("Total consumed: %d messages", totalConsumed)
	return a
}

func (a *Actions) Then() *Consequences {
	return &Consequences{ctx: a.ctx}
}

func (c *Consequences) Expect(expectation Expectation) *Consequences {
	if err := expectation(c.ctx); err != nil {
		c.ctx.t.Errorf("Expectation failed: %v", err)
	}
	return c
}

func (c *Consequences) And(expectation Expectation) *Consequences {
	return c.Expect(expectation)
}

type Expectation func(*Context) error

// SimulateNetworkFailure simulates a network failure by closing the connection
func (a *Actions) SimulateNetworkFailure() *Actions {
	a.ctx.t.Log("Simulating network failure...")

	if a.ctx.conn != nil {
		if err := a.ctx.conn.Close(); err != nil {
			a.ctx.t.Logf("Warning: Failed to close connection: %v", err)
		}
		a.ctx.conn = nil
	}

	// network recovery simulation
	time.Sleep(100 * time.Millisecond)

	a.ctx.t.Log("Network failure simulated")
	return a
}

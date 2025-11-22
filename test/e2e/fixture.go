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
)

const (
	e2eComposeFile      = "test/docker-compose.yml"
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
	time.Sleep(2 * time.Second)
	return a
}

func (c *Context) Cleanup() {
	c.t.Log("Cleanup will be handled by Makefile")
}

func (a *Actions) PublishMessages() *Actions {
	a.ctx.t.Logf("Publishing %d messages to topic '%s'...", a.ctx.numMessages, a.ctx.topic)

	for i := 0; i < a.ctx.numMessages; i++ {
		conn, err := net.Dial("tcp", a.ctx.brokerAddr)
		if err != nil {
			a.ctx.t.Errorf("Failed to connect for message %d: %v", i, err)
			continue
		}

		payload := fmt.Sprintf("test-message-%d", i)
		publishCmd := fmt.Sprintf("PUBLISH %s %s", a.ctx.topic, payload)
		cmdBytes := util.EncodeMessage(a.ctx.topic, publishCmd)

		if err := util.WriteWithLength(conn, cmdBytes); err != nil {
			a.ctx.t.Errorf("Failed to send message %d: %v", i, err)
			conn.Close()
			continue
		}

		respBytes, err := util.ReadWithLength(conn)
		if err != nil {
			a.ctx.t.Errorf("Failed to read ack for message %d: %v", i, err)
			conn.Close()
			continue
		}

		resp := string(respBytes)
		if strings.HasPrefix(resp, "ERROR:") {
			a.ctx.t.Errorf("Publish failed for message %d: %s", i, resp)
		} else {
			a.ctx.publishedCount++
		}

		conn.Close()

		if a.ctx.publishDelayMS > 0 {
			time.Sleep(time.Duration(a.ctx.publishDelayMS) * time.Millisecond)
		}
	}

	a.ctx.t.Logf("Published %d/%d messages successfully", a.ctx.publishedCount, a.ctx.numMessages)
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

		consumeCmd := fmt.Sprintf("CONSUME %s %d 0", a.ctx.topic, partition)
		cmdBytes := util.EncodeMessage(a.ctx.topic, consumeCmd)

		if err := util.WriteWithLength(conn, cmdBytes); err != nil {
			a.ctx.t.Errorf("Failed to send CONSUME command for partition %d: %v", partition, err)
			conn.Close()
			continue
		}

		conn.SetReadDeadline(time.Now().Add(5 * time.Second))

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

			conn.SetReadDeadline(time.Now().Add(5 * time.Second))
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

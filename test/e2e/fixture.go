package e2e

import (
	"fmt"
	"net/http"
	"os/exec"
	"testing"
	"time"
)

// Context holds test environment configuration (Given phase)
type Context struct {
	t              *testing.T
	brokerAddr     string
	topic          string
	partitions     int
	numMessages    int
	publishDelayMS int
}

// Actions performs test actions (When phase)
type Actions struct {
	ctx *Context
}

// Consequences verifies test results (Then phase)
type Consequences struct {
	ctx *Context
}

// Given creates a new test context with default values
func Given(t *testing.T) *Context {
	return &Context{
		t:              t,
		brokerAddr:     "localhost:9000",
		topic:          "test-topic",
		partitions:     4,
		numMessages:    10,
		publishDelayMS: 100,
	}
}

// WithTopic sets the topic name for the test
func (c *Context) WithTopic(topic string) *Context {
	c.topic = topic
	return c
}

// WithPartitions sets the number of partitions for the test
func (c *Context) WithPartitions(n int) *Context {
	c.partitions = n
	return c
}

// WithNumMessages sets the number of messages to publish
func (c *Context) WithNumMessages(n int) *Context {
	c.numMessages = n
	return c
}

// When transitions to the action phase
func (c *Context) When() *Actions {
	return &Actions{ctx: c}
}

// StartBroker verifies that the broker is already running (started by Makefile)
func (a *Actions) StartBroker() *Actions {
	a.ctx.t.Log("Verifying broker is already running...")

	if err := waitForHealth(); err != nil {
		a.ctx.t.Fatalf("Broker is not healthy: %v", err)
	}

	a.ctx.t.Log("Broker is healthy and ready")
	return a
}

func waitForHealth() error {
	for i := 0; i < 30; i++ {
		resp, err := http.Get("http://localhost:9080/health")
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return nil
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("broker never became healthy")
}

// StopBroker stops the broker for retry testing
func (a *Actions) StopBroker() *Actions {
	cmd := exec.Command("docker", "compose", "-f", "test/docker-compose.yml", "stop", "broker")
	if err := cmd.Run(); err != nil {
		a.ctx.t.Logf("Warning: Failed to stop broker: %v", err)
	}
	time.Sleep(2 * time.Second)
	return a
}

// Cleanup is now a no-op since Makefile handles cleanup
func (c *Context) Cleanup() {
	// Makefile의 e2e-clean이 모든 정리를 담당
	c.t.Log("Cleanup will be handled by Makefile")
}

// PublishMessages waits for publisher to complete message publishing
func (a *Actions) PublishMessages() *Actions {
	time.Sleep(15 * time.Second)
	return a
}

// ConsumeMessages waits for consumer to receive messages
func (a *Actions) ConsumeMessages() *Actions {
	time.Sleep(5 * time.Second)
	return a
}

// Then transitions to the verification phase
func (a *Actions) Then() *Consequences {
	return &Consequences{ctx: a.ctx}
}

// Expect verifies an expectation and chains to next verification
func (c *Consequences) Expect(expectation Expectation) *Consequences {
	if err := expectation(c.ctx); err != nil {
		c.ctx.t.Errorf("Expectation failed: %v", err)
	}
	return c
}

// And chains additional expectations for verification
func (c *Consequences) And(expectation Expectation) *Consequences {
	return c.Expect(expectation)
}

// Expectation is a function type for test verification logic
type Expectation func(*Context) error

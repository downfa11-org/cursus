package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

const defaultBrokerAddr = "localhost:9000"

// TestContext holds all test state and configuration
type TestContext struct {
	t              *testing.T
	brokerAddr     string
	topic          string
	partitions     int
	numMessages    int
	publishDelayMS int

	// Test state
	startTime      time.Time
	publishedCount int
	consumedCount  int
	consumerGroup  string

	// Producer state
	producerID       string
	seqNum           uint64
	publishedSeqNums []uint64
	acks             string

	// Consumer state
	memberID           string
	generation         int
	assignedPartitions []int

	// Client helper
	client *BrokerClient
}

// Given creates a new test context with default values
func Given(t *testing.T) *TestContext {
	return &TestContext{
		t:              t,
		brokerAddr:     defaultBrokerAddr,
		topic:          "test-topic",
		partitions:     1,
		numMessages:    10,
		publishDelayMS: 100,
		startTime:      time.Now(),
		consumerGroup:  fmt.Sprintf("test-group-%d", time.Now().UnixNano()),

		memberID:   fmt.Sprintf("e2e-consumer-%d", time.Now().UnixNano()),
		generation: 0,

		producerID: uuid.New().String(),
		seqNum:     0,
		acks:       "1",
		client:     nil, // lazily initialized
	}
}

func (ctx *TestContext) getClient() *BrokerClient {
	if ctx.client == nil {
		ctx.client = NewBrokerClient(ctx.brokerAddr)

		ctx.client.mu.Lock()
		ctx.client.memberID = ctx.memberID
		ctx.client.mu.Unlock()
	}

	return ctx.client
}

// Configuration methods (fluent interface)
func (ctx *TestContext) WithTopic(topic string) *TestContext {
	ctx.topic = topic
	return ctx
}

func (ctx *TestContext) WithPartitions(partitions int) *TestContext {
	ctx.partitions = partitions
	return ctx
}

func (ctx *TestContext) WithNumMessages(num int) *TestContext {
	ctx.numMessages = num
	return ctx
}

func (ctx *TestContext) WithPublishDelay(delayMS int) *TestContext {
	ctx.publishDelayMS = delayMS
	return ctx
}

func (ctx *TestContext) WithAcks(acks string) *TestContext {
	ctx.acks = acks
	return ctx
}

func (ctx *TestContext) WithConsumerGroup(group string) *TestContext {
	ctx.consumerGroup = group
	return ctx
}

func (ctx *TestContext) WithDefaultConsumerGroup() *TestContext {
	ctx.t.Log("Warning: Using default-group may cause test isolation issues")
	ctx.consumerGroup = "default-group"
	return ctx
}

// When returns Actions for test execution
func (ctx *TestContext) When() *Actions {
	return &Actions{ctx: ctx}
}

// Then returns Consequences for assertions
func (ctx *TestContext) Then() *Consequences {
	return &Consequences{ctx: ctx}
}

// Cleanup stops broker and cleans up resources
func (ctx *TestContext) Cleanup() {
	ctx.t.Log("Cleaning up test resources...")

	if ctx.topic != "" {
		if err := ctx.getClient().DeleteTopic(ctx.topic); err != nil {
			if !strings.Contains(err.Error(), "not found") {
				ctx.t.Logf("Failed to delete topic %s: %v", ctx.topic, err)
			}
		} else {
			ctx.t.Logf("Topic %s deleted", ctx.topic)
		}
	}

	if ctx.client != nil {
		ctx.client.Close()
	}

	time.Sleep(1 * time.Second)
}

// SyncClientState updates the TestContext with the latest group state from the BrokerClient
func (ctx *TestContext) SyncClientState(client *BrokerClient) {
	client.mu.Lock()
	defer client.mu.Unlock()

	if client.memberID != "" && client.memberID != ctx.memberID {
		ctx.t.Logf("Context updated: memberID changed from '%s' to '%s'", ctx.memberID, client.memberID)
		ctx.memberID = client.memberID
		ctx.generation = client.generation
	}
}

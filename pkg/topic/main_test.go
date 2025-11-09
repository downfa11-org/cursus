package topic

import (
	"testing"
	"time"

	"github.com/downfa11-org/go-broker/pkg/config"
	"github.com/downfa11-org/go-broker/pkg/disk"
	"github.com/downfa11-org/go-broker/pkg/types"
)

func TestTopicManager(t *testing.T) {
	cfg := &config.Config{CleanupInterval: 1}
	dm := disk.NewDiskManager(cfg.LogDir, cfg.BufferSize)
	tm := NewTopicManager(cfg, dm)
	defer tm.Stop()

	topicName := "test-topic"
	groupName := "test-group"

	t1 := tm.CreateTopic(topicName, 4)
	t2 := tm.GetTopic(topicName)
	if t1 != t2 {
		t.Errorf("CreateTopic/GetTopic returned different instances")
	}

	tm.RegisterConsumerGroup(topicName, groupName, 1)
	consumerCh := tm.Consume(topicName, groupName, 0)
	if consumerCh == nil {
		t.Fatalf("Consume failed: consumer channel is nil")
	}

	msg1 := types.Message{Payload: "unique-message-1"}
	tm.Publish(topicName, msg1)

	select {
	case receivedMsg := <-consumerCh:
		if string(receivedMsg.Payload) != "unique-message-1" {
			t.Errorf("expected payload 'unique-message-1', got %s", receivedMsg.Payload)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("first publish failed: timed out waiting for message reception")
	}

	tm.Publish(topicName, msg1)

	select {
	case receivedMsg := <-consumerCh:
		t.Errorf("duplicate message inserted: received message %v", receivedMsg)
	case <-time.After(50 * time.Millisecond):
		t.Logf("successful: did not receive duplicate message.")
	}
	tm.CleanupDedup()
}

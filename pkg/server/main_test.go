package server_test

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/downfa11-org/go-broker/pkg/config"
	"github.com/downfa11-org/go-broker/pkg/disk"
	"github.com/downfa11-org/go-broker/pkg/server"
	"github.com/downfa11-org/go-broker/pkg/topic"
	"github.com/downfa11-org/go-broker/util"
)

func TestHandleConnection(t *testing.T) {
	dm := disk.NewDiskManager("testconn", 1024)
	tm := topic.NewTopicManager(&config.Config{CleanupInterval: 60}, dm)

	client, serverConn := net.Pipe()
	defer client.Close()
	defer serverConn.Close()

	go server.HandleConnection(serverConn, tm, dm, false)

	topicName := "default"
	partitions := 4
	topicObj := tm.CreateTopic(topicName, partitions)

	consumerGroup := "test-group"
	topicObj.RegisterConsumerGroup(consumerGroup, 1)

	payloadStr := "hello"
	data := util.EncodeMessage(topicName, payloadStr)

	length := make([]byte, 4)
	binary.BigEndian.PutUint32(length, uint32(len(data)))

	if _, err := client.Write(length); err != nil {
		t.Fatalf("failed to write message length: %v", err)
	}
	if _, err := client.Write(data); err != nil {
		t.Fatalf("failed to write message data: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	ch := tm.Consume(topicName, consumerGroup, 0)
	if ch == nil {
		t.Fatalf("failed to get consumer channel for topic %s", topicName)
	}

	timeout := time.After(500 * time.Millisecond)
	select {
	case m := <-ch:
		if m.Payload != payloadStr {
			t.Errorf("expected message '%s', got '%s'", payloadStr, m.Payload)
		}
	case <-timeout:
		t.Errorf("message not received by consumer within timeout")
	}
}

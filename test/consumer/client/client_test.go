package client_test

import (
	"net"
	"testing"

	"github.com/downfa11-org/go-broker/consumer/client"
)

func TestNewConsumerClient(t *testing.T) {
	c1 := client.NewConsumerClient()
	c2 := client.NewConsumerClient()

	if c1.ID == "" || c2.ID == "" {
		t.Errorf("Expected non-empty IDs")
	}

	if c1.ID == c2.ID {
		t.Errorf("Expected unique IDs, got same: %s", c1.ID)
	}
}

func TestConsumerClient_Connect(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create test listener: %v", err)
	}
	defer ln.Close()

	addr := ln.Addr().String()
	client := client.NewConsumerClient()

	conn, err := client.Connect(addr)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer conn.Close()
}

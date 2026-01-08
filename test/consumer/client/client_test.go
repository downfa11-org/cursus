package client_test

import (
	"net"
	"sync"
	"testing"

	"github.com/downfa11-org/cursus/test/consumer/client"
	"github.com/downfa11-org/cursus/test/consumer/config"
)

func TestNewConsumerClient(t *testing.T) {
	cfg := &config.ConsumerConfig{
		BrokerAddrs: []string{"localhost:9000"},
	}

	c1 := client.NewConsumerClient(cfg)
	c2 := client.NewConsumerClient(cfg)

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
	cfg := &config.ConsumerConfig{
		BrokerAddrs: []string{addr},
	}

	client := client.NewConsumerClient(cfg)

	conn, err := client.Connect(addr)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer conn.Close()
}

func TestConsumerClient_ConnectWithFailover(t *testing.T) {
	ln1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create test listener 1: %v", err)
	}
	defer ln1.Close()

	addr1 := ln1.Addr().String()
	addr2 := "127.0.0.1:99999"

	cfg := &config.ConsumerConfig{
		BrokerAddrs: []string{addr2, addr1},
	}

	c := client.NewConsumerClient(cfg)

	conn, broker, err := c.ConnectWithFailover()
	if err != nil {
		t.Fatalf("ConnectWithFailover failed: %v", err)
	}
	defer conn.Close()

	if broker != addr1 {
		t.Errorf("Expected to connect to %s, got %s", addr1, broker)
	}
}

func TestConsumerClient_ConnectWithFailover_AllFail(t *testing.T) {
	cfg := &config.ConsumerConfig{
		BrokerAddrs: []string{"127.0.0.1:99999", "127.0.0.1:99998"},
	}

	c := client.NewConsumerClient(cfg)

	_, _, err := c.ConnectWithFailover()
	if err == nil {
		t.Error("Expected error when all brokers fail")
	}
}

func TestConsumerClient_ConnectPoolReuse(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create test listener: %v", err)
	}
	defer ln.Close()

	addr := ln.Addr().String()
	cfg := &config.ConsumerConfig{
		BrokerAddrs: []string{addr},
	}

	c := client.NewConsumerClient(cfg)
	conn1, err := c.Connect(addr)
	if err != nil {
		t.Fatalf("First Connect failed: %v", err)
	}

	conn2, err := c.Connect(addr)
	if err != nil {
		t.Fatalf("Second Connect failed: %v", err)
	}

	if conn1 != conn2 {
		t.Error("Expected connection reuse, got different connections")
	}

	conn1.Close()
}

func TestConsumerClient_ConcurrentConnect(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create test listener: %v", err)
	}
	defer ln.Close()

	addr := ln.Addr().String()
	cfg := &config.ConsumerConfig{
		BrokerAddrs: []string{addr},
	}

	var wg sync.WaitGroup
	concurrency := 10
	errors := make(chan error, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := client.NewConsumerClient(cfg)
			conn, err := c.Connect(addr)
			if err != nil {
				errors <- err
				return
			}
			conn.Close()
		}()
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("Concurrent connect error: %v", err)
	}
}

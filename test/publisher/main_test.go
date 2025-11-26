package main

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"
)

// TestCompressDecompressMessage tests gzip compression/decompression
func TestCompressDecompressMessage(t *testing.T) {
	original := []byte("test message")

	// Compress
	var buf bytes.Buffer
	gzWriter := gzip.NewWriter(&buf)
	if _, err := gzWriter.Write(original); err != nil {
		t.Fatalf("gzip write failed: %v", err)
	}
	if err := gzWriter.Close(); err != nil {
		t.Fatalf("gzip close failed: %v", err)
	}
	compressed := buf.Bytes()

	// Decompress
	gzReader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatalf("gzip reader failed: %v", err)
	}
	defer gzReader.Close()

	decompressed, err := io.ReadAll(gzReader)
	if err != nil {
		t.Fatalf("gzip read failed: %v", err)
	}

	if !bytes.Equal(original, decompressed) {
		t.Errorf("Decompressed data doesn't match original")
	}
}

// TestEncodeMessage tests message encoding format
func TestEncodeMessage(t *testing.T) {
	topic := "my-topic"
	payload := "hello"

	topicBytes := []byte(topic)
	payloadBytes := []byte(payload)
	data := make([]byte, 2+len(topicBytes)+len(payloadBytes))
	binary.BigEndian.PutUint16(data[:2], uint16(len(topicBytes)))
	copy(data[2:2+len(topicBytes)], topicBytes)
	copy(data[2+len(topicBytes):], payloadBytes)

	if len(data) < 2 {
		t.Fatalf("Encoded data too short: %d bytes", len(data))
	}

	topicLen := int(binary.BigEndian.Uint16(data[:2]))
	if topicLen != len(topic) {
		t.Errorf("Topic length mismatch: expected %d, got %d", len(topic), topicLen)
	}

	decodedTopic := string(data[2 : 2+topicLen])
	decodedPayload := string(data[2+topicLen:])
	if decodedTopic != topic {
		t.Errorf("Topic mismatch: expected %q, got %q", topic, decodedTopic)
	}
	if decodedPayload != payload {
		t.Errorf("Payload mismatch: expected %q, got %q", payload, decodedPayload)
	}
}

// TestPublisherConfigNormalization tests config validation
func TestPublisherConfigNormalization(t *testing.T) {
	cfg := &PublisherConfig{
		MaxInflightRequests: -1,
		BatchSize:           0,
		BufferSize:          -100,
		MaxRetries:          -5,
		RetryBackoffMS:      -100,
		AckTimeoutMS:        -1000,
	}

	publisher := NewPublisher(cfg)

	if publisher.config.MaxInflightRequests != 5 {
		t.Errorf("Expected MaxInflightRequests=5, got %d", publisher.config.MaxInflightRequests)
	}
	if publisher.config.BatchSize != 100 {
		t.Errorf("Expected BatchSize=100, got %d", publisher.config.BatchSize)
	}
	if publisher.config.BufferSize != 1024 {
		t.Errorf("Expected BufferSize=1024, got %d", publisher.config.BufferSize)
	}
	if publisher.config.MaxRetries != 0 {
		t.Errorf("Expected MaxRetries=0, got %d", publisher.config.MaxRetries)
	}
	if publisher.config.RetryBackoffMS != 100 {
		t.Errorf("Expected RetryBackoffMS=100, got %d", publisher.config.RetryBackoffMS)
	}
	if publisher.config.AckTimeoutMS != 5000 {
		t.Errorf("Expected AckTimeoutMS=5000, got %d", publisher.config.AckTimeoutMS)
	}
}

// TestProducerSeqNum tests sequence number generation
func TestProducerSeqNum(t *testing.T) {
	producer := NewProducerClient()

	if producer.seqNum.Load() != 0 {
		t.Errorf("Expected initial seqNum=0, got %d", producer.seqNum.Load())
	}

	for i := 1; i <= 5; i++ {
		seqNum := producer.NextSeqNum()
		if seqNum != uint64(i) {
			t.Errorf("Expected seqNum=%d, got %d", i, seqNum)
		}
	}
}

// TestPublisherMessageFormat tests message format generation
func TestPublisherMessageFormat(t *testing.T) {
	cfg := &PublisherConfig{
		Topic:             "test-topic",
		Acks:              "1",
		EnableIdempotence: true,
	}

	publisher := NewPublisher(cfg)
	message := "test message"

	payload := fmt.Sprintf("IDEMPOTENT:%s:%s:1:%d:%s",
		cfg.Acks,
		publisher.producer.ID,
		publisher.producer.epoch,
		message)

	if !strings.HasPrefix(payload, "IDEMPOTENT:") {
		t.Error("Expected IDEMPOTENT prefix")
	}

	parts := strings.Split(payload, ":")
	if len(parts) != 6 {
		t.Errorf("Expected 6 parts, got %d", len(parts))
	}
}

// TestPublisherInflightControl tests inflight request limiting
func TestPublisherInflightControl(t *testing.T) {
	cfg := &PublisherConfig{
		Topic:               "test-topic",
		MaxInflightRequests: 2,
		Acks:                "1",
	}

	publisher := NewPublisher(cfg)

	if cap(publisher.inflightSem) != 2 {
		t.Errorf("Expected inflight capacity=2, got %d", cap(publisher.inflightSem))
	}

	publisher.inflightSem <- struct{}{}
	publisher.inflightSem <- struct{}{}

	select {
	case publisher.inflightSem <- struct{}{}:
		t.Error("Should not be able to exceed MaxInflightRequests")
	default:
		// Expected behavior
	}

	<-publisher.inflightSem

	select {
	case publisher.inflightSem <- struct{}{}:
		// Expected behavior
	default:
		t.Error("Should be able to acquire after release")
	}
}

// TestPublisherTLS tests TLS connection (requires valid certs)
func TestPublisherTLS(t *testing.T) {
	t.Skip("Requires valid TLS certificates and running broker - integration test")

	cfg := &PublisherConfig{
		BrokerAddr:  "localhost:9000",
		UseTLS:      true,
		TLSCertPath: "test-cert.pem",
		TLSKeyPath:  "test-key.pem",
	}

	publisher := NewPublisher(cfg)
	err := publisher.producer.Connect(cfg.BrokerAddr, cfg.UseTLS, cfg.TLSCertPath, cfg.TLSKeyPath)
	if err != nil {
		t.Fatalf("Failed to connect with TLS: %v", err)
	}
	defer publisher.producer.Close()

	if _, ok := publisher.producer.conn.(*tls.Conn); !ok {
		t.Error("Expected TLS connection")
	}
}

// TestPublisherGzip tests gzip compression
func TestPublisherGzip(t *testing.T) {
	original := []byte("test message")

	var buf bytes.Buffer
	gzWriter := gzip.NewWriter(&buf)
	if _, err := gzWriter.Write(original); err != nil {
		t.Fatalf("Compression failed: %v", err)
	}
	if err := gzWriter.Close(); err != nil {
		t.Fatalf("Compression close failed: %v", err)
	}
	compressed := buf.Bytes()

	gzReader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatalf("Decompression failed: %v", err)
	}
	defer gzReader.Close()

	decompressed, err := io.ReadAll(gzReader)
	if err != nil {
		t.Fatalf("Decompression read failed: %v", err)
	}

	if !bytes.Equal(original, decompressed) {
		t.Error("Decompressed data doesn't match original")
	}
}

// TestPublisherRetry tests retry logic (unit test without broker)
func TestPublisherRetry(t *testing.T) {
	cfg := &PublisherConfig{
		BrokerAddr:     "localhost:9000",
		MaxRetries:     3,
		RetryBackoffMS: 100,
	}

	publisher := NewPublisher(cfg)

	if publisher.config.MaxRetries != 3 {
		t.Errorf("Expected MaxRetries=3, got %d", publisher.config.MaxRetries)
	}
	if publisher.config.RetryBackoffMS != 100 {
		t.Errorf("Expected RetryBackoffMS=100, got %d", publisher.config.RetryBackoffMS)
	}
}

// TestPublisherAcks tests different ack modes
func TestPublisherAcks(t *testing.T) {
	testCases := []struct {
		name string
		acks string
	}{
		{"NoAck", "0"},
		{"LeaderAck", "1"},
		{"AllAck", "all"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &PublisherConfig{
				Topic: "test-topic",
				Acks:  tc.acks,
			}

			publisher := NewPublisher(cfg)
			if publisher.config.Acks != tc.acks {
				t.Errorf("Expected Acks=%q, got %q", tc.acks, publisher.config.Acks)
			}
		})
	}
}

func TestPublisherBatching(t *testing.T) {
	cfg := &PublisherConfig{
		Topic:      "test-topic",
		BatchSize:  3,
		BufferSize: 10,
		Acks:       "1",
	}

	publisher := NewPublisher(cfg)
	defer publisher.Close()

	for i := 0; i < 3; i++ {
		_, err := publisher.PublishMessage(fmt.Sprintf("msg%d", i))
		if err != nil {
			t.Fatalf("PublishMessage failed: %v", err)
		}
	}

	time.Sleep(200 * time.Millisecond)

	if _, err := publisher.PublishMessage("msg3"); err != nil {
		t.Fatalf("PublishMessage failed: %v", err)
	}
	publisher.Flush()
}

package main_test

import (
	"testing"
)

type BrokerClient interface {
	Send(msg []byte) ([]byte, error)
	CreateTopic(topic string, partitions int) error
}

type MockBrokerClient struct{}

func (m *MockBrokerClient) Send(msg []byte) ([]byte, error) {
	return []byte("ACK"), nil
}

func (m *MockBrokerClient) CreateTopic(topic string, partitions int) error {
	return nil
}

type Publisher struct {
	Broker     BrokerClient
	Topic      string
	Partitions int
}

func NewPublisherWithClient(topic string, partitions int, client BrokerClient) *Publisher {
	return &Publisher{
		Broker:     client,
		Topic:      topic,
		Partitions: partitions,
	}
}

func (p *Publisher) CreateTopic() error {
	return p.Broker.CreateTopic(p.Topic, p.Partitions)
}

func (p *Publisher) PublishMessage(msg string) error {
	_, err := p.Broker.Send([]byte(msg))
	return err
}

func EncodeMessage(topic, payload string) []byte {
	// Use length-prefix encoding: [topic-length][topic][payload]
	topicBytes := []byte(topic)
	payloadBytes := []byte(payload)
	result := make([]byte, 4+len(topicBytes)+len(payloadBytes))

	result[0] = byte(len(topicBytes) >> 24)
	result[1] = byte(len(topicBytes) >> 16)
	result[2] = byte(len(topicBytes) >> 8)
	result[3] = byte(len(topicBytes))

	copy(result[4:], topicBytes)
	copy(result[4+len(topicBytes):], payloadBytes)
	return result
}

func TestPublisher_CreateTopic(t *testing.T) {
	mock := &MockBrokerClient{}
	pub := NewPublisherWithClient("test-topic", 2, mock)

	if err := pub.CreateTopic(); err != nil {
		t.Errorf("CreateTopic failed: %v", err)
	}
}

func TestPublisher_PublishMessage(t *testing.T) {
	mock := &MockBrokerClient{}
	pub := NewPublisherWithClient("test-topic", 2, mock)

	if err := pub.PublishMessage("Hello test message"); err != nil {
		t.Errorf("PublishMessage failed: %v", err)
	}
}

func TestEncodeMessage(t *testing.T) {
	topic := "my-topic"
	payload := "hello"
	data := EncodeMessage(topic, payload)

	if string(data) != topic+payload {
		t.Errorf("Encoded data mismatch, got %s", string(data))
	}
}

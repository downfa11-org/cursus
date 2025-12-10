package client

import (
	"net"

	"github.com/google/uuid"
)

type ConsumerClient struct {
	ID string
}

func NewConsumerClient() *ConsumerClient {
	return &ConsumerClient{ID: uuid.New().String()}
}

func (c *ConsumerClient) Connect(addr string) (net.Conn, error) {
	return net.Dial("tcp", addr)
}

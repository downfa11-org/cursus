package delivery

import (
	"fmt"
	"time"

	"github.com/downfa11-org/go-broker/pkg/cluster/controller"
	"github.com/downfa11-org/go-broker/pkg/cluster/transport"
	"github.com/downfa11-org/go-broker/util"
)

type MessageDelivery struct {
	transport         *transport.Transport
	clusterController *controller.ClusterController
	brokerID          string
	localAddr         string
}

func NewMessageDelivery(cc *controller.ClusterController, brokerID, localAddr string, transportTimeout time.Duration) *MessageDelivery {
	if transportTimeout == 0 {
		transportTimeout = 5 * time.Second
	}

	return &MessageDelivery{
		transport:         transport.NewTransport(transportTimeout),
		brokerID:          brokerID,
		localAddr:         localAddr,
		clusterController: cc,
	}
}

func (md *MessageDelivery) DeliverMessage(topic string, partition int, originalCmd string) error {
	util.Debug("Delivering message to topic %s partition %d", topic, partition)

	targetBroker, err := md.getPartitionLeader(topic, partition)
	if err != nil {
		util.Error("Failed to determine partition leader for %s-%d: %v", topic, partition, err)
		return fmt.Errorf("failed to determine partition leader: %w", err)
	}

	if targetBroker == md.localAddr {
		util.Warn("Message already at leader broker %s for %s-%d", targetBroker, topic, partition)
		return fmt.Errorf("message is already at the leader broker")
	}

	util.Debug("Forwarding message to broker %s for %s-%d", targetBroker, topic, partition)
	_, err = md.transport.SendRequest(targetBroker, originalCmd)
	if err != nil {
		return fmt.Errorf("failed to forward message to leader %s: %w", targetBroker, err)
	}
	return nil
}

func (md *MessageDelivery) getPartitionLeader(topic string, partition int) (string, error) {
	return md.clusterController.GetPartitionLeader(topic, partition)
}

package delivery

import (
	"fmt"
	"time"

	"github.com/downfa11-org/go-broker/pkg/cluster/controller"
	"github.com/downfa11-org/go-broker/pkg/cluster/discovery"
	"github.com/downfa11-org/go-broker/pkg/cluster/transport"
	"github.com/downfa11-org/go-broker/pkg/types"
)

type MessageDelivery struct {
	serviceDiscovery  discovery.ServiceDiscovery
	transport         *transport.Transport
	clusterController *controller.ClusterController
	brokerID          string
	localAddr         string
}

func NewMessageDelivery(sd discovery.ServiceDiscovery, cc *controller.ClusterController, brokerID, localAddr string) *MessageDelivery {
	return &MessageDelivery{
		serviceDiscovery:  sd,
		transport:         transport.NewTransport(5 * time.Second),
		brokerID:          brokerID,
		localAddr:         localAddr,
		clusterController: cc,
	}
}

func (md *MessageDelivery) DeliverMessage(topic string, partition int, msg types.Message, originalCmd string) error {
	targetBroker, err := md.getPartitionLeader(topic, partition)
	if err != nil {
		return fmt.Errorf("failed to determine partition leader: %w", err)
	}

	if targetBroker == md.localAddr {
		return fmt.Errorf("message is already at the leader broker")
	}

	_, err = md.transport.SendRequest(targetBroker, originalCmd)
	return err
}

func (md *MessageDelivery) getPartitionLeader(topic string, partition int) (string, error) {
	return md.clusterController.GetPartitionLeader(topic, partition)
}

package routing

import (
	"fmt"
	"hash/fnv"
	"time"

	"github.com/downfa11-org/go-broker/pkg/cluster/discovery"
	"github.com/downfa11-org/go-broker/pkg/cluster/replication"
	"github.com/downfa11-org/go-broker/pkg/cluster/transport"
	"github.com/downfa11-org/go-broker/util"
)

type ClientRouter struct {
	serviceDiscovery discovery.ServiceDiscovery
	brokerID         string
	localAddr        string
}

type RouteDecision struct {
	TargetBroker string
	IsLocal      bool
	Reason       string
}

func NewClientRouter(sd discovery.ServiceDiscovery, brokerID, localAddr string) *ClientRouter {
	return &ClientRouter{
		serviceDiscovery: sd,
		brokerID:         brokerID,
		localAddr:        localAddr,
	}
}

func (r *ClientRouter) RouteTopicPartition(topic string, partition int) (*RouteDecision, error) {
	util.Debug("Routing topic %s partition %d", topic, partition)

	brokers, err := r.serviceDiscovery.DiscoverBrokers()
	if err != nil {
		util.Error("Failed to discover brokers for routing: %v", err)
		return nil, fmt.Errorf("failed to discover brokers: %w", err)
	}

	if len(brokers) == 0 {
		util.Error("No brokers available for routing")
		return nil, fmt.Errorf("no brokers available")
	}

	target := r.selectBrokerForPartition(topic, partition, brokers)
	isLocal := target == r.localAddr

	util.Debug("Routed %s-%d to broker %s (local: %v)", topic, partition, target, isLocal)

	return &RouteDecision{
		TargetBroker: target,
		IsLocal:      target == r.localAddr,
		Reason:       "partition-based routing",
	}, nil
}

func (r *ClientRouter) RouteConsumerGroup(groupID string) (*RouteDecision, error) {
	util.Debug("Routing consumer group %s", groupID)

	brokers, err := r.serviceDiscovery.DiscoverBrokers()
	if err != nil {
		util.Error("Failed to discover brokers for consumer group routing: %v", err)
		return nil, fmt.Errorf("failed to discover brokers: %w", err)
	}

	if len(brokers) == 0 {
		return nil, fmt.Errorf("no brokers available")
	}

	hash := fnv.New32a()
	hash.Write([]byte(groupID))
	idx := hash.Sum32() % uint32(len(brokers))

	target := brokers[idx].Addr
	util.Debug("Routed consumer group %s to broker %s (local: %v)", groupID, target, target == r.localAddr)

	return &RouteDecision{
		TargetBroker: target,
		IsLocal:      target == r.localAddr,
		Reason:       "group-based routing",
	}, nil
}

func (r *ClientRouter) selectBrokerForPartition(topic string, partition int, brokers []replication.BrokerInfo) string {
	hash := fnv.New32a()
	hash.Write([]byte(fmt.Sprintf("%s-%d", topic, partition)))
	idx := hash.Sum32() % uint32(len(brokers))

	selected := brokers[idx].Addr
	util.Debug("Selected broker %s for %s-%d (hash index: %d)", selected, topic, partition, idx)
	return selected
}

func (r *ClientRouter) ForwardRequest(targetBroker string, command string) (string, error) {
	util.Debug("Forwarding request to broker %s", targetBroker)

	transport := transport.NewTransport(5 * time.Second)
	resp, err := transport.SendRequest(targetBroker, command)

	if err != nil {
		util.Error("Failed to forward request to %s: %v", targetBroker, err)
		return "", err
	}

	util.Debug("Received response from broker %s", targetBroker)
	return resp, nil
}

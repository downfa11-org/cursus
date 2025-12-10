package routing

import (
	"fmt"
	"hash/fnv"
	"time"

	"github.com/downfa11-org/go-broker/pkg/cluster/discovery"
	"github.com/downfa11-org/go-broker/pkg/cluster/replication"
	"github.com/downfa11-org/go-broker/pkg/cluster/transport"
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
	brokers, err := r.serviceDiscovery.DiscoverBrokers()
	if err != nil {
		return nil, fmt.Errorf("failed to discover brokers: %w", err)
	}

	if len(brokers) == 0 {
		return nil, fmt.Errorf("no brokers available")
	}

	target := r.selectBrokerForPartition(topic, partition, brokers)

	return &RouteDecision{
		TargetBroker: target,
		IsLocal:      target == r.localAddr,
		Reason:       "partition-based routing",
	}, nil
}

func (r *ClientRouter) RouteConsumerGroup(groupID string) (*RouteDecision, error) {
	brokers, err := r.serviceDiscovery.DiscoverBrokers()
	if err != nil {
		return nil, fmt.Errorf("failed to discover brokers: %w", err)
	}

	hash := fnv.New32a()
	hash.Write([]byte(groupID))
	idx := hash.Sum32() % uint32(len(brokers))

	target := brokers[idx].Addr

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

	return brokers[idx].Addr
}

func (r *ClientRouter) ForwardRequest(targetBroker string, command string) (string, error) {
	transport := transport.NewTransport(5 * time.Second)
	return transport.SendRequest(targetBroker, command)
}

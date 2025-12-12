package routing

import (
	"errors"
	"fmt"
	"hash/fnv"
	"strings"
	"testing"

	"github.com/downfa11-org/go-broker/pkg/cluster/replication"
)

type MockServiceDiscovery struct {
	DiscoverBrokersFunc func() ([]replication.BrokerInfo, error)
}

func (m *MockServiceDiscovery) Register() error                                       { return nil }
func (m *MockServiceDiscovery) Deregister() error                                     { return nil }
func (m *MockServiceDiscovery) JoinCluster(arg1, arg2 string) (string, error)         { return "", nil }
func (m *MockServiceDiscovery) SetRaftManager(rm *replication.RaftReplicationManager) {}
func (m *MockServiceDiscovery) AddNode(nodeID string, addr string) (leader string, err error) {
	return "", nil
}
func (m *MockServiceDiscovery) RemoveNode(nodeID string) (leader string, err error) { return "", nil }
func (m *MockServiceDiscovery) DiscoverBrokers() ([]replication.BrokerInfo, error) {
	return m.DiscoverBrokersFunc()
}

var testBrokers = []replication.BrokerInfo{
	{ID: "b1", Addr: "192.168.1.1:9092"},
	{ID: "b2", Addr: "192.168.1.2:9092"},
	{ID: "b3", Addr: "192.168.1.3:9092"},
}

func TestNewClientRouter(t *testing.T) {
	mockSD := &MockServiceDiscovery{}
	router := NewClientRouter(mockSD, "b1", "192.168.1.1:9092")

	if router.brokerID != "b1" || router.localAddr != "192.168.1.1:9092" {
		t.Errorf("NewClientRouter failed to set fields correctly")
	}
}

func TestRouteTopicPartition_SuccessLocal(t *testing.T) {
	const (
		topic     = "test-topic"
		partition = 1
		localAddr = "192.168.1.2:9092"
		brokerID  = "b2"
	)

	hash := fnv.New32a()
	hash.Write([]byte(fmt.Sprintf("%s-%d", topic, partition)))
	expectedIdx := int(hash.Sum32() % 3)

	expectedTarget := testBrokers[expectedIdx].Addr

	mockSD := &MockServiceDiscovery{
		DiscoverBrokersFunc: func() ([]replication.BrokerInfo, error) {
			return testBrokers, nil
		},
	}

	router := NewClientRouter(mockSD, brokerID, localAddr)
	decision, err := router.RouteTopicPartition(topic, partition)

	if err != nil {
		t.Fatalf("RouteTopicPartition failed unexpectedly: %v", err)
	}

	if decision.TargetBroker != expectedTarget {
		t.Errorf("Expected target broker %s (idx=%d), got %s", expectedTarget, expectedIdx, decision.TargetBroker)
	}

	expectedIsLocal := (expectedTarget == localAddr)
	if decision.IsLocal != expectedIsLocal {
		t.Errorf("Expected IsLocal to be %v, got %v", expectedIsLocal, decision.IsLocal)
	}

	if decision.Reason != "partition-based routing" {
		t.Errorf("Expected reason 'partition-based routing', got %s", decision.Reason)
	}
}

func TestRouteTopicPartition_SuccessRemote(t *testing.T) {
	const (
		topic     = "test-topic"
		partition = 1
		localAddr = "192.168.1.99:9092"
		brokerID  = "b99"
	)

	hash := fnv.New32a()
	hash.Write([]byte(fmt.Sprintf("%s-%d", topic, partition)))
	expectedIdx := int(hash.Sum32() % 3)
	expectedTarget := testBrokers[expectedIdx].Addr

	mockSD := &MockServiceDiscovery{
		DiscoverBrokersFunc: func() ([]replication.BrokerInfo, error) {
			return testBrokers, nil
		},
	}

	router := NewClientRouter(mockSD, brokerID, localAddr)
	decision, err := router.RouteTopicPartition(topic, partition)

	if err != nil {
		t.Fatalf("RouteTopicPartition failed unexpectedly: %v", err)
	}

	if decision.TargetBroker != expectedTarget {
		t.Errorf("Expected target broker %s (idx=%d), got %s", expectedTarget, expectedIdx, decision.TargetBroker)
	}

	if decision.IsLocal {
		t.Errorf("Expected IsLocal to be false, got true")
	}
}

func TestRouteTopicPartition_DiscoveryFailure(t *testing.T) {
	expectedErr := errors.New("discovery failed")
	mockSD := &MockServiceDiscovery{
		DiscoverBrokersFunc: func() ([]replication.BrokerInfo, error) {
			return nil, expectedErr
		},
	}
	router := NewClientRouter(mockSD, "b1", "192.168.1.1:9092")

	_, err := router.RouteTopicPartition("topic", 0)

	if err == nil {
		t.Fatalf("Expected error, got nil")
	}
	if !errors.Is(err, expectedErr) {
		t.Errorf("Expected error containing '%v', got '%v'", expectedErr, err)
	}
}

func TestRouteTopicPartition_NoBrokersAvailable(t *testing.T) {
	mockSD := &MockServiceDiscovery{
		DiscoverBrokersFunc: func() ([]replication.BrokerInfo, error) {
			return []replication.BrokerInfo{}, nil
		},
	}
	router := NewClientRouter(mockSD, "b1", "192.168.1.1:9092")

	_, err := router.RouteTopicPartition("topic", 0)

	if err == nil {
		t.Fatalf("Expected error, got nil")
	}
	if !strings.Contains(err.Error(), "no brokers available") {
		t.Errorf("Expected 'no brokers available' error, got: %v", err)
	}
}

func TestRouteConsumerGroup_SuccessRemote(t *testing.T) {
	const (
		groupID   = "test-group"
		localAddr = "192.168.1.99:9092"
		brokerID  = "b99"
	)

	mockSD := &MockServiceDiscovery{
		DiscoverBrokersFunc: func() ([]replication.BrokerInfo, error) {
			return testBrokers, nil
		},
	}

	router := NewClientRouter(mockSD, brokerID, localAddr)

	decision, err := router.RouteConsumerGroup(groupID)

	if err != nil {
		t.Fatalf("RouteConsumerGroup failed unexpectedly: %v", err)
	}

	expectedTarget := testBrokers[2].Addr // 192.168.1.3:9092

	if decision.TargetBroker != expectedTarget {
		t.Errorf("Expected target broker %s, got %s", expectedTarget, decision.TargetBroker)
	}
	if decision.IsLocal {
		t.Errorf("Expected IsLocal to be false, got true")
	}
	if decision.Reason != "group-based routing" {
		t.Errorf("Expected reason 'group-based routing', got %s", decision.Reason)
	}
}

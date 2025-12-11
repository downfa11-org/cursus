package replication

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/downfa11-org/go-broker/pkg/types"
	"github.com/hashicorp/raft"
)

type MockRaft struct {
	BootstrapClusterFunc func(raft.Configuration) raft.Future
	ApplyFunc            func([]byte, time.Duration) raft.ApplyFuture
	AddVoterFunc         func(raft.ServerID, raft.ServerAddress, uint64, time.Duration) raft.IndexFuture
	StateFunc            func() raft.RaftState
	ShutdownFunc         func() raft.Future
}

func (m *MockRaft) AddVoter(id raft.ServerID, addr raft.ServerAddress, prevIndex uint64, term time.Duration) raft.IndexFuture {
	return m.AddVoterFunc(id, addr, prevIndex, term)
}
func (m *MockRaft) BootstrapCluster(c raft.Configuration) raft.Future {
	return m.BootstrapClusterFunc(c)
}
func (m *MockRaft) Apply(data []byte, timeout time.Duration) raft.ApplyFuture {
	return m.ApplyFunc(data, timeout)
}
func (m *MockRaft) State() raft.RaftState {
	return m.StateFunc()
}
func (m *MockRaft) Shutdown() raft.Future {
	return m.ShutdownFunc()
}
func (m *MockRaft) Leader() raft.ServerAddress {
	return raft.ServerAddress("")
}

type MockConfigurationFuture struct {
	raft.ConfigurationFuture
}

func (m *MockRaft) GetConfiguration() raft.ConfigurationFuture {
	return &MockConfigurationFuture{}
}

func (m *MockRaft) RemoveServer(id raft.ServerID, prevIndex uint64, prevTerm time.Duration) raft.IndexFuture {
	return &MockFuture{ErrorVal: nil}
}

type MockFuture struct {
	ErrorVal error
}

func (m *MockFuture) Error() error          { return m.ErrorVal }
func (m *MockFuture) Index() uint64         { return 1 }
func (m *MockFuture) Response() interface{} { return nil }
func (m *MockFuture) Logs() []raft.Log      { return nil }

type MockApplyFuture struct {
	ErrorVal error
}

func (m *MockApplyFuture) Error() error          { return m.ErrorVal }
func (m *MockApplyFuture) Index() uint64         { return 1 }
func (m *MockApplyFuture) Response() interface{} { return nil }
func (m *MockApplyFuture) Logs() []raft.Log      { return nil }

type MockBrokerFSM struct {
	GetBrokersFunc           func() []BrokerInfo
	GetPartitionMetadataFunc func(key string) *PartitionMetadata
}

func (m *MockBrokerFSM) Apply(*raft.Log) interface{}         { return nil }
func (m *MockBrokerFSM) Restore(rc io.ReadCloser) error      { return nil }
func (m *MockBrokerFSM) Snapshot() (raft.FSMSnapshot, error) { return nil, nil }
func (m *MockBrokerFSM) GetBrokers() []BrokerInfo            { return m.GetBrokersFunc() }
func (m *MockBrokerFSM) GetPartitionMetadata(key string) *PartitionMetadata {
	return m.GetPartitionMetadataFunc(key)
}

type MockISRManager struct {
	HasQuorumFunc func(topic string, partition int, required int) bool
}

func (m *MockISRManager) HasQuorum(topic string, partition int, required int) bool {
	return m.HasQuorumFunc(topic, partition, required)
}

func newTestRaftRM(raftMock *MockRaft, fsmMock *MockBrokerFSM, isrMock *MockISRManager) *RaftReplicationManager {
	if fsmMock == nil {
		fsmMock = &MockBrokerFSM{
			GetBrokersFunc:           func() []BrokerInfo { return []BrokerInfo{} },
			GetPartitionMetadataFunc: func(key string) *PartitionMetadata { return nil },
		}
	}
	if isrMock == nil {
		isrMock = &MockISRManager{HasQuorumFunc: func(topic string, partition int, required int) bool { return true }}
	}
	return &RaftReplicationManager{
		raft:             raftMock,
		fsm:              fsmMock,
		isrManager:       isrMock,
		brokerID:         "b1",
		localAddr:        "127.0.0.1:8000",
		peers:            make(map[string]string),
		partitionLeaders: make(map[string]string),
	}
}

func TestBootstrapCluster_Success(t *testing.T) {
	mockRaft := &MockRaft{
		BootstrapClusterFunc: func(c raft.Configuration) raft.Future {
			if len(c.Servers) != 3 { // b1(self) + b2 + b3
				t.Errorf("Expected 3 servers, got %d", len(c.Servers))
			}
			return &MockFuture{ErrorVal: nil}
		},
	}
	rm := newTestRaftRM(mockRaft, nil, nil)

	peers := []string{"127.0.0.1:8000", "127.0.0.1:8001", "127.0.0.1:8002"}

	if err := rm.BootstrapCluster(peers); err != nil {
		t.Fatalf("BootstrapCluster failed: %v", err)
	}
}

func TestReplicateMessage_Success(t *testing.T) {
	testMsg := types.Message{Key: "k1", Payload: "data"}

	mockRaft := &MockRaft{
		ApplyFunc: func(data []byte, timeout time.Duration) raft.ApplyFuture {
			var entry ReplicationEntry
			if err := json.Unmarshal(data, &entry); err != nil {
				t.Fatalf("Failed to unmarshal applied data: %v", err)
			}
			if entry.Topic != "t1" || entry.Partition != 0 || string(entry.Message.Payload) != "data" {
				t.Errorf("Applied data mismatch: %+v", entry)
			}
			return &MockApplyFuture{ErrorVal: nil}
		},
	}
	rm := newTestRaftRM(mockRaft, nil, nil)

	if err := rm.ReplicateMessage("t1", 0, testMsg); err != nil {
		t.Fatalf("ReplicateMessage failed: %v", err)
	}
}

func TestUpdatePartitionLeader_Success(t *testing.T) {
	const (
		topic     = "t1"
		partition = 0
		leader    = "127.0.0.1:8002"
	)

	mockFSM := &MockBrokerFSM{
		GetBrokersFunc: func() []BrokerInfo {
			return []BrokerInfo{
				{ID: "b1", Addr: "127.0.0.1:8000"},
				{ID: "b2", Addr: "127.0.0.1:8001"},
				{ID: "b3", Addr: "127.0.0.1:8002"},
			}
		},
	}

	mockRaft := &MockRaft{
		ApplyFunc: func(data []byte, timeout time.Duration) raft.ApplyFuture {
			dataStr := string(data)
			if !strings.HasPrefix(dataStr, "PARTITION:") {
				t.Fatalf("Expected PARTITION prefix, got: %s", dataStr)
			}
			parts := strings.SplitN(dataStr, ":", 3)
			if parts[1] != "t1-0" {
				t.Fatalf("Partition key mismatch: %s", parts[1])
			}

			var meta PartitionMetadata
			if err := json.Unmarshal([]byte(parts[2]), &meta); err != nil {
				t.Fatalf("Failed to unmarshal metadata: %v", err)
			}
			if meta.Leader != leader {
				t.Errorf("Leader mismatch: %s", meta.Leader)
			}
			if len(meta.Replicas) != 3 {
				t.Errorf("Replicas count mismatch: %d", len(meta.Replicas))
			}
			return &MockApplyFuture{ErrorVal: nil}
		},
	}
	rm := newTestRaftRM(mockRaft, mockFSM, nil)

	if err := rm.UpdatePartitionLeader(topic, partition, leader); err != nil {
		t.Fatalf("UpdatePartitionLeader failed: %v", err)
	}
}

func TestGetPartitionReplicas_HashSelection(t *testing.T) {
	mockFSM := &MockBrokerFSM{
		GetBrokersFunc: func() []BrokerInfo {
			return []BrokerInfo{
				{Addr: "10.0.0.1"}, {Addr: "10.0.0.2"}, {Addr: "10.0.0.3"},
				{Addr: "10.0.0.4"}, {Addr: "10.0.0.5"},
			}
		},
	}
	rm := newTestRaftRM(nil, mockFSM, nil)
	replicas := rm.GetPartitionReplicas("test-topic", 0)

	if len(replicas) != 3 {
		t.Fatalf("Expected 3 replicas, got %d", len(replicas))
	}

	hash := fnv.New32a()
	hash.Write([]byte("test-topic-0"))
	startIdx := hash.Sum32() % 5

	expected := []string{
		fmt.Sprintf("10.0.0.%d", startIdx+1),
		fmt.Sprintf("10.0.0.%d", (startIdx+1)%5+1),
		fmt.Sprintf("10.0.0.%d", (startIdx+2)%5+1),
	}

	for i, addr := range replicas {
		if addr != expected[i] {
			t.Errorf("Replica %d mismatch. Expected %s, got %s", i, expected[i], addr)
		}
	}
}

func TestReplicateWithQuorum_QuorumCheckFailure(t *testing.T) {
	mockISR := &MockISRManager{
		HasQuorumFunc: func(topic string, partition int, required int) bool {
			return false
		},
	}
	rm := newTestRaftRM(nil, nil, mockISR)

	err := rm.ReplicateWithQuorum("t1", 0, types.Message{}, 2)

	if err == nil || !strings.Contains(err.Error(), "not enough in-sync replicas") {
		t.Errorf("Expected 'not enough in-sync replicas' error, got: %v", err)
	}
}

func TestReplicateWithQuorum_RaftApplyFailure(t *testing.T) {
	expectedErr := errors.New("raft apply error")

	mockRaft := &MockRaft{
		ApplyFunc: func(data []byte, timeout time.Duration) raft.ApplyFuture {
			return &MockApplyFuture{ErrorVal: expectedErr}
		},
	}
	rm := newTestRaftRM(mockRaft, nil, nil)

	err := rm.ReplicateWithQuorum("t1", 0, types.Message{}, 1)

	if !errors.Is(err, expectedErr) {
		t.Errorf("Expected Raft apply failure error, got: %v", err)
	}
}

func TestValidateLeaderEpoch_Success(t *testing.T) {
	const epoch = 100
	mockFSM := &MockBrokerFSM{
		GetPartitionMetadataFunc: func(key string) *PartitionMetadata {
			return &PartitionMetadata{LeaderEpoch: epoch}
		},
	}
	rm := newTestRaftRM(nil, mockFSM, nil)

	if !rm.ValidateLeaderEpoch("t1", 0, epoch) {
		t.Error("Epoch validation failed for matching epoch")
	}
}

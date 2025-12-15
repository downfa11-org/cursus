package replication

import (
	"io"
	"testing"
	"time"

	"github.com/downfa11-org/go-broker/pkg/cluster/replication/fsm"
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
	ConfigVal raft.Configuration
	ErrorVal  error
}

func (m *MockConfigurationFuture) Configuration() raft.Configuration {
	return m.ConfigVal
}

func (m *MockConfigurationFuture) Error() error {
	return m.ErrorVal
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
	GetBrokersFunc           func() []fsm.BrokerInfo
	GetPartitionMetadataFunc func(key string) *fsm.PartitionMetadata
}

func (m *MockBrokerFSM) Apply(*raft.Log) interface{}         { return nil }
func (m *MockBrokerFSM) Restore(rc io.ReadCloser) error      { return nil }
func (m *MockBrokerFSM) Snapshot() (raft.FSMSnapshot, error) { return nil, nil }
func (m *MockBrokerFSM) GetBrokers() []fsm.BrokerInfo        { return m.GetBrokersFunc() }
func (m *MockBrokerFSM) GetPartitionMetadata(key string) *fsm.PartitionMetadata {
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
			GetBrokersFunc:           func() []fsm.BrokerInfo { return []fsm.BrokerInfo{} },
			GetPartitionMetadataFunc: func(key string) *fsm.PartitionMetadata { return nil },
		}
	}
	if isrMock == nil {
		isrMock = &MockISRManager{HasQuorumFunc: func(topic string, partition int, required int) bool { return true }}
	}
	return &RaftReplicationManager{
		raft:       raftMock,
		fsm:        fsmMock,
		isrManager: isrMock,
		brokerID:   "b1",
		localAddr:  "127.0.0.1:8000",
		peers:      make(map[string]string),
	}
}

func TestBootstrapCluster_Success(t *testing.T) {
	mockRaft := &MockRaft{
		BootstrapClusterFunc: func(c raft.Configuration) raft.Future {
			if len(c.Servers) != 3 { // b1(self) + b2 + b3
				t.Errorf("Expected 3 servers, got %d", len(c.Servers))
			}
			return &MockConfigurationFuture{
				ConfigVal: c,
				ErrorVal:  nil,
			}
		},
	}
	rm := newTestRaftRM(mockRaft, nil, nil)

	peers := []string{"127.0.0.1:8000", "127.0.0.1:8001", "127.0.0.1:8002"}

	if err := rm.BootstrapCluster(peers); err != nil {
		t.Fatalf("BootstrapCluster failed: %v", err)
	}
}

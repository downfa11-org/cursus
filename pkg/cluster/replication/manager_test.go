package replication

import (
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

type MockRaft struct {
	ApplyFunc            func([]byte, time.Duration) raft.ApplyFuture
	AddVoterFunc         func(raft.ServerID, raft.ServerAddress, uint64, time.Duration) raft.IndexFuture
	RemoveServerFunc     func(raft.ServerID, uint64, time.Duration) raft.IndexFuture
	StateFunc            func() raft.RaftState
	LeaderFunc           func() raft.ServerAddress
	BootstrapClusterFunc func(raft.Configuration) raft.Future
	ShutdownFunc         func() raft.Future
}

func (m *MockRaft) Apply(d []byte, t time.Duration) raft.ApplyFuture { return m.ApplyFunc(d, t) }
func (m *MockRaft) AddVoter(id raft.ServerID, addr raft.ServerAddress, idx uint64, t time.Duration) raft.IndexFuture {
	return m.AddVoterFunc(id, addr, idx, t)
}
func (m *MockRaft) RemoveServer(id raft.ServerID, idx uint64, t time.Duration) raft.IndexFuture {
	return m.RemoveServerFunc(id, idx, t)
}
func (m *MockRaft) State() raft.RaftState      { return m.StateFunc() }
func (m *MockRaft) Leader() raft.ServerAddress { return m.LeaderFunc() }
func (m *MockRaft) BootstrapCluster(c raft.Configuration) raft.Future {
	return m.BootstrapClusterFunc(c)
}
func (m *MockRaft) GetConfiguration() raft.ConfigurationFuture { return &MockConfigurationFuture{} }
func (m *MockRaft) Shutdown() raft.Future                      { return m.ShutdownFunc() }

func newTestRaftRM(raftMock *MockRaft) *RaftReplicationManager {
	return &RaftReplicationManager{
		raft:      raftMock,
		brokerID:  "b1",
		localAddr: "127.0.0.1:8000",
		peers:     make(map[string]string),
		leaderCh:  make(chan bool, 1),
	}
}

func TestAddVoter_Success(t *testing.T) {
	called := false
	mockRaft := &MockRaft{
		AddVoterFunc: func(id raft.ServerID, addr raft.ServerAddress, idx uint64, timeout time.Duration) raft.IndexFuture {
			called = true
			if id != "node2" || addr != "127.0.0.1:8001" {
				t.Errorf("Unexpected voter data: %v, %v", id, addr)
			}
			return &MockFuture{ErrorVal: nil}
		},
	}

	rm := newTestRaftRM(mockRaft)
	err := rm.AddVoter("node2", "127.0.0.1:8001")

	if err != nil {
		t.Fatalf("AddVoter failed: %v", err)
	}
	if !called {
		t.Error("AddVoterFunc was not called")
	}
}

func TestApplyCommand(t *testing.T) {
	mockRaft := &MockRaft{
		ApplyFunc: func(data []byte, timeout time.Duration) raft.ApplyFuture {
			expected := "PREFIX:payload"
			if string(data) != expected {
				t.Errorf("Expected %s, got %s", expected, string(data))
			}
			return &MockFuture{ErrorVal: nil}
		},
	}

	rm := newTestRaftRM(mockRaft)
	err := rm.ApplyCommand("PREFIX", []byte("payload"))

	if err != nil {
		t.Errorf("ApplyCommand failed: %v", err)
	}
}

type MockFuture struct {
	raft.IndexFuture
	raft.ApplyFuture
	ErrorVal    error
	ResponseVal interface{}
}

func (m *MockFuture) Error() error          { return m.ErrorVal }
func (m *MockFuture) Response() interface{} { return m.ResponseVal }
func (m *MockFuture) Index() uint64         { return 0 }

type MockConfigurationFuture struct {
	raft.ConfigurationFuture
}

func (m *MockConfigurationFuture) Error() error { return nil }
func (m *MockConfigurationFuture) Configuration() raft.Configuration {
	return raft.Configuration{}
}

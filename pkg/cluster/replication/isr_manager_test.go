package replication

import (
	"strings"
	"testing"

	"github.com/downfa11-org/cursus/pkg/types"
)

type MockISRManager struct {
	HasQuorumFunc func(topic string, partition int, required int) bool
}

func (m *MockISRManager) HasQuorum(t string, p int, r int) bool { return m.HasQuorumFunc(t, p, r) }
func (m *MockISRManager) UpdateHeartbeat(id string)             {}
func (m *MockISRManager) GetISR() []string                      { return nil }

func TestReplicateWithQuorum_FailsWhenNoQuorum(t *testing.T) {
	mockISR := &MockISRManager{
		HasQuorumFunc: func(topic string, partition int, required int) bool {
			return false
		},
	}

	rm := &RaftReplicationManager{
		isrManager: mockISR,
	}

	msg := types.Message{SeqNum: 1, Payload: "test"}
	_, err := rm.ReplicateWithQuorum("my-topic", 0, msg, 3)

	if err == nil {
		t.Fatal("Expected error due to lack of quorum, but got nil")
	}

	expectedError := "not enough in-sync replicas"
	if !strings.Contains(err.Error(), expectedError) {
		t.Errorf("Expected error message to contain '%s', got '%v'", expectedError, err)
	}
}

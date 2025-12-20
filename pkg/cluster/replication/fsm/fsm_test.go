package fsm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func newTestFSM() *BrokerFSM {
	fsm := NewBrokerFSM(nil, nil, nil)

	if fsm.brokers == nil {
		fsm.brokers = make(map[string]*BrokerInfo)
	}
	if fsm.partitionMetadata == nil {
		fsm.partitionMetadata = make(map[string]*PartitionMetadata)
	}
	if fsm.logs == nil {
		fsm.logs = make(map[uint64]*ReplicationEntry)
	}

	return fsm
}

func TestBrokerFSM_Apply_Register(t *testing.T) {
	fsm := newTestFSM()
	brokerInfo := BrokerInfo{ID: "b1", Addr: "127.0.0.1:9092", Status: "active", LastSeen: time.Now()}
	data, _ := json.Marshal(brokerInfo)

	log := &raft.Log{Data: []byte(fmt.Sprintf("REGISTER:%s", data)), Index: 1}

	result := fsm.Apply(log)
	if result != nil {
		t.Fatalf("Apply failed: %v", result)
	}

	brokers := fsm.GetBrokers()
	if len(brokers) != 1 || brokers[0].ID != "b1" {
		t.Errorf("Broker not registered correctly: %+v", brokers)
	}
}

func TestBrokerFSM_Apply_Register_InvalidPayload(t *testing.T) {
	fsm := newTestFSM()

	log := &raft.Log{
		Data:  []byte("REGISTER:{invalid-json"),
		Index: 1,
	}

	result := fsm.Apply(log)
	if result == nil {
		t.Fatal("expected error result for invalid REGISTER payload")
	}

	if len(fsm.GetBrokers()) != 0 {
		t.Fatal("broker should not be registered on invalid payload")
	}
}

func TestBrokerFSM_Apply_Deregister(t *testing.T) {
	fsm := newTestFSM()
	fsm.brokers["b1"] = &BrokerInfo{ID: "b1"}

	log := &raft.Log{Data: []byte("DEREGISTER:b1"), Index: 2}
	fsm.Apply(log)

	if len(fsm.GetBrokers()) != 0 {
		t.Error("Broker not deregistered")
	}
}

func TestBrokerFSM_Apply_Deregister_ReturnsNil(t *testing.T) {
	fsm := newTestFSM()
	fsm.brokers["b1"] = &BrokerInfo{ID: "b1"}

	log := &raft.Log{Data: []byte("DEREGISTER:b1"), Index: 2}
	result := fsm.Apply(log)

	if result != nil {
		t.Fatalf("DEREGISTER should return nil, got: %v", result)
	}

	if len(fsm.GetBrokers()) != 0 {
		t.Fatal("broker not deregistered")
	}
}

func TestBrokerFSM_Apply_Partition(t *testing.T) {
	fsm := newTestFSM()
	metadata := PartitionMetadata{Leader: "l1", Replicas: []string{"r1"}, LeaderEpoch: 1}
	data, _ := json.Marshal(metadata)
	key := "t1-0"

	log := &raft.Log{Data: []byte(fmt.Sprintf("PARTITION:%s:%s", key, data)), Index: 4}

	result := fsm.Apply(log)
	if result != nil {
		t.Fatalf("Apply failed: %v", result)
	}

	meta := fsm.GetPartitionMetadata(key)
	if meta == nil || meta.Leader != "l1" {
		t.Errorf("Partition metadata not updated correctly: %+v", meta)
	}
}

func TestBrokerFSM_Apply_UnknownCommand(t *testing.T) {
	fsm := newTestFSM()

	log := &raft.Log{
		Data:  []byte("UNKNOWN:payload"),
		Index: 1,
	}

	result := fsm.Apply(log)
	if result == nil {
		t.Fatal("expected error for unknown command")
	}
}

func TestBrokerFSM_Apply_UpdatesAppliedIndex(t *testing.T) {
	fsm := newTestFSM()

	log1 := &raft.Log{Data: []byte("DEREGISTER:x"), Index: 10}
	log2 := &raft.Log{Data: []byte("DEREGISTER:y"), Index: 11}

	fsm.Apply(log1)
	if fsm.applied != 10 {
		t.Fatalf("applied index not updated, expected 10 got %d", fsm.applied)
	}

	fsm.Apply(log2)
	if fsm.applied != 11 {
		t.Fatalf("applied index not monotonic, expected 11 got %d", fsm.applied)
	}
}

func TestBrokerFSM_Snapshot_Restore(t *testing.T) {
	fsm := newTestFSM()
	fsm.brokers["b1"] = &BrokerInfo{ID: "b1", Addr: "a1"}
	fsm.partitionMetadata["t1-0"] = &PartitionMetadata{Leader: "l1"}
	fsm.logs[5] = &ReplicationEntry{Topic: "t1"}
	fsm.applied = 5

	snapshot, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	buf := new(bytes.Buffer)
	sink := &MockSnapshotSink{Writer: buf}
	if err := snapshot.Persist(sink); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	newFSM := newTestFSM()
	rc := io.NopCloser(bytes.NewReader(buf.Bytes()))

	if err := newFSM.Restore(rc); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	if len(newFSM.brokers) != 1 || newFSM.brokers["b1"].Addr != "a1" {
		t.Errorf("Brokers not restored correctly: %+v", newFSM.brokers)
	}
	if len(newFSM.partitionMetadata) != 1 || newFSM.partitionMetadata["t1-0"].Leader != "l1" {
		t.Errorf("Metadata not restored correctly: %+v", newFSM.partitionMetadata)
	}
	if newFSM.applied != 5 {
		t.Errorf("Applied index not restored correctly: %d", newFSM.applied)
	}
}

func TestBrokerFSM_Snapshot_ClosesSink(t *testing.T) {
	fsm := newTestFSM()
	fsm.brokers["b1"] = &BrokerInfo{ID: "b1"}

	snapshot, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	buf := new(bytes.Buffer)
	sink := &MockSnapshotSink{Writer: buf}

	if err := snapshot.Persist(sink); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	if !sink.closed {
		t.Fatal("snapshot sink was not closed")
	}
}

type MockSnapshotSink struct {
	io.Writer
	closed bool
}

func (m *MockSnapshotSink) ID() string    { return "" }
func (m *MockSnapshotSink) Close() error  { m.closed = true; return nil }
func (m *MockSnapshotSink) Cancel() error { return nil }

package controller

import (
	"testing"

	"github.com/cursus-io/cursus/pkg/cluster/replication"
	"github.com/cursus-io/cursus/pkg/config"
	"github.com/cursus-io/cursus/pkg/topic"
	"github.com/cursus-io/cursus/pkg/types"
)

type MockLocalProcessor struct {
	processed bool
}

func (m *MockLocalProcessor) ProcessCommand(cmd string) string {
	m.processed = true
	return "OK"
}

type MockStorageHandler struct{}

func (m *MockStorageHandler) ReadMessages(o uint64, max int) ([]types.Message, error) {
	return nil, nil
}
func (m *MockStorageHandler) GetAbsoluteOffset() uint64      { return 0 }
func (m *MockStorageHandler) GetLatestOffset() uint64        { return 0 }
func (m *MockStorageHandler) GetSegmentPath(b uint64) string { return "" }
func (m *MockStorageHandler) AppendMessage(t string, p int, msg *types.Message) (uint64, error) {
	return 0, nil
}
func (m *MockStorageHandler) AppendMessageSync(t string, p int, msg *types.Message) (uint64, error) {
	return 0, nil
}
func (m *MockStorageHandler) WriteBatch(b []types.DiskMessage) error { return nil }
func (m *MockStorageHandler) Flush()                                 {}
func (m *MockStorageHandler) Close() error                           { return nil }

type FakeHandlerProvider struct{}

func (f *FakeHandlerProvider) GetHandler(t string, p int) (types.StorageHandler, error) {
	return &MockStorageHandler{}, nil
}

func TestClusterRouter_LocalProcess(t *testing.T) {
	cfg := &config.Config{LogDir: t.TempDir()}
	hp := &FakeHandlerProvider{}
	_ = topic.NewTopicManager(cfg, hp, nil)

	processor := &MockLocalProcessor{}

	rm := &replication.RaftReplicationManager{}
	router := NewClusterRouter("node1", "localhost:7000", processor, rm, 9000)

	if router.processLocally("CREATE topic=t1") != "OK" {
		t.Fatal("Expected local processing to succeed")
	}
	if !processor.processed {
		t.Fatal("Expected processor.processed to be true")
	}
}

package fsm

import (
	"encoding/json"

	"github.com/downfa11-org/go-broker/util"
	"github.com/hashicorp/raft"
)

type BrokerFSMSnapshot struct {
	applied           uint64
	logs              map[uint64]*ReplicationEntry
	brokers           map[string]*BrokerInfo
	partitionMetadata map[string]*PartitionMetadata
}

func (s *BrokerFSMSnapshot) Persist(sink raft.SnapshotSink) error {
	state := struct {
		Applied           uint64                        `json:"applied"`
		Logs              map[uint64]*ReplicationEntry  `json:"logs"`
		Brokers           map[string]*BrokerInfo        `json:"brokers"`
		PartitionMetadata map[string]*PartitionMetadata `json:"partitionMetadata"`
	}{
		Applied:           s.applied,
		Logs:              s.logs,
		Brokers:           s.brokers,
		PartitionMetadata: s.partitionMetadata,
	}

	util.Debug("Persisting snapshot data")
	err := json.NewEncoder(sink).Encode(state)
	if err != nil {
		cancelErr := sink.Cancel()
		if cancelErr != nil {
			util.Error("Failed to cancel snapshot after encoding error: %v", cancelErr)
		}
		return err
	}
	return sink.Close()
}

func (s *BrokerFSMSnapshot) Release() {}

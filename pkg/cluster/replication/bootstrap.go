package replication

import (
	"fmt"
	"strconv"

	"github.com/downfa11-org/go-broker/util"
	"github.com/hashicorp/raft"
)

func (rm *RaftReplicationManager) BootstrapCluster(peers []string) error {
	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(rm.brokerID),
				Address: raft.ServerAddress(rm.localAddr),
			},
		},
	}

	for _, peerAddr := range peers {
		configuration.Servers = append(configuration.Servers, raft.Server{
			ID:      raft.ServerID(generateIDFromAddr(peerAddr)),
			Address: raft.ServerAddress(peerAddr),
		})
	}

	future := rm.raft.BootstrapCluster(configuration)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to bootstrap cluster: %w", err)
	}

	return nil
}

func generateIDFromAddr(addr string) string {
	id := util.GenerateID(addr)
	return strconv.FormatUint(id, 10)
}

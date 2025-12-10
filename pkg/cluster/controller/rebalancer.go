package controller

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/downfa11-org/go-broker/pkg/cluster/replication"
	"github.com/downfa11-org/go-broker/util"
)

func (cc *ClusterController) RebalancePartitions() error {
	brokers, err := cc.discovery.DiscoverBrokers()
	if err != nil {
		return err
	}

	topics := cc.getAllTopics()
	for _, topic := range topics { // range over slice of strings
		if err := cc.rebalanceTopic(topic, brokers); err != nil {
			util.Error("Failed to rebalance topic %s: %v", topic, err)
		}
	}

	return nil
}

func (cc *ClusterController) rebalanceTopic(topic string, brokers []replication.BrokerInfo) error {
	partitionCount := cc.getPartitionCount(topic)

	cc.mu.Lock()
	defer cc.mu.Unlock()

	// round-robin
	leaderCount := make(map[string]int)
	for _, broker := range brokers {
		leaderCount[broker.Addr] = 0
	}

	for partition := 0; partition < partitionCount; partition++ {
		leader := cc.selectLeaderWithLeastLoad(brokers, leaderCount)
		key := fmt.Sprintf("%s-%d", topic, partition)
		cc.partitionLeaders[key] = leader.Addr
		leaderCount[leader.Addr]++

		if err := cc.raftManager.UpdatePartitionLeader(topic, partition, leader.Addr); err != nil {
			return err
		}
	}

	return nil
}

func (cc *ClusterController) ReassignPartition(topic string, partition int, targetReplicas []string) error {
	key := fmt.Sprintf("%s-%d", topic, partition)

	cc.mu.Lock()
	defer cc.mu.Unlock()

	metadata := cc.partitionMetadata[key]
	if metadata == nil {
		return fmt.Errorf("partition metadata not found")
	}

	metadata.Replicas = targetReplicas
	metadata.ISR = []string{metadata.Leader}

	data, _ := json.Marshal(metadata)
	future := cc.raftManager.GetRaft().Apply([]byte(fmt.Sprintf("PARTITION:%s:%s", key, string(data))), 5*time.Second)

	return future.Error()
}

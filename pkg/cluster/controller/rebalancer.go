package controller

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/downfa11-org/go-broker/pkg/cluster/replication"
	"github.com/downfa11-org/go-broker/util"
)

func (cc *ClusterController) RebalancePartitions() error {
	util.Info("Starting partition rebalancing")

	brokers, err := cc.discovery.DiscoverBrokers()
	if err != nil {
		util.Error("Failed to discover brokers for rebalancing: %v", err)
		return err
	}

	topics := cc.getAllTopics()
	util.Info("Rebalancing %d topics across %d brokers", len(topics), len(brokers))

	for _, topic := range topics { // range over slice of strings
		if err := cc.rebalanceTopic(topic, brokers); err != nil {
			util.Error("Failed to rebalance topic %s: %v", topic, err)
		}
	}

	util.Info("Partition rebalancing completed")
	return nil
}

func (cc *ClusterController) rebalanceTopic(topic string, brokers []replication.BrokerInfo) error {
	partitionCount := cc.getPartitionCount(topic)
	util.Debug("Rebalancing topic %s with %d partitions", topic, partitionCount)

	cc.mu.Lock()
	defer cc.mu.Unlock()

	// round-robin
	leaderCount := make(map[string]int)
	for _, broker := range brokers {
		leaderCount[broker.Addr] = 0
	}

	reassigned := 0
	for partition := 0; partition < partitionCount; partition++ {
		leader := cc.selectLeaderWithLeastLoad(brokers, leaderCount)
		key := fmt.Sprintf("%s-%d", topic, partition)
		cc.partitionLeaders[key] = leader.Addr
		leaderCount[leader.Addr]++
		reassigned++

		if err := cc.raftManager.UpdatePartitionLeader(topic, partition, leader.Addr); err != nil {
			util.Error("Failed to update leader for %s: %v", key, err)
			return err
		}
	}

	util.Debug("Reassigned %d partitions for topic %s", reassigned, topic)
	return nil
}

func (cc *ClusterController) ReassignPartition(topic string, partition int, targetReplicas []string) error {
	key := fmt.Sprintf("%s-%d", topic, partition)
	util.Info("Reassigning partition %s to replicas: %v", key, targetReplicas)

	cc.mu.Lock()
	defer cc.mu.Unlock()

	metadata := cc.partitionMetadata[key]
	if metadata == nil {
		util.Error("Partition metadata not found for %s", key)
		return fmt.Errorf("partition metadata not found")
	}

	metadata.Replicas = targetReplicas
	metadata.ISR = []string{metadata.Leader}

	data, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal partition metadata: %w", err)
	}
	future := cc.raftManager.GetRaft().Apply([]byte(fmt.Sprintf("PARTITION:%s:%s", key, string(data))), 5*time.Second)

	if err := future.Error(); err != nil {
		util.Error("Failed to apply partition reassignment for %s: %v", key, err)
		return err
	}

	util.Info("Successfully reassigned partition %s", key)
	return nil
}

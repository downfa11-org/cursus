package fsm

import (
	"encoding/json"
	"fmt"

	"github.com/downfa11-org/go-broker/util"
)

type PartitionMetadata struct {
	Leader         string
	Replicas       []string
	ISR            []string
	LeaderEpoch    int64
	PartitionCount int
}

func (f *BrokerFSM) applyRegisterCommand(jsonData string) error {
	var broker BrokerInfo
	if err := json.Unmarshal([]byte(jsonData), &broker); err != nil {
		util.Error("Failed to unmarshal broker registration: %v", err)
		return err
	}

	f.storeBroker(broker.ID, &broker)
	util.Info("Registered broker %s at %s", broker.ID, broker.Addr)
	return nil
}

func (f *BrokerFSM) applyDeregisterCommand(brokerID string) error {
	f.removeBroker(brokerID)
	util.Info("Deregistered broker %s", brokerID)
	return nil
}

func (f *BrokerFSM) applyMessageCommand(index uint64, jsonData string) error {
	entry, err := f.parseMessageEntry(jsonData)
	if err != nil {
		util.Error("Failed to parse message entry at index %d: %v", index, err)
		return err
	}

	if f.tm == nil {
		util.Error("TopicManager not initialized in FSM.")
		return fmt.Errorf("TopicManager not initialized")
	}

	if t := f.tm.GetTopic(entry.Topic); t == nil {
		util.Error("FATAL: Topic '%s' does not exist in TopicManager when applying log index %d.", entry.Topic, index)
		return fmt.Errorf("topic '%s' not found during MESSAGE apply at index %d", entry.Topic, index)
	}

	f.storeLogEntry(index, entry)

	err = f.persistMessage(entry)
	if err != nil {
		util.Error("Failed to persist message for topic %s partition %d: %v", entry.Topic, entry.Partition, err)
		return err
	}

	util.Info("Successfully replicated message for topic %s partition %d at index %d", entry.Topic, entry.Partition, index)
	return nil
}

func (f *BrokerFSM) applyTopicCommand(data string) interface{} {
	var topicCmd struct {
		Name       string `json:"name"`
		Partitions int    `json:"partitions"`
	}

	if err := json.Unmarshal([]byte(data), &topicCmd); err != nil {
		util.Error("Failed to unmarshal topic command: %v", err)
		return err
	}

	f.partitionMetadata[topicCmd.Name] = &PartitionMetadata{
		PartitionCount: topicCmd.Partitions,
	}

	if f.tm != nil {
		f.tm.CreateTopic(topicCmd.Name, topicCmd.Partitions)
		util.Info("Successfully created topic '%s' with %d partitions on follower", topicCmd.Name, topicCmd.Partitions)
	}

	return nil
}

func (f *BrokerFSM) applyPartitionCommand(data string) error {
	key, metadata, err := f.parsePartitionCommand(data)
	if err != nil {
		return err
	}

	f.storePartitionMetadata(key, metadata)
	util.Debug("Updated partition metadata for %s", key)
	return nil
}

func (f *BrokerFSM) handleUnknownCommand(data string) interface{} {
	preview := data
	if len(preview) > 20 {
		preview = preview[:20]
	}
	util.Debug("Unknown log entry type: %s", preview)
	return nil
}

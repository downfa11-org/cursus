package fsm

import (
	"encoding/json"
	"fmt"

	"github.com/downfa11-org/go-broker/pkg/coordinator"
	"github.com/downfa11-org/go-broker/pkg/types"
	"github.com/downfa11-org/go-broker/util"
)

type PartitionMetadata struct {
	Leader         string // todo
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

	if broker.ID == "" || broker.Addr == "" {
		return fmt.Errorf("invalid broker registration: %+v", broker)
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

func (f *BrokerFSM) applyTopicCommand(data string) interface{} {
	var topicCmd struct {
		Name       string `json:"name"`
		Partitions int    `json:"partitions"`
		LeaderID   string `json:"leader_id"`
	}

	if err := json.Unmarshal([]byte(data), &topicCmd); err != nil {
		util.Error("Failed to unmarshal topic command: %v", err)
		return err
	}

	leader := topicCmd.LeaderID
	if leader == "" {
		leader = f.getCurrentRaftLeaderID()
	}

	f.mu.Lock()
	f.partitionMetadata[topicCmd.Name] = &PartitionMetadata{
		PartitionCount: topicCmd.Partitions,
		Leader:         leader,
		LeaderEpoch:    1,
	}
	f.mu.Unlock()

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
	util.Error("Unknown log entry type: %s", preview)
	return fmt.Errorf("unknown command: %s", preview)
}

func (f *BrokerFSM) applyMessageCommand(jsonData string) interface{} {
	util.Debug("FSM received Raft command JSON: %s", jsonData)

	cmd, err := parseMessageCommand(jsonData)
	if err != nil {
		return errorAckResponse(err.Error(), "", 0)
	}

	if err := validateMessageCommand(cmd); err != nil {
		return errorAckResponse(err.Error(), cmd.Messages[0].ProducerID, cmd.Messages[0].Epoch)
	}

	return f.applyMessage(cmd)
}

func (f *BrokerFSM) applyMessage(cmd *MessageCommand) interface{} {
	if len(cmd.Messages) == 1 {
		return f.applySingle(cmd)
	}
	return f.applyBatch(cmd)
}

func (f *BrokerFSM) applySingle(cmd *MessageCommand) interface{} {
	msg := &cmd.Messages[0]

	if err := f.persistMessage(cmd.Topic, cmd.Partition, msg); err != nil {
		return errorAckResponse(err.Error(), msg.ProducerID, msg.Epoch)
	}

	return types.AckResponse{
		Status:        "OK",
		LastOffset:    msg.Offset,
		ProducerID:    msg.ProducerID,
		ProducerEpoch: msg.Epoch,
		SeqStart:      msg.SeqNum,
		SeqEnd:        msg.SeqNum,
	}
}

func (f *BrokerFSM) applyBatch(cmd *MessageCommand) interface{} {
	if err := f.persistBatch(cmd.Topic, cmd.Partition, cmd.Messages); err != nil {
		m := cmd.Messages[0]
		return errorAckResponse(fmt.Sprintf("batch persist failed: %v", err), m.ProducerID, m.Epoch)
	}

	first := cmd.Messages[0]
	last := cmd.Messages[len(cmd.Messages)-1]

	return types.AckResponse{
		Status:        "OK",
		LastOffset:    last.Offset,
		ProducerID:    first.ProducerID,
		ProducerEpoch: first.Epoch,
		SeqStart:      first.SeqNum,
		SeqEnd:        last.SeqNum,
	}
}

func (f *BrokerFSM) applyGroupSyncCommand(jsonData string) interface{} {
	var cmd struct {
		Type   string `json:"type"` // JOIN or LEAVE
		Group  string `json:"group"`
		Member string `json:"member"`
		Topic  string `json:"topic"`
	}

	if err := json.Unmarshal([]byte(jsonData), &cmd); err != nil {
		util.Error("Failed to unmarshal group sync: %v", err)
		return err
	}

	if f.cd == nil {
		util.Warn("Coordinator not set in FSM, skipping group sync")
		return nil
	}

	switch cmd.Type {
	case "JOIN":
		if f.cd.GetGroup(cmd.Group) == nil {
			util.Info("FSM: Group '%s' not found, creating implicitly for topic '%s'", cmd.Group, cmd.Topic)

			partitionCount := 4 // default
			if t := f.tm.GetTopic(cmd.Topic); t != nil {
				partitionCount = len(t.Partitions)
			}

			if err := f.cd.RegisterGroup(cmd.Topic, cmd.Group, partitionCount); err != nil {
				util.Error("FSM: Failed to implicitly register group: %v", err)
				return err
			}
		}

		_, err := f.cd.AddConsumer(cmd.Group, cmd.Member)
		if err != nil {
			util.Error("FSM: Failed to sync JOIN for member %s: %v", cmd.Member, err)
		} else {
			util.Info("FSM: Synced JOIN group=%s member=%s", cmd.Group, cmd.Member)
		}
	case "LEAVE":
		err := f.cd.RemoveConsumer(cmd.Group, cmd.Member)
		if err != nil {
			util.Warn("FSM: LEAVE failed (potentially already removed): %v", err)
		} else {
			util.Info("FSM: Synced LEAVE group=%s member=%s", cmd.Group, cmd.Member)
		}
	}
	return nil
}

func (f *BrokerFSM) applyOffsetSyncCommand(jsonData string) interface{} {
	var cmd struct {
		Group     string `json:"group"`
		Topic     string `json:"topic"`
		Partition int    `json:"partition"`
		Offset    uint64 `json:"offset"`
	}

	if err := json.Unmarshal([]byte(jsonData), &cmd); err != nil {
		util.Error("Failed to unmarshal offset sync: %v", err)
		return err
	}

	if f.cd == nil {
		return nil
	}

	err := f.cd.CommitOffset(cmd.Group, cmd.Topic, cmd.Partition, cmd.Offset)
	if err != nil {
		util.Error("FSM: Failed to sync offset: %v", err)
		return err
	}

	util.Debug("FSM: Synced OFFSET group=%s topic=%s p=%d o=%d", cmd.Group, cmd.Topic, cmd.Partition, cmd.Offset)
	return nil
}

func (f *BrokerFSM) applyBatchOffsetSyncCommand(jsonData string) interface{} {
	var cmd struct {
		Group   string                   `json:"group"`
		Topic   string                   `json:"topic"`
		Offsets []coordinator.OffsetItem `json:"offsets"`
	}

	if err := json.Unmarshal([]byte(jsonData), &cmd); err != nil {
		util.Error("Failed to unmarshal batch offset sync: %v", err)
		return err
	}

	if f.cd == nil {
		return nil
	}

	err := f.cd.CommitOffsetsBulk(cmd.Group, cmd.Topic, cmd.Offsets)
	if err != nil {
		util.Error("FSM: Failed to sync batch offsets: %v", err)
		return err
	}

	util.Debug("FSM: Synced BATCH_OFFSET group=%s topic=%s count=%d",
		cmd.Group, cmd.Topic, len(cmd.Offsets))
	return nil
}

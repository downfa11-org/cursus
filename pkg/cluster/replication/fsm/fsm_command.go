package fsm

import (
	"encoding/json"
	"fmt"
	"strings"

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
	util.Debug("Unknown log entry type: %s", preview)
	return nil
}

func (f *BrokerFSM) applyMessageCommand(jsonData string) interface{} {
	util.Debug("FSM received Raft command JSON: %s", jsonData)

	messageData := make(map[string]interface{})
	decoder := json.NewDecoder(strings.NewReader(jsonData))
	decoder.UseNumber()

	if err := decoder.Decode(&messageData); err != nil {
		util.Error("Failed to unmarshal message data: %v", err)
		return types.AckResponse{
			Status:   "ERROR",
			ErrorMsg: fmt.Sprintf("invalid JSON format: %v", err),
		}
	}

	if messageData["Messages"] != nil {
		if messages, ok := messageData["Messages"].([]interface{}); ok {
			return f.applyBatchMessage(messageData, messages)
		}
	}

	return f.applySingleMessage(messageData)
}

func (f *BrokerFSM) applySingleMessage(messageData map[string]interface{}) interface{} {
	topicName, err := getStringField(messageData, "Topic")
	if err != nil {
		return errorAckResponse(err.Error(), "", 0)
	}

	partition, err := getIntField(messageData, "Partition")
	if err != nil {
		return errorAckResponse(err.Error(), "", 0)
	}

	msg := &types.Message{
		Payload:    getOptionalStringField(messageData, "Payload"),
		ProducerID: getOptionalStringField(messageData, "ProducerID"),
		Offset:     uint64(getOptionalInt64Field(messageData, "Offset")),
		SeqNum:     getOptionalUint64Field(messageData, "SeqNum"),
		Epoch:      getOptionalInt64Field(messageData, "Epoch"),
	}

	if err := f.persistMessage(topicName, partition, msg); err != nil {
		util.Error("FSM failed to persist message: %v", err)
		return errorAckResponse(fmt.Sprintf("persist failed: %v", err), msg.ProducerID, msg.Epoch)
	}

	return types.AckResponse{
		Status:        "OK",
		LastOffset:    msg.Offset,
		ProducerEpoch: msg.Epoch,
		ProducerID:    msg.ProducerID,
		SeqStart:      msg.SeqNum,
		SeqEnd:        msg.SeqNum,
	}
}

func (f *BrokerFSM) applyBatchMessage(messageData map[string]interface{}, messages []interface{}) interface{} {
	if len(messages) == 0 {
		return errorAckResponse("empty batch", "", 0)
	}

	topicName, err := getStringField(messageData, "Topic")
	if err != nil {
		return errorAckResponse(err.Error(), "", 0)
	}

	partition, err := getIntField(messageData, "Partition")
	if err != nil {
		return errorAckResponse(err.Error(), "", 0)
	}

	batchMsgs := make([]types.Message, 0, len(messages))
	for i, msgInterface := range messages {
		msgData, ok := msgInterface.(map[string]interface{})
		if !ok {
			return errorAckResponse(fmt.Sprintf("invalid message at index %d", i), "", batchMsgs[0].Epoch)
		}

		batchMsgs = append(batchMsgs, types.Message{
			Payload:    getOptionalStringField(msgData, "Payload"),
			Offset:     uint64(getOptionalInt64Field(msgData, "Offset")),
			ProducerID: getOptionalStringField(msgData, "ProducerID"),
			SeqNum:     getOptionalUint64Field(msgData, "SeqNum"),
			Epoch:      getOptionalInt64Field(msgData, "Epoch"),
		})
	}

	if err := f.persistBatch(topicName, partition, batchMsgs); err != nil {
		util.Error("FSM failed to persist batch: %v", err)
		return errorAckResponse(fmt.Sprintf("batch persist failed: %v", err), batchMsgs[0].ProducerID, batchMsgs[0].Epoch)
	}

	lastOffset := batchMsgs[len(batchMsgs)-1].Offset
	seqStart := batchMsgs[0].SeqNum
	seqEnd := batchMsgs[len(batchMsgs)-1].SeqNum

	util.Debug("FSM Ack Return - ProducerID: %s, ProducerEpoch: %d", batchMsgs[0].ProducerID, batchMsgs[0].Epoch)

	return types.AckResponse{
		Status:        "OK",
		LastOffset:    lastOffset,
		ProducerEpoch: batchMsgs[0].Epoch,
		ProducerID:    batchMsgs[0].ProducerID,
		SeqStart:      seqStart,
		SeqEnd:        seqEnd,
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

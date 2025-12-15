package fsm

import (
	"encoding/json"
	"fmt"

	"github.com/downfa11-org/go-broker/pkg/types"
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

func (f *BrokerFSM) applyMessageCommand(jsonData string) interface{} {
	messageData := make(map[string]interface{})
	if err := json.Unmarshal([]byte(jsonData), &messageData); err != nil {
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
	topicName, err := getStringField(messageData, "topic")
	if err != nil {
		return errorAckResponse(err.Error())
	}

	partition, err := getIntField(messageData, "partition")
	if err != nil {
		return errorAckResponse(err.Error())
	}

	msg := &types.Message{
		Payload:    getOptionalStringField(messageData, "payload"),
		ProducerID: getOptionalStringField(messageData, "producerId"),
		SeqNum:     getOptionalUint64Field(messageData, "seqNum"),
		Epoch:      getOptionalInt64Field(messageData, "epoch"),
	}

	if err := f.persistMessage(topicName, partition, msg); err != nil {
		util.Error("FSM failed to persist message: %v", err)
		return errorAckResponse(fmt.Sprintf("persist failed: %v", err))
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
		return errorAckResponse("empty batch")
	}

	topicName, err := getStringField(messageData, "Topic")
	if err != nil {
		return errorAckResponse(err.Error())
	}

	partition, err := getIntField(messageData, "Partition")
	if err != nil {
		return errorAckResponse(err.Error())
	}

	batchMsgs := make([]types.Message, 0, len(messages))
	for i, msgInterface := range messages {
		msgData, ok := msgInterface.(map[string]interface{})
		if !ok {
			return errorAckResponse(fmt.Sprintf("invalid message at index %d", i))
		}

		batchMsgs = append(batchMsgs, types.Message{
			Payload:    getOptionalStringField(msgData, "Payload"),
			ProducerID: getOptionalStringField(msgData, "ProducerId"),
			SeqNum:     getOptionalUint64Field(msgData, "SeqNum"),
			Epoch:      getOptionalInt64Field(msgData, "Epoch"),
		})
	}

	if err := f.persistBatch(topicName, partition, batchMsgs); err != nil {
		util.Error("FSM failed to persist batch: %v", err)
		return errorAckResponse(fmt.Sprintf("batch persist failed: %v", err))
	}

	lastOffset := batchMsgs[len(batchMsgs)-1].Offset
	seqStart := batchMsgs[0].SeqNum
	seqEnd := batchMsgs[len(batchMsgs)-1].SeqNum

	return types.AckResponse{
		Status:        "OK",
		LastOffset:    lastOffset,
		ProducerEpoch: batchMsgs[0].Epoch,
		ProducerID:    batchMsgs[0].ProducerID,
		SeqStart:      seqStart,
		SeqEnd:        seqEnd,
	}
}

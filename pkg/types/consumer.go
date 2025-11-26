package types

import "time"

type ConsumerState int

const (
	ConsumerStateActive ConsumerState = iota
	ConsumerStateDead
	ConsumerStateRebalancing
)

// Consumer represents a single consumer instance in a group.
type Consumer struct {
	ID                 int
	MsgCh              chan Message
	LastHeartbeat      time.Time
	AssignedPartitions []int
	State              ConsumerState
	StopCh             chan struct{}
}

// ConsumerGroup contains consumers subscribed to the same topic.
type ConsumerGroup struct {
	Name             string
	Consumers        []*Consumer
	CommittedOffsets map[int]int64
}

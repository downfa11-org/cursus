package types

// Message represents a single message
type Message struct {
	ID         uint64
	ProducerID string
	SeqNum     uint64
	Payload    string
	Offset     uint64
	Key        string // optional: partition routing key
	Epoch      int64
}

func (m Message) String() string {
	return m.Payload
}

type Batch struct {
	Topic      string
	Partition  int32
	BatchStart uint64
	BatchEnd   uint64
	Acks       int
	Messages   []Message
}

// AppendResult represents the result of appending a message to storage
type AppendResult struct {
	SegmentIndex int
	Offset       int
}

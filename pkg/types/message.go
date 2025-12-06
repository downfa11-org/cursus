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
	RetryCount int
	Retry      bool
	Value      []byte
}

func (m Message) String() string {
	return m.Payload
}

type Batch struct {
	Topic      string
	Partition  int
	BatchStart uint64
	BatchEnd   uint64
	Messages   []Message
}

// AppendResult represents the result of appending a message to storage
type AppendResult struct {
	SegmentIndex int
	Offset       int
}

type AckResponse struct {
	Status        string `json:"status"`
	LastOffset    uint64 `json:"last_offset"`
	ProducerEpoch int64  `json:"producer_epoch"`
	ProducerID    string `json:"producer_id"`
	SeqStart      uint64 `json:"seq_start"`
	SeqEnd        uint64 `json:"seq_end"`
	ErrorMsg      string `json:"error,omitempty"`
}

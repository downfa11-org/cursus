package types

type Message struct {
	Offset     uint64
	ProducerID string
	SeqNum     uint64
	Payload    string
	Key        string // optional: partition routing key
	Epoch      int64

	RetryCount int
	Retry      bool
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

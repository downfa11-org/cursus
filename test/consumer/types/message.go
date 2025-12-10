package types

type Message struct {
	Offset     uint64
	ProducerID string
	SeqNum     uint64
	Payload    string
	Key        string // optional: partition routing key
	Epoch      int64
}

type Batch struct {
	Topic      string
	Partition  int
	BatchStart uint64
	BatchEnd   uint64
	Messages   []Message
}

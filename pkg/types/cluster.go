package types

type MessageCommand struct {
	Topic        string    `json:"topic"`
	Partition    int       `json:"partition"`
	Messages     []Message `json:"messages"`
	Acks         string    `json:"acks"`
	IsIdempotent bool      `json:"is_idempotent"`
}

type BatchMessageCommand struct {
	Topic     string    `json:"topic"`
	Partition int       `json:"partition"`
	Messages  []Message `json:"messages"`
	Acks      string    `json:"acks"`
}

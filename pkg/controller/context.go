package controller

type ClientContext struct {
	ConsumerGroup string
	ConsumerIdx   int
	CurrentTopics map[string]struct{}
}

func NewClientContext(group string, idx int) *ClientContext {
	return &ClientContext{
		ConsumerGroup: group,
		ConsumerIdx:   idx,
		CurrentTopics: make(map[string]struct{}),
	}
}

func (ctx *ClientContext) SetConsumerGroup(groupName string) {
	ctx.ConsumerGroup = groupName
}

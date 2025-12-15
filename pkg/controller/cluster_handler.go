package controller

func (ch *CommandHandler) ProcessCommand(cmd string) string {
	ctx := NewClientContext("default-group", 0)
	return ch.HandleCommand(cmd, ctx)
}

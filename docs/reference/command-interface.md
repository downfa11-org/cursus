# Command Interface

Clients interact with go-broker through a text-based command protocol over TCP:

| Command | Example | Description |
|---------|---------|-------------|
| CREATE | `CREATE topic1 4` | Create topic with 4 partitions |
| DELETE | `DELETE topic1` | Delete topic |
| LIST | `LIST` | List all topics |
| SUBSCRIBE | `SUBSCRIBE topic1 group1 3` | Register consumer group with 3 consumers |
| PUBLISH | `PUBLISH topic1 message` | Publish message to topic |
| CONSUME | `CONSUME topic1 0 0` | Consume from partition 0 at offset 0 |

Commands are processed by controller.CommandHandler, which maintains references to both TopicManager and DiskManager.

The handler routes commands to appropriate subsystems based on command type.

- Message framing: All TCP communication uses _4-byte big-endian_ length prefixes, enabling reliable streaming over TCP.


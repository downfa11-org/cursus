package fsm

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/downfa11-org/go-broker/pkg/types"
	"github.com/downfa11-org/go-broker/util"
)

func errorAckResponse(msg string) types.AckResponse {
	return types.AckResponse{
		Status:   "ERROR",
		ErrorMsg: msg,
	}
}

func (f *BrokerFSM) parsePartitionCommand(data string) (string, *PartitionMetadata, error) {
	parts := strings.SplitN(data, ":", 3)

	if len(parts) != 3 {
		return "", nil, fmt.Errorf("invalid PARTITION command format: expected PARTITION:key:json")
	}

	key := parts[1]
	var metadata PartitionMetadata
	if err := json.Unmarshal([]byte(parts[2]), &metadata); err != nil {
		util.Error("Failed to unmarshal partition metadata: %v", err)
		return "", nil, err
	}

	return key, &metadata, nil
}

func getStringField(data map[string]interface{}, key string) (string, error) {
	if val, ok := data[key]; ok {
		if str, ok := val.(string); ok {
			return str, nil
		}
		return "", fmt.Errorf("field '%s' is not a string", key)
	}
	return "", fmt.Errorf("missing required field: %s", key)
}

func getIntField(data map[string]interface{}, key string) (int, error) {
	if val, ok := data[key]; ok {
		switch v := val.(type) {
		case float64:
			return int(v), nil
		case int:
			return v, nil
		case int64:
			return int(v), nil
		default:
			return 0, fmt.Errorf("field '%s' is not a number", key)
		}
	}
	return 0, fmt.Errorf("missing required field: %s", key)
}

func getOptionalStringField(data map[string]interface{}, key string) string {
	if val, ok := data[key]; ok {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return ""
}

func getOptionalUint64Field(data map[string]interface{}, key string) uint64 {
	if val, ok := data[key]; ok {
		switch v := val.(type) {
		case float64:
			return uint64(v)
		case uint64:
			return v
		case int64:
			return uint64(v)
		}
	}
	return 0
}

func getOptionalInt64Field(data map[string]interface{}, key string) int64 {
	if val, ok := data[key]; ok {
		switch v := val.(type) {
		case float64:
			return int64(v)
		case int64:
			return v
		case uint64:
			return int64(v)
		}
	}
	return 0
}

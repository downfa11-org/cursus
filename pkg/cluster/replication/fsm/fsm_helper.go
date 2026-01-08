package fsm

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/downfa11-org/go-broker/pkg/types"
	"github.com/downfa11-org/go-broker/util"
)

func errorAckResponse(msg, producerID string, epoch int64) types.AckResponse {
	return types.AckResponse{
		Status:        "ERROR",
		ErrorMsg:      msg,
		ProducerID:    producerID,
		ProducerEpoch: epoch,
	}
}

func (f *BrokerFSM) parsePartitionCommand(data string) (string, *PartitionMetadata, error) {
	startIdx := strings.Index(data, "{")
	if startIdx == -1 {
		return "", nil, fmt.Errorf("invalid PARTITION command: JSON metadata not found")
	}

	prefix := data[:startIdx]
	prefix = strings.TrimPrefix(prefix, "PARTITION:")
	key := strings.TrimSuffix(prefix, ":")

	if key == "" {
		return "", nil, fmt.Errorf("invalid PARTITION command: missing key")
	}

	var metadata PartitionMetadata
	dec := json.NewDecoder(strings.NewReader(data[startIdx:]))
	if err := dec.Decode(&metadata); err != nil {
		util.Error("Failed to unmarshal partition metadata for key %s: %v", key, err)
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
		case json.Number:
			i64, err := v.Int64()
			if err == nil {
				return int(i64), nil
			}
			return 0, fmt.Errorf("field '%s' is an invalid number format", key)
		case float64:
			return int(v), nil
		case int:
			return v, nil
		case int64:
			return int(v), nil
		case string:
			i, err := strconv.Atoi(v)
			if err == nil {
				return i, nil
			}
			return 0, fmt.Errorf("field '%s' is an invalid number string", key)
		default:
			return 0, fmt.Errorf("field '%s' is not a number", key)
		}
	}
	return 0, fmt.Errorf("missing required field: %s", key)
}

func getOptionalStringField(data map[string]interface{}, key string) (string, error) {
	val, ok := data[key]
	if !ok || val == nil {
		return "", nil
	}

	if s, ok := val.(string); ok {
		return s, nil
	}

	if f, ok := val.(float64); ok {
		return fmt.Sprintf("%.0f", f), nil
	}

	return "", fmt.Errorf("field '%s' is not a string", key)
}

func getOptionalUint64Field(m map[string]interface{}, key string) (uint64, error) {
	val, ok := m[key]
	if !ok || val == nil {
		return 0, nil
	}

	switch v := val.(type) {
	case uint64:
		return v, nil

	case int64:
		if v < 0 {
			return 0, fmt.Errorf("negative int64 for uint64 field '%s': %d", key, v)
		}
		return uint64(v), nil

	case float64:
		if v < 0 {
			return 0, fmt.Errorf("negative float64 for uint64 field '%s': %f", key, v)
		}
		return uint64(v), nil

	case json.Number:
		i, err := v.Int64()
		if err != nil {
			return 0, fmt.Errorf("invalid json.Number for uint64 field '%s'", key)
		}
		if i < 0 {
			return 0, fmt.Errorf("negative json.Number for uint64 field '%s': %d", key, i)
		}
		return uint64(i), nil

	default:
		return 0, fmt.Errorf("field '%s' is not a uint64-compatible type", key)
	}
}

func getOptionalInt64Field(data map[string]interface{}, key string) (int64, error) {
	val, ok := data[key]
	if !ok || val == nil {
		return 0, nil
	}

	switch v := val.(type) {
	case json.Number:
		i64, err := v.Int64()
		if err != nil {
			return 0, fmt.Errorf("invalid json.Number for int64 field '%s'", key)
		}
		return i64, nil

	case float64:
		if v > float64(math.MaxInt64) || v < float64(math.MinInt64) {
			return 0, fmt.Errorf("float64 overflow/underflow for int64 field '%s': %f", key, v)
		}
		return int64(v), nil

	case int64:
		return v, nil

	case uint64:
		if v > uint64(math.MaxInt64) {
			return 0, fmt.Errorf("uint64 overflow for int64 field '%s': %d", key, v)
		}
		return int64(v), nil

	default:
		return 0, fmt.Errorf("field '%s' is not an int64-compatible type", key)
	}
}

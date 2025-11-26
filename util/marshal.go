package util

import (
	"encoding/json"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

type LogLevel int

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarn
	LogLevelError
)

func parseLogLevelString(s string) LogLevel {
	switch strings.ToLower(s) {
	case "debug":
		return LogLevelDebug
	case "info":
		return LogLevelInfo
	case "warn", "warning":
		return LogLevelWarn
	case "error":
		return LogLevelError
	default:
		return LogLevelInfo
	}
}

// UnmarshalYAML implements custom YAML unmarshaling for LogLevel
func (l *LogLevel) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err == nil {
		*l = parseLogLevelString(s)
		return nil
	}

	var i int
	if err := value.Decode(&i); err != nil {
		return fmt.Errorf("log_level must be a string (debug/info/warn/error) or integer (0-3)")
	}
	if i < 0 || i > int(LogLevelError) {
		return fmt.Errorf("log_level integer must be between 0 and %d", LogLevelError)
	}
	*l = LogLevel(i)
	return nil
}

// UnmarshalJSON implements custom JSON unmarshaling for LogLevel
func (l *LogLevel) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		*l = parseLogLevelString(s)
		return nil
	}

	var i int
	if err := json.Unmarshal(data, &i); err != nil {
		return fmt.Errorf("log_level must be a string (debug/info/warn/error) or integer (0-3)")
	}
	if i < 0 || i > int(LogLevelError) {
		return fmt.Errorf("log_level integer must be between 0 and %d", LogLevelError)
	}
	*l = LogLevel(i)
	return nil
}

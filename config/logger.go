package config

import "fmt"

// LogLevel represents the logging verbosity level
type LogLevel string

const (
	LogLevelSilent  LogLevel = "silent"  // No logging output
	LogLevelError   LogLevel = "error"   // Only errors
	LogLevelInfo    LogLevel = "info"    // Info, warnings, and errors
	LogLevelDebug   LogLevel = "debug"   // Debug info + all above
	LogLevelVerbose LogLevel = "verbose" // All details including trace
)

// LoggerConfig holds the configuration for logging
type LoggerConfig struct {
	Level      LogLevel `json:"level" yaml:"level" toml:"level"`                                        // Log level
	AddSource  bool     `json:"add_source,omitempty" yaml:"add_source,omitempty" toml:"add_source"`    // Include source file and line number
	TimeFormat string   `json:"time_format,omitempty" yaml:"time_format,omitempty" toml:"time_format"` // Time format (empty for no timestamp)
}

// Validate validates the logger configuration
func (lc *LoggerConfig) Validate() error {
	switch lc.Level {
	case LogLevelSilent, LogLevelError, LogLevelInfo, LogLevelDebug, LogLevelVerbose:
		// Valid levels
	case "":
		// Empty is OK, will be set to default in ApplyDefaults
	default:
		return fmt.Errorf("invalid log level: %s (must be one of: silent, error, info, debug, verbose)", lc.Level)
	}
	return nil
}

// ApplyDefaults sets default values for logger configuration
func (lc *LoggerConfig) ApplyDefaults() {
	if lc.Level == "" {
		lc.Level = LogLevelInfo // Default to info level
	}
	if lc.TimeFormat == "" {
		lc.TimeFormat = "2006-01-02 15:04:05" // Default time format
	}
}

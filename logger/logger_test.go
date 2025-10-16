package logger

import (
	"bytes"
	"strings"
	"testing"

	"github.com/olegkotsar/yomins-sync/config"
	"github.com/stretchr/testify/require"
)

func TestNewLogger(t *testing.T) {
	cfg := &config.LoggerConfig{
		Level: config.LogLevelInfo,
	}
	logger := NewLogger(cfg)
	require.NotNil(t, logger)
}

func TestLogLevel_Silent(t *testing.T) {
	var buf bytes.Buffer
	cfg := &config.LoggerConfig{
		Level: config.LogLevelSilent,
	}
	logger := NewLoggerWithWriter(cfg, &buf)

	logger.Error("error message")
	logger.Warn("warn message")
	logger.Info("info message")
	logger.Debug("debug message")
	logger.Verbose("verbose message")

	// Silent level should not log anything
	require.Empty(t, buf.String())
}

func TestLogLevel_Error(t *testing.T) {
	var buf bytes.Buffer
	cfg := &config.LoggerConfig{
		Level:      config.LogLevelError,
		TimeFormat: "", // Disable timestamp for easier testing
	}
	logger := NewLoggerWithWriter(cfg, &buf)

	logger.Error("error message")
	logger.Warn("warn message")
	logger.Info("info message")
	logger.Debug("debug message")
	logger.Verbose("verbose message")

	output := buf.String()
	require.Contains(t, output, "error message")
	require.NotContains(t, output, "warn message")
	require.NotContains(t, output, "info message")
	require.NotContains(t, output, "debug message")
	require.NotContains(t, output, "verbose message")
}

func TestLogLevel_Info(t *testing.T) {
	var buf bytes.Buffer
	cfg := &config.LoggerConfig{
		Level:      config.LogLevelInfo,
		TimeFormat: "", // Disable timestamp for easier testing
	}
	logger := NewLoggerWithWriter(cfg, &buf)

	logger.Error("error message")
	logger.Warn("warn message")
	logger.Info("info message")
	logger.Debug("debug message")
	logger.Verbose("verbose message")

	output := buf.String()
	require.Contains(t, output, "error message")
	require.Contains(t, output, "warn message")
	require.Contains(t, output, "info message")
	require.NotContains(t, output, "debug message")
	require.NotContains(t, output, "verbose message")
}

func TestLogLevel_Debug(t *testing.T) {
	var buf bytes.Buffer
	cfg := &config.LoggerConfig{
		Level:      config.LogLevelDebug,
		TimeFormat: "", // Disable timestamp for easier testing
	}
	logger := NewLoggerWithWriter(cfg, &buf)

	logger.Error("error message")
	logger.Warn("warn message")
	logger.Info("info message")
	logger.Debug("debug message")
	logger.Verbose("verbose message")

	output := buf.String()
	require.Contains(t, output, "error message")
	require.Contains(t, output, "warn message")
	require.Contains(t, output, "info message")
	require.Contains(t, output, "debug message")
	require.NotContains(t, output, "verbose message")
}

func TestLogLevel_Verbose(t *testing.T) {
	var buf bytes.Buffer
	cfg := &config.LoggerConfig{
		Level:      config.LogLevelVerbose,
		TimeFormat: "", // Disable timestamp for easier testing
	}
	logger := NewLoggerWithWriter(cfg, &buf)

	logger.Error("error message")
	logger.Warn("warn message")
	logger.Info("info message")
	logger.Debug("debug message")
	logger.Verbose("verbose message")

	output := buf.String()
	require.Contains(t, output, "error message")
	require.Contains(t, output, "warn message")
	require.Contains(t, output, "info message")
	require.Contains(t, output, "debug message")
	require.Contains(t, output, "verbose message")
}

func TestLogger_WithFormatting(t *testing.T) {
	var buf bytes.Buffer
	cfg := &config.LoggerConfig{
		Level:      config.LogLevelInfo,
		TimeFormat: "", // Disable timestamp for easier testing
	}
	logger := NewLoggerWithWriter(cfg, &buf)

	logger.Info("Processing %d files", 100)

	output := buf.String()
	require.Contains(t, output, "Processing 100 files")
}

func TestLogger_With(t *testing.T) {
	var buf bytes.Buffer
	cfg := &config.LoggerConfig{
		Level:      config.LogLevelInfo,
		TimeFormat: "", // Disable timestamp for easier testing
	}
	logger := NewLoggerWithWriter(cfg, &buf)

	// Create a logger with context
	contextLogger := logger.With("component", "processor")
	contextLogger.Info("test message")

	output := buf.String()
	require.Contains(t, output, "component=processor")
	require.Contains(t, output, "test message")
}

func TestLogger_WithFields(t *testing.T) {
	var buf bytes.Buffer
	cfg := &config.LoggerConfig{
		Level:      config.LogLevelInfo,
		TimeFormat: "", // Disable timestamp for easier testing
	}
	logger := NewLoggerWithWriter(cfg, &buf)

	// Create a logger with multiple context fields
	fields := map[string]interface{}{
		"component": "cache",
		"operation": "sync",
		"count":     42,
	}
	contextLogger := logger.WithFields(fields)
	contextLogger.Info("operation completed")

	output := buf.String()
	require.Contains(t, output, "component=cache")
	require.Contains(t, output, "operation=sync")
	require.Contains(t, output, "count=42")
	require.Contains(t, output, "operation completed")
}

func TestLogger_ChainedWith(t *testing.T) {
	var buf bytes.Buffer
	cfg := &config.LoggerConfig{
		Level:      config.LogLevelInfo,
		TimeFormat: "", // Disable timestamp for easier testing
	}
	logger := NewLoggerWithWriter(cfg, &buf)

	// Chain multiple With calls
	contextLogger := logger.With("component", "processor").With("worker", "1")
	contextLogger.Info("processing started")

	output := buf.String()
	require.Contains(t, output, "component=processor")
	require.Contains(t, output, "worker=1")
	require.Contains(t, output, "processing started")
}

func TestLogger_Timestamp(t *testing.T) {
	var buf bytes.Buffer
	cfg := &config.LoggerConfig{
		Level:      config.LogLevelInfo,
		TimeFormat: "2006-01-02 15:04:05",
	}
	logger := NewLoggerWithWriter(cfg, &buf)

	logger.Info("test message")

	output := buf.String()
	// Should contain a timestamp in the format YYYY-MM-DD HH:MM:SS
	require.Regexp(t, `\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}`, output)
	require.Contains(t, output, "test message")
}

func TestLogger_LevelInOutput(t *testing.T) {
	var buf bytes.Buffer
	cfg := &config.LoggerConfig{
		Level:      config.LogLevelVerbose,
		TimeFormat: "",
	}
	logger := NewLoggerWithWriter(cfg, &buf)

	logger.Error("error msg")
	logger.Info("info msg")
	logger.Debug("debug msg")
	logger.Verbose("verbose msg")

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")

	require.Len(t, lines, 4)
	require.Contains(t, lines[0], "[error]")
	require.Contains(t, lines[1], "[info]")
	require.Contains(t, lines[2], "[debug]")
	require.Contains(t, lines[3], "[verbose]")
}

func TestNoOpLogger(t *testing.T) {
	logger := NewNoOpLogger()
	require.NotNil(t, logger)

	// Should not panic
	logger.Error("error")
	logger.Warn("warn")
	logger.Info("info")
	logger.Debug("debug")
	logger.Verbose("verbose")

	contextLogger := logger.With("key", "value")
	require.NotNil(t, contextLogger)
	contextLogger.Info("test")

	fieldsLogger := logger.WithFields(map[string]interface{}{"key": "value"})
	require.NotNil(t, fieldsLogger)
	fieldsLogger.Info("test")
}

func TestLoggerConfig_Defaults(t *testing.T) {
	cfg := &config.LoggerConfig{}
	cfg.ApplyDefaults()

	require.Equal(t, config.LogLevelInfo, cfg.Level)
	require.Equal(t, "2006-01-02 15:04:05", cfg.TimeFormat)
}

func TestLoggerConfig_Validation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     config.LoggerConfig
		wantErr bool
	}{
		{
			name:    "valid silent level",
			cfg:     config.LoggerConfig{Level: config.LogLevelSilent},
			wantErr: false,
		},
		{
			name:    "valid error level",
			cfg:     config.LoggerConfig{Level: config.LogLevelError},
			wantErr: false,
		},
		{
			name:    "valid info level",
			cfg:     config.LoggerConfig{Level: config.LogLevelInfo},
			wantErr: false,
		},
		{
			name:    "valid debug level",
			cfg:     config.LoggerConfig{Level: config.LogLevelDebug},
			wantErr: false,
		},
		{
			name:    "valid verbose level",
			cfg:     config.LoggerConfig{Level: config.LogLevelVerbose},
			wantErr: false,
		},
		{
			name:    "empty level (will use default)",
			cfg:     config.LoggerConfig{Level: ""},
			wantErr: false,
		},
		{
			name:    "invalid level",
			cfg:     config.LoggerConfig{Level: "invalid"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

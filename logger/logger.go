package logger

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/olegkotsar/yomins-sync/config"
)

// Logger defines the logging interface
type Logger interface {
	// Error logs an error message
	Error(msg string, args ...interface{})
	// Warn logs a warning message
	Warn(msg string, args ...interface{})
	// Info logs an informational message
	Info(msg string, args ...interface{})
	// Debug logs a debug message
	Debug(msg string, args ...interface{})
	// Verbose logs a verbose/trace message
	Verbose(msg string, args ...interface{})

	// With returns a new logger with additional context fields
	With(key string, value interface{}) Logger
	// WithFields returns a new logger with multiple context fields
	WithFields(fields map[string]interface{}) Logger
}

// DefaultLogger is the default logger implementation
type DefaultLogger struct {
	mu         sync.Mutex
	cfg        *config.LoggerConfig
	writer     io.Writer
	fields     map[string]interface{}
	addSource  bool
	timeFormat string
}

// NewLogger creates a new logger with the given configuration
func NewLogger(cfg *config.LoggerConfig) Logger {
	if cfg == nil {
		cfg = &config.LoggerConfig{}
	}
	cfg.ApplyDefaults()

	return &DefaultLogger{
		cfg:        cfg,
		writer:     os.Stdout,
		fields:     make(map[string]interface{}),
		addSource:  cfg.AddSource,
		timeFormat: cfg.TimeFormat,
	}
}

// NewLoggerWithWriter creates a logger with a custom writer (useful for testing)
func NewLoggerWithWriter(cfg *config.LoggerConfig, writer io.Writer) Logger {
	if cfg == nil {
		cfg = &config.LoggerConfig{}
	}
	cfg.ApplyDefaults()

	return &DefaultLogger{
		cfg:        cfg,
		writer:     writer,
		fields:     make(map[string]interface{}),
		addSource:  cfg.AddSource,
		timeFormat: cfg.TimeFormat,
	}
}

// shouldLog checks if a message at the given level should be logged
func (l *DefaultLogger) shouldLog(level config.LogLevel) bool {
	if l.cfg.Level == config.LogLevelSilent {
		return false
	}

	// Define level hierarchy
	levels := map[config.LogLevel]int{
		config.LogLevelSilent:  0,
		config.LogLevelError:   1,
		config.LogLevelInfo:    2,
		config.LogLevelDebug:   3,
		config.LogLevelVerbose: 4,
	}

	currentLevel := levels[l.cfg.Level]
	messageLevel := levels[level]

	return messageLevel <= currentLevel
}

// log is the internal logging method
func (l *DefaultLogger) log(level config.LogLevel, msg string, args ...interface{}) {
	if !l.shouldLog(level) {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	// Build the log message
	var output string

	// Add timestamp if configured
	if l.timeFormat != "" {
		output += time.Now().Format(l.timeFormat) + " "
	}

	// Add level
	output += fmt.Sprintf("[%s] ", level)

	// Add source information if configured
	if l.addSource {
		_, file, line, ok := runtime.Caller(2)
		if ok {
			output += fmt.Sprintf("%s:%d ", file, line)
		}
	}

	// Add context fields if any
	if len(l.fields) > 0 {
		output += "["
		first := true
		for k, v := range l.fields {
			if !first {
				output += ", "
			}
			output += fmt.Sprintf("%s=%v", k, v)
			first = false
		}
		output += "] "
	}

	// Add the message
	if len(args) > 0 {
		output += fmt.Sprintf(msg, args...)
	} else {
		output += msg
	}

	output += "\n"

	// Write to output
	fmt.Fprint(l.writer, output)
}

// Error logs an error message
func (l *DefaultLogger) Error(msg string, args ...interface{}) {
	l.log(config.LogLevelError, msg, args...)
}

// Warn logs a warning message
func (l *DefaultLogger) Warn(msg string, args ...interface{}) {
	l.log(config.LogLevelInfo, msg, args...)
}

// Info logs an informational message
func (l *DefaultLogger) Info(msg string, args ...interface{}) {
	l.log(config.LogLevelInfo, msg, args...)
}

// Debug logs a debug message
func (l *DefaultLogger) Debug(msg string, args ...interface{}) {
	l.log(config.LogLevelDebug, msg, args...)
}

// Verbose logs a verbose/trace message
func (l *DefaultLogger) Verbose(msg string, args ...interface{}) {
	l.log(config.LogLevelVerbose, msg, args...)
}

// With returns a new logger with an additional context field
func (l *DefaultLogger) With(key string, value interface{}) Logger {
	newFields := make(map[string]interface{}, len(l.fields)+1)
	for k, v := range l.fields {
		newFields[k] = v
	}
	newFields[key] = value

	return &DefaultLogger{
		cfg:        l.cfg,
		writer:     l.writer,
		fields:     newFields,
		addSource:  l.addSource,
		timeFormat: l.timeFormat,
	}
}

// WithFields returns a new logger with multiple context fields
func (l *DefaultLogger) WithFields(fields map[string]interface{}) Logger {
	newFields := make(map[string]interface{}, len(l.fields)+len(fields))
	for k, v := range l.fields {
		newFields[k] = v
	}
	for k, v := range fields {
		newFields[k] = v
	}

	return &DefaultLogger{
		cfg:        l.cfg,
		writer:     l.writer,
		fields:     newFields,
		addSource:  l.addSource,
		timeFormat: l.timeFormat,
	}
}

// NoOpLogger is a logger that does nothing (useful for testing or when logging is disabled)
type NoOpLogger struct{}

// NewNoOpLogger creates a no-op logger
func NewNoOpLogger() Logger {
	return &NoOpLogger{}
}

func (n *NoOpLogger) Error(msg string, args ...interface{})                     {}
func (n *NoOpLogger) Warn(msg string, args ...interface{})                      {}
func (n *NoOpLogger) Info(msg string, args ...interface{})                      {}
func (n *NoOpLogger) Debug(msg string, args ...interface{})                     {}
func (n *NoOpLogger) Verbose(msg string, args ...interface{})                   {}
func (n *NoOpLogger) With(key string, value interface{}) Logger                 { return n }
func (n *NoOpLogger) WithFields(fields map[string]interface{}) Logger           { return n }

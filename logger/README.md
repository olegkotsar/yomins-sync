# Logger

A simple, structured logging library with configurable verbosity levels and context support.

## Features

- **Multiple log levels**: Silent, Error, Info, Debug, Verbose
- **Structured logging**: Add context fields with `With()` and `WithFields()`
- **Thread-safe**: Safe for concurrent use
- **Dependency injection**: Easy to inject into components
- **Configurable**: Control output format, timestamps, and source information
- **Testing support**: `NewLoggerWithWriter()` for capturing output in tests

## Quick Start

```go
import (
    "github.com/olegkotsar/yomins-sync/config"
    "github.com/olegkotsar/yomins-sync/logger"
)

// Create logger with configuration
cfg := &config.LoggerConfig{
    Level:      config.LogLevelInfo,
    AddSource:  false,
    TimeFormat: "2006-01-02 15:04:05",
}
log := logger.NewLogger(cfg)

// Basic logging
log.Info("Application started")
log.Error("Failed to connect: %v", err)
log.Debug("Configuration loaded: %+v", config)
```

## Log Levels

Levels in order of verbosity (each level includes all levels above it):

1. **Silent** - No output
2. **Error** - Only errors
3. **Info** - Info, warnings, and errors (default)
4. **Debug** - Debug information + all above
5. **Verbose** - Trace-level details + all above

## Adding Context

Use `With()` to add a single field or `WithFields()` for multiple:

```go
// Single field
workerLog := log.With("worker_id", 5)
workerLog.Info("Processing batch")
// Output: [info] [worker_id=5] Processing batch

// Multiple fields
cacheLog := log.WithFields(map[string]interface{}{
    "component": "cache",
    "operation": "sync",
    "count":     1000,
})
cacheLog.Info("Operation completed")
// Output: [info] [component=cache, operation=sync, count=1000] Operation completed
```

## Dependency Injection

The logger is designed to be injected into structs:

```go
type Service struct {
    logger logger.Logger
}

func NewService(log logger.Logger) *Service {
    if log == nil {
        log = logger.NewNoOpLogger()
    }
    return &Service{
        logger: log.With("component", "service"),
    }
}

func (s *Service) Process() {
    s.logger.Info("Processing started")
    s.logger.Debug("Loading configuration")
}
```

## Testing

Use `NewLoggerWithWriter()` to capture output in tests:

```go
func TestMyFunction(t *testing.T) {
    var buf bytes.Buffer
    cfg := &config.LoggerConfig{
        Level:      config.LogLevelDebug,
        TimeFormat: "", // Disable timestamp for easier testing
    }
    log := logger.NewLoggerWithWriter(cfg, &buf)

    // Run code that logs
    myFunction(log)

    // Assert on output
    output := buf.String()
    require.Contains(t, output, "expected log message")
}
```

## Configuration Options

```go
type LoggerConfig struct {
    Level      LogLevel // Log verbosity level
    AddSource  bool     // Include file:line in output
    TimeFormat string   // Time format (empty = no timestamp)
}
```

**Default values**:
- Level: `LogLevelInfo`
- AddSource: `false`
- TimeFormat: `"2006-01-02 15:04:05"`

## No-Op Logger

For testing or when logging is not needed:

```go
log := logger.NewNoOpLogger()
// All methods are no-ops
log.Info("This does nothing")
```

## Output Format

```
[timestamp] [level] [context_fields] message
```

Examples:
```
2025-01-15 10:30:45 [info] Application started
2025-01-15 10:30:46 [debug] [worker_id=5] Processing batch
2025-01-15 10:30:47 [error] Failed to connect: connection refused
```

## Best Practices

1. **Create component-specific loggers**: Use `With("component", "name")` to identify log sources
2. **Use appropriate levels**:
   - `Error` for failures that require attention
   - `Warn` for concerning but non-critical issues
   - `Info` for important state changes
   - `Debug` for detailed diagnostic information
   - `Verbose` for trace-level debugging
3. **Add context, not noise**: Include relevant IDs, counts, or states
4. **Format messages clearly**: Use descriptive messages with context via format args
5. **Inject the logger**: Don't use global loggers; inject via constructors

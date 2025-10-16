package logger_test

import (
	"github.com/olegkotsar/yomins-sync/config"
	"github.com/olegkotsar/yomins-sync/logger"
)

// Example demonstrates basic logger usage
func Example_basic() {
	cfg := &config.LoggerConfig{
		Level:      config.LogLevelInfo,
		TimeFormat: "", // Disable timestamp for example
	}

	log := logger.NewLogger(cfg)

	log.Info("Application started")
	log.Debug("This won't be shown (level is Info)")
	log.Error("An error occurred: %s", "connection failed")
	log.Warn("Warning: resource usage high")

	// Output will show Info, Warn, and Error messages
}

// Example_withContext demonstrates using logger with context fields
func Example_withContext() {
	cfg := &config.LoggerConfig{
		Level:      config.LogLevelInfo,
		TimeFormat: "",
	}

	log := logger.NewLogger(cfg)

	// Create a logger with context for a specific component
	processorLog := log.With("component", "processor")
	processorLog.Info("Processing started")

	// Add more context
	workerLog := processorLog.With("worker_id", 5)
	workerLog.Info("Worker processing batch")

	// Use WithFields for multiple context values at once
	cacheLog := log.WithFields(map[string]interface{}{
		"component": "cache",
		"operation": "sync",
		"count":     1000,
	})
	cacheLog.Info("Cache operation completed")
}

// Example_logLevels demonstrates different log levels
func Example_logLevels() {
	// Silent - no output
	silentLog := logger.NewLogger(&config.LoggerConfig{Level: config.LogLevelSilent})
	silentLog.Info("This won't be logged")

	// Error - only errors
	errorLog := logger.NewLogger(&config.LoggerConfig{Level: config.LogLevelError, TimeFormat: ""})
	errorLog.Error("Error logged")
	errorLog.Info("This won't be logged")

	// Info - errors, warnings, and info
	infoLog := logger.NewLogger(&config.LoggerConfig{Level: config.LogLevelInfo, TimeFormat: ""})
	infoLog.Error("Error logged")
	infoLog.Info("Info logged")
	infoLog.Debug("This won't be logged")

	// Debug - errors, warnings, info, and debug
	debugLog := logger.NewLogger(&config.LoggerConfig{Level: config.LogLevelDebug, TimeFormat: ""})
	debugLog.Debug("Debug logged")
	debugLog.Verbose("This won't be logged")

	// Verbose - everything
	verboseLog := logger.NewLogger(&config.LoggerConfig{Level: config.LogLevelVerbose, TimeFormat: ""})
	verboseLog.Verbose("Verbose logged")
}

// Example_injection shows how to inject logger into a struct
func Example_injection() {
	// Create logger configuration
	cfg := &config.LoggerConfig{
		Level:      config.LogLevelDebug,
		TimeFormat: "15:04:05",
	}

	// Create logger
	log := logger.NewLogger(cfg)

	// Example service that uses the logger
	type Service struct {
		logger logger.Logger
	}

	svc := &Service{
		logger: log.With("service", "example"),
	}

	svc.logger.Info("Service initialized")
	svc.logger.Debug("Configuration loaded")

}

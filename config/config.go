package config

import (
	"fmt"
	"os"
	"strconv"
)

// AppConfig represents the complete application configuration
type AppConfig struct {
	Source      SourceConfig      `json:"source" yaml:"source" toml:"source"`
	Cache       CacheConfig       `json:"cache" yaml:"cache" toml:"cache"`
	Destination DestinationConfig `json:"destination" yaml:"destination" toml:"destination"`
	Logger      LoggerConfig      `json:"logger" yaml:"logger" toml:"logger"`
	DryRun      bool              `json:"dry_run" yaml:"dry_run" toml:"dry_run"` // If true, no files will be uploaded to destination
}

// Validate validates the entire configuration
func (ac *AppConfig) Validate() error {
	if err := ac.Source.Validate(); err != nil {
		return fmt.Errorf("source config error: %w", err)
	}
	if err := ac.Cache.Validate(); err != nil {
		return fmt.Errorf("cache config error: %w", err)
	}
	if err := ac.Destination.Validate(); err != nil {
		return fmt.Errorf("destination config error: %w", err)
	}
	if err := ac.Logger.Validate(); err != nil {
		return fmt.Errorf("logger config error: %w", err)
	}
	return nil
}

// ApplyDefaults applies default values to all components
func (ac *AppConfig) ApplyDefaults() {
	ac.Source.Common.ApplyDefaults()
	ac.Destination.Common.ApplyDefaults()
	ac.Logger.ApplyDefaults()

	// Apply defaults for specific configs
	if ac.Source.S3 != nil {
		// S3 has no defaults currently
	}
	if ac.Cache.Bbolt != nil {
		ac.Cache.Bbolt.ApplyDefaults()
	}
	if ac.Destination.FTP != nil {
		ac.Destination.FTP.ApplyDefaults()
	}
}

// LoadFromEnv loads configuration from environment variables
// This is a helper to populate config from env vars
func LoadFromEnv() (*AppConfig, error) {
	cfg := &AppConfig{}

	// General configuration
	cfg.DryRun = getEnvBool("DRY_RUN", false)

	// Logger configuration
	cfg.Logger.Level = LogLevel(getEnv("LOG_LEVEL", string(LogLevelInfo)))

	// Cache configuration
	cfg.Cache.CacheType = CacheType(getEnv("CACHE_TYPE", string(CacheTypeBbolt)))
	cfg.Cache.Bbolt = &BboltConfig{
		Path:   getEnv("CACHE_BBOLT_PATH", "./cache.db"),
		Bucket: getEnv("CACHE_BBOLT_BUCKET", "files"),
		Mode:   0600,
		NoSync: getEnvBool("CACHE_BBOLT_NO_SYNC", false),
	}

	// Source configuration
	cfg.Source.SourceType = SourceType(getEnv("SOURCE_TYPE", string(SourceTypeS3)))
	cfg.Source.Common.WorkerCount = getEnvInt("SOURCE_WORKER_COUNT", 5)
	cfg.Source.Common.MaxQueueSize = getEnvInt("SOURCE_MAX_QUEUE_SIZE", 1000)
	cfg.Source.Common.TimeoutSeconds = getEnvInt("SOURCE_TIMEOUT_SECONDS", 30)
	cfg.Source.Common.MaxRetries = getEnvInt("SOURCE_MAX_RETRIES", 3)
	cfg.Source.Common.MaxRPS = getEnvInt("SOURCE_MAX_RPS", 0)
	cfg.Source.Common.ListingStrategy = ListingStrategy(getEnv("SOURCE_LISTING_STRATEGY", string(ListingStrategyRecursive)))
	cfg.Source.Common.ListingLevel = getEnvInt("SOURCE_LISTING_LEVEL", 0)

	cfg.Source.S3 = &S3Config{
		Region:          getEnv("S3_REGION", ""),
		Bucket:          getEnv("S3_BUCKET", ""),
		AccessKeyID:     getEnv("S3_ACCESS_KEY_ID", ""),
		SecretAccessKey: getEnv("S3_SECRET_ACCESS_KEY", ""),
		Endpoint:        getEnv("S3_ENDPOINT", ""),
	}

	// Destination configuration
	cfg.Destination.DestinationType = DestinationType(getEnv("DESTINATION_TYPE", string(DestinationTypeFTP)))
	cfg.Destination.Common.WorkerCount = getEnvInt("DESTINATION_WORKER_COUNT", 5)
	cfg.Destination.Common.TimeoutSeconds = getEnvInt("DESTINATION_TIMEOUT_SECONDS", 30)
	cfg.Destination.Common.MaxRetries = getEnvInt("DESTINATION_MAX_RETRIES", 3)

	cfg.Destination.FTP = &FTPConfig{
		Host:     getEnv("FTP_HOST", ""),
		Port:     getEnvInt("FTP_PORT", 21),
		Username: getEnv("FTP_USERNAME", ""),
		Password: getEnv("FTP_PASSWORD", ""),
		BasePath: getEnv("FTP_BASE_PATH", "/"),
		UseTLS:   getEnvBool("FTP_USE_TLS", false),
	}

	// Apply defaults
	cfg.ApplyDefaults()

	return cfg, nil
}

// Helper functions for environment variables
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolVal, err := strconv.ParseBool(value); err == nil {
			return boolVal
		}
	}
	return defaultValue
}

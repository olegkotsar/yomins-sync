// The source configuration is designed to allow adding other sources in the future. To do this, you need to add a new SourceType, update SourceConfig, and define the validation for the new source.
package config

import "fmt"

// SourceType represents the type of storage backend
type SourceType string

const (
	SourceTypeS3 SourceType = "s3"
)

// ListingStrategy defines how to list objects from the source
type ListingStrategy string

const (
	ListingStrategyRecursive ListingStrategy = "recursive" // Default: recursive traversal with delimiter
	ListingStrategyByLevel   ListingStrategy = "by_level"  // Collect prefixes at a specific level, then list flat
)

// sourceconfig holds the configuration for a storage source
type SourceConfig struct {
	SourceType SourceType `json:"type" yaml:"type" toml:"type"`

	// Common options for all sources
	Common CommonSourceConfig `json:"common,omitempty" yaml:"common,omitempty" toml:"common,omitempty"`

	// type-specific configurations
	S3 *S3Config `json:"s3,omitempty" yaml:"s3,omitempty" toml:"s3,omitempty"`
}

// CommonSourceConfig contains general settings applicable to all sources
type CommonSourceConfig struct {
	WorkerCount     int             `json:"worker_count,omitempty" yaml:"worker_count,omitempty" toml:"worker_count,omitempty"`             // optional: number of concurrent workers
	MaxQueueSize    int             `json:"max_queue_size,omitempty" yaml:"max_queue_size,omitempty" toml:"max_queue_size,omitempty"`       // optional: channel buffer size
	TimeoutSeconds  int             `json:"timeout_seconds,omitempty" yaml:"timeout_seconds,omitempty" toml:"timeout_seconds,omitempty"`    // optional: request timeout in seconds
	MaxRetries      int             `json:"max_retries,omitempty" yaml:"max_retries,omitempty" toml:"max_retries,omitempty"`                // optional: maximum number of retries for API calls
	MaxRPS          int             `json:"max_rps,omitempty" yaml:"max_rps,omitempty" toml:"max_rps,omitempty"`                            // optional: maximum requesr per second to the backend (S3 API)
	ListingStrategy ListingStrategy `json:"listing_strategy,omitempty" yaml:"listing_strategy,omitempty" toml:"listing_strategy,omitempty"` // optional: listing strategy (recursive or by_level)
	ListingLevel    int             `json:"listing_level,omitempty" yaml:"listing_level,omitempty" toml:"listing_level,omitempty"`          // optional: depth level for by_level strategy (required if by_level is used)
}

// S3Config holds S3-specific configuration
type S3Config struct {
	Region          string `json:"region" yaml:"region" toml:"region"`
	Bucket          string `json:"bucket" yaml:"bucket" toml:"bucket"`
	AccessKeyID     string `json:"access_key_id,omitempty" yaml:"access_key_id,omitempty" toml:"access_key_id,omitempty"`
	SecretAccessKey string `json:"secret_access_key,omitempty" yaml:"secret_access_key,omitempty" toml:"secret_access_key,omitempty"`
	Endpoint        string `json:"endpoint,omitempty" yaml:"endpoint,omitempty" toml:"endpoint,omitempty"` // For S3-compatible services
}

// Validate ensures the configuration is valid for the specified source type
func (sc *SourceConfig) Validate() error {
	if err := sc.Common.Validate(); err != nil {
		return err
	}

	switch sc.SourceType {
	case SourceTypeS3:
		if sc.S3 == nil {
			return fmt.Errorf("s3 configuration is required when type is 's3'")
		}
		return sc.S3.Validate()
	default:
		return fmt.Errorf("unsupported source type: %s", sc.SourceType)
	}
}

// GetActiveConfig returns the active configuration based on the source type
func (sc *SourceConfig) GetActiveConfig() interface{} {
	switch sc.SourceType {
	case SourceTypeS3:
		return sc.S3
	default:
		return nil
	}
}

// Validate validates S3 configuration
func (s3c *S3Config) Validate() error {
	if s3c.Bucket == "" {
		return fmt.Errorf("s3 bucket is required")
	}
	if s3c.AccessKeyID == "" {
		return fmt.Errorf("s3 access key is required")
	}
	if s3c.SecretAccessKey == "" {
		return fmt.Errorf("s3 secret key is required")
	}
	if s3c.Endpoint == "" {
		return fmt.Errorf("s3 endpoint is required")
	}
	return nil
}

// ApplyDefaults sets default values if they are not provided
func (c *CommonSourceConfig) ApplyDefaults() {
	if c.WorkerCount <= 0 {
		c.WorkerCount = 5
	}
	if c.MaxQueueSize <= 0 {
		c.MaxQueueSize = 1000
	}
	if c.TimeoutSeconds <= 0 {
		c.TimeoutSeconds = 30
	}
	if c.MaxRetries <= 0 {
		c.MaxRetries = 3
	}
	// MaxRPS leave 0 (means no limit)

	// Default to recursive listing strategy
	if c.ListingStrategy == "" {
		c.ListingStrategy = ListingStrategyRecursive
	}
}

func (c *CommonSourceConfig) Validate() error {
	if c.WorkerCount < 0 {
		return fmt.Errorf("worker_count cannot be negative")
	}
	if c.MaxRetries < 0 {
		return fmt.Errorf("max_retries cannot be negative")
	}

	// Validate listing strategy
	switch c.ListingStrategy {
	case ListingStrategyRecursive:
		// No additional validation needed
	case ListingStrategyByLevel:
		if c.ListingLevel <= 0 {
			return fmt.Errorf("listing_level must be greater than 0 when using by_level strategy")
		}
	case "":
		// Empty is OK, will be set to default in ApplyDefaults
	default:
		return fmt.Errorf("unsupported listing strategy: %s (must be 'recursive' or 'by_level')", c.ListingStrategy)
	}

	return nil
}

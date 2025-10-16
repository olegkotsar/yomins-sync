package config

import (
	"fmt"
	"os"
)

// CacheType represents the type of cache backend
type CacheType string

const (
	CacheTypeBbolt CacheType = "bbolt"
)

// CacheConfig holds the configuration for cache
type CacheConfig struct {
	CacheType CacheType `json:"type" yaml:"type" toml:"type"`

	// Type-specific configs
	Bbolt *BboltConfig `json:"bbolt,omitempty" yaml:"bbolt,omitempty" toml:"bbolt,omitempty"`
}

// BboltConfig holds bbolt-specific configuration
type BboltConfig struct {
	Path   string      `json:"path" yaml:"path" toml:"path"`                                        // Path to bbolt DB file
	Bucket string      `json:"bucket" yaml:"bucket" toml:"bucket"`                                  // Name of the bucket
	Mode   os.FileMode `json:"mode,omitempty" yaml:"mode,omitempty" toml:"mode,omitempty"`          // File open mode: "0600", "0644"
	NoSync bool        `json:"no_sync,omitempty" yaml:"no_sync,omitempty" toml:"no_sync,omitempty"` // Disable fsync for better performance
}

// Validate validates the cache configuration
func (cc *CacheConfig) Validate() error {
	switch cc.CacheType {
	case CacheTypeBbolt:
		if cc.Bbolt == nil {
			return fmt.Errorf("bbolt configuration is required when type is 'bbolt'")
		}
		return cc.Bbolt.Validate()
	default:
		return fmt.Errorf("unsupported cache type: %s", cc.CacheType)
	}
}

func (cc *CacheConfig) GetActiveConfig() interface{} {
	switch cc.CacheType {
	case CacheTypeBbolt:
		return cc.Bbolt
	default:
		return nil
	}
}

func (bc *BboltConfig) Validate() error {
	if bc.Path == "" {
		return fmt.Errorf("bbolt path is required")
	}
	if bc.Bucket == "" {
		return fmt.Errorf("bbolt bucket is required")
	}
	return nil
}

// ApplyDefaults sets default values if not provided for bbolt
func (bc *BboltConfig) ApplyDefaults() {
	if bc.Path == "" {
		bc.Path = "./cache.db" // Default path in the current directory
	}
	if bc.Bucket == "" {
		bc.Bucket = "files" // Default bucket name
	}
	if bc.Mode == 0 {
		bc.Mode = 0600 // Default file permission
	}
	// NoSync remains false by default for data safety
}

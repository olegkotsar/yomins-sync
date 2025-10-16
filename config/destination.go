package config

import "fmt"

// DestinationType represents the type of destination backend
type DestinationType string

const (
	DestinationTypeFTP DestinationType = "ftp"
)

// DestinationConfig holds the configuration for a destination
type DestinationConfig struct {
	DestinationType DestinationType `json:"type" yaml:"type" toml:"type"`

	// Common options for all destinations
	Common CommonDestinationConfig `json:"common,omitempty" yaml:"common,omitempty" toml:"common,omitempty"`

	// Type-specific configurations
	FTP *FTPConfig `json:"ftp,omitempty" yaml:"ftp,omitempty" toml:"ftp,omitempty"`
}

// CommonDestinationConfig contains general settings applicable to all destinations
type CommonDestinationConfig struct {
	WorkerCount    int `json:"worker_count,omitempty" yaml:"worker_count,omitempty" toml:"worker_count,omitempty"`          // optional: number of concurrent upload workers
	TimeoutSeconds int `json:"timeout_seconds,omitempty" yaml:"timeout_seconds,omitempty" toml:"timeout_seconds,omitempty"` // optional: operation timeout in seconds
	MaxRetries     int `json:"max_retries,omitempty" yaml:"max_retries,omitempty" toml:"max_retries,omitempty"`             // optional: maximum number of retries for operations
}

// FTPConfig holds FTP-specific configuration
type FTPConfig struct {
	Host     string `json:"host" yaml:"host" toml:"host"`                                           // FTP server host
	Port     int    `json:"port" yaml:"port" toml:"port"`                                           // FTP server port (default: 21)
	Username string `json:"username" yaml:"username" toml:"username"`                               // FTP username
	Password string `json:"password,omitempty" yaml:"password,omitempty" toml:"password,omitempty"` // FTP password
	BasePath string `json:"base_path,omitempty" yaml:"base_path,omitempty" toml:"base_path"`        // Base path on FTP server (optional)
	UseTLS   bool   `json:"use_tls,omitempty" yaml:"use_tls,omitempty" toml:"use_tls,omitempty"`    // Use FTPS (FTP over TLS)
}

// Validate ensures the configuration is valid for the specified destination type
func (dc *DestinationConfig) Validate() error {
	if err := dc.Common.Validate(); err != nil {
		return err
	}

	switch dc.DestinationType {
	case DestinationTypeFTP:
		if dc.FTP == nil {
			return fmt.Errorf("ftp configuration is required when type is 'ftp'")
		}
		return dc.FTP.Validate()
	default:
		return fmt.Errorf("unsupported destination type: %s", dc.DestinationType)
	}
}

// GetActiveConfig returns the active configuration based on the destination type
func (dc *DestinationConfig) GetActiveConfig() interface{} {
	switch dc.DestinationType {
	case DestinationTypeFTP:
		return dc.FTP
	default:
		return nil
	}
}

// Validate validates FTP configuration
func (fc *FTPConfig) Validate() error {
	if fc.Host == "" {
		return fmt.Errorf("ftp host is required")
	}
	if fc.Port <= 0 || fc.Port > 65535 {
		return fmt.Errorf("ftp port must be between 1 and 65535")
	}
	if fc.Username == "" {
		return fmt.Errorf("ftp username is required")
	}
	// Password can be empty for anonymous FTP
	return nil
}

// ApplyDefaults sets default values for destination configuration
func (c *CommonDestinationConfig) ApplyDefaults() {
	if c.WorkerCount <= 0 {
		c.WorkerCount = 5
	}
	if c.TimeoutSeconds <= 0 {
		c.TimeoutSeconds = 30
	}
	if c.MaxRetries <= 0 {
		c.MaxRetries = 3
	}
}

// Validate validates common destination configuration
func (c *CommonDestinationConfig) Validate() error {
	if c.WorkerCount < 0 {
		return fmt.Errorf("worker_count cannot be negative")
	}
	if c.MaxRetries < 0 {
		return fmt.Errorf("max_retries cannot be negative")
	}
	if c.TimeoutSeconds < 0 {
		return fmt.Errorf("timeout_seconds cannot be negative")
	}
	return nil
}

// ApplyDefaults sets default values for FTP configuration
func (fc *FTPConfig) ApplyDefaults() {
	if fc.Port == 0 {
		fc.Port = 21 // Default FTP port
	}
	if fc.BasePath == "" {
		fc.BasePath = "/" // Default to root
	}
}

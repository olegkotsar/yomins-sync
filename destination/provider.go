package destination

import (
	"context"
	"fmt"
	"io"

	"github.com/olegkotsar/yomins-sync/config"
)

type DestinationProvider interface {
	Upload(ctx context.Context, path string, content io.Reader) error
	Delete(ctx context.Context, path string) error
	FileExists(ctx context.Context, path string) (bool, error)
	GetWorkerCount() int
	Close() error
}

// CreateDestination creates a destination provider based on configuration
func CreateDestination(cfg *config.DestinationConfig) (DestinationProvider, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid destination configuration: %w", err)
	}

	switch cfg.DestinationType {
	case config.DestinationTypeFTP:
		return NewFTPDestination(cfg.FTP, &cfg.Common)
	default:
		return nil, fmt.Errorf("unsupported destination type: %s", cfg.DestinationType)
	}
}

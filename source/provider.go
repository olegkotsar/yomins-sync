package source

import (
	"context"
	"fmt"
	"io"

	"github.com/olegkotsar/yomins-sync/config"
	"github.com/olegkotsar/yomins-sync/model"
)

type SourceProvider interface {
	ListObjectsFlat(ctx context.Context, prefix string) ([]model.RemoteFile, error)
	ListObjectsRecursiveStream(ctx context.Context) (<-chan model.RemoteFile, <-chan error)
	ListObjectsByLevelStream(ctx context.Context, level int) (<-chan model.RemoteFile, <-chan error)
	// ListObjectsStream selects the appropriate listing strategy based on configuration
	ListObjectsStream(ctx context.Context) (<-chan model.RemoteFile, <-chan error)
	// GetObject downloads a file from the source
	GetObject(ctx context.Context, key string) (io.ReadCloser, error)
	// GetCurrentRPS returns the current requests per second rate for monitoring
	GetCurrentRPS() int64
}

func CreateSource(cfg *config.SourceConfig, common *config.CommonSourceConfig) (SourceProvider, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid source configuration: %w", err)
	}

	switch cfg.SourceType {
	case config.SourceTypeS3:
		return NewS3Source(cfg.S3, common)
	default:
		return nil, fmt.Errorf("unsupported source type: %s", cfg.SourceType)
	}
}

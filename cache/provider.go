package cache

import (
	"context"
	"errors"
	"fmt"

	"github.com/olegkotsar/yomins-sync/config"
	"github.com/olegkotsar/yomins-sync/model"
)

type CacheProvider interface {
	Set(key string, meta model.FileMeta) error
	Get(key string) (*model.FileMeta, error)
	BatchSet(entries map[string]model.FileMeta) error
	GetByPrefix(prefix string) (map[string]model.FileMeta, error)
	DumpAll() (map[string]model.FileMeta, error)
	Delete(key string) error
	Close() error
	Count() (int64, error)
	// IterateBatches streams cache entries in batches of the specified size
	IterateBatches(ctx context.Context, batchSize int) (<-chan map[string]model.FileMeta, <-chan error)
}

var (
	ErrKeyNotFound    error = errors.New("key not found")
	ErrBucketNotFound error = errors.New("bucket not found")
)

func CreateCache(cfg *config.CacheConfig) (CacheProvider, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid source configuration: %w", err)
	}

	switch cfg.CacheType {
	case config.CacheTypeBbolt:
		return NewBboltCache(cfg.Bbolt)
	default:
		return nil, fmt.Errorf("unsupported source type: %s", cfg.CacheType)
	}
}

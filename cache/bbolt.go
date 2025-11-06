package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/olegkotsar/yomins-sync/config"
	"github.com/olegkotsar/yomins-sync/model"
	"go.etcd.io/bbolt"
)

const defaultBucket = "files"

type BboltCache struct {
	db     *bbolt.DB
	bucket string
}

// NewBboltCache creates a new BboltCache based on configuration
func NewBboltCache(cfg *config.BboltConfig) (*BboltCache, error) {
	// Apply defaults to ensure required values are set
	cfg.ApplyDefaults()

	// Validate config
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid bbolt config: %w", err)
	}

	// Open bbolt database
	db, err := bbolt.Open(cfg.Path, cfg.Mode, nil)
	if err != nil {
		return nil, err
	}

	// Create bucket if not exists
	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(cfg.Bucket))
		return err
	})
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to create bucket: %w", err)
	}

	return &BboltCache{
		db:     db,
		bucket: cfg.Bucket,
	}, nil
}

func (c *BboltCache) Close() error {
	return c.db.Close()
}

func (c *BboltCache) Set(key string, meta model.FileMeta) error {
	return c.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(c.bucket))
		if b == nil {
			return ErrBucketNotFound
		}
		val, err := json.Marshal(meta)
		if err != nil {
			return err
		}
		return b.Put([]byte(key), val)
	})
}

func (c *BboltCache) Get(key string) (*model.FileMeta, error) {
	var meta model.FileMeta
	err := c.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(c.bucket))
		if b == nil {
			return ErrBucketNotFound
		}
		val := b.Get([]byte(key))
		if val == nil {
			return ErrKeyNotFound
		}
		return json.Unmarshal(val, &meta)
	})
	if err != nil {
		return nil, err
	}
	return &meta, nil
}

func (c *BboltCache) BatchSet(entries map[string]model.FileMeta) error {
	return c.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(c.bucket))
		if b == nil {
			return ErrBucketNotFound
		}
		for key, meta := range entries {
			val, err := json.Marshal(meta)
			if err != nil {
				return err
			}
			if err := b.Put([]byte(key), val); err != nil {
				return err
			}
		}
		return nil
	})
}

func (c *BboltCache) DumpAll() (map[string]model.FileMeta, error) {
	results := make(map[string]model.FileMeta)

	err := c.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(c.bucket))
		if b == nil {
			return ErrBucketNotFound
		}

		return b.ForEach(func(k, v []byte) error {
			var meta model.FileMeta
			if err := json.Unmarshal(v, &meta); err != nil {
				return fmt.Errorf("unmarshal error for key %s: %w", k, err)
			}
			results[string(k)] = meta
			return nil
		})
	})

	return results, err
}

func (c *BboltCache) GetByPrefix(prefix string) (map[string]model.FileMeta, error) {
	results := make(map[string]model.FileMeta)

	err := c.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(c.bucket))
		if b == nil {
			return ErrBucketNotFound
		}

		c := b.Cursor()

		for k, v := c.Seek([]byte(prefix)); k != nil && strings.HasPrefix(string(k), prefix); k, v = c.Next() {
			var meta model.FileMeta
			if err := json.Unmarshal(v, &meta); err != nil {
				return fmt.Errorf("unmarshal error for key %s: %w", k, err)
			}
			results[string(k)] = meta
		}

		return nil
	})

	return results, err
}

func (c *BboltCache) Delete(key string) error {
	return c.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(c.bucket))
		if b == nil {
			return ErrBucketNotFound
		}

		// Try to get the key first to check existence
		val := b.Get([]byte(key))
		if val == nil {
			return ErrKeyNotFound
		}

		// Delete the key
		return b.Delete([]byte(key))
	})
}

func (c *BboltCache) Count() (int64, error) {
	var count int64 = 0
	err := c.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(c.bucket))
		return b.ForEach(func(k, v []byte) error {
			count++
			return nil
		})
	})
	return count, err
}

// IterateBatches streams cache entries in batches of the specified size.
// Uses short-lived sequential transactions to allow interleaved write operations.
func (c *BboltCache) IterateBatches(ctx context.Context, batchSize int) (<-chan map[string]model.FileMeta, <-chan error) {
	batchCh := make(chan map[string]model.FileMeta)
	errCh := make(chan error, 1)

	go func() {
		defer close(batchCh)
		defer close(errCh)

		var nextKey []byte = nil // Position tracker for resuming iteration

		for {
			// Read one batch in a short-lived transaction
			batch, resumeKey, err := c.readOneBatch(ctx, nextKey, batchSize)
			if err != nil {
				errCh <- err
				return
			}

			// If batch is empty, iteration is complete
			if len(batch) == 0 {
				return
			}

			// Send batch to consumer (transaction is already closed at this point)
			select {
			case batchCh <- batch:
				// If resumeKey is nil, we've reached the end of iteration
				if resumeKey == nil {
					return
				}
				nextKey = resumeKey // Save position for next iteration
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}
		}
	}()

	return batchCh, errCh
}

// readOneBatch reads a single batch of entries in a short-lived transaction.
// Returns the batch, the next key to resume from, and any error.
func (c *BboltCache) readOneBatch(ctx context.Context, startKey []byte, batchSize int) (
	batch map[string]model.FileMeta,
	nextKey []byte,
	err error,
) {
	batch = make(map[string]model.FileMeta, batchSize)

	err = c.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(c.bucket))
		if b == nil {
			return ErrBucketNotFound
		}

		cursor := b.Cursor()

		// Position cursor at start key
		var k, v []byte
		if startKey == nil {
			k, v = cursor.First()
		} else {
			k, v = cursor.Seek(startKey)
		}

		// Read up to batchSize entries
		for ; k != nil && len(batch) < batchSize; k, v = cursor.Next() {
			// Check context cancellation
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			var meta model.FileMeta
			if err := json.Unmarshal(v, &meta); err != nil {
				return fmt.Errorf("unmarshal error for key %s: %w", k, err)
			}

			batch[string(k)] = meta
		}

		// Save next key for resuming iteration
		// IMPORTANT: Must deep copy the key because bbolt keys are only valid during the transaction
		if k != nil {
			nextKey = append([]byte(nil), k...)
		}

		return nil
	}) // Transaction closes here - read lock is released!

	return batch, nextKey, err
}

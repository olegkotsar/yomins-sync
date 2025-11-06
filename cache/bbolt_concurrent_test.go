package cache

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/olegkotsar/yomins-sync/model"
	"github.com/stretchr/testify/require"
)

// TestIterateBatches_ConcurrentWrites verifies that writes can happen during iteration
// without causing deadlocks. This is the core benefit of Solution 3.
func TestIterateBatches_ConcurrentWrites(t *testing.T) {
	c, cleanup := newTestBboltCache(t)
	defer cleanup()

	// Setup: Add 5000 SYNCED entries
	entries := make(map[string]model.FileMeta)
	for i := 0; i < 5000; i++ {
		key := fmt.Sprintf("file%05d", i)
		entries[key] = model.FileMeta{
			Hash:    fmt.Sprintf("hash%d", i),
			ModTime: int64(i),
			Size:    int64(i * 10),
			Status:  model.StatusSynced,
		}
	}
	require.NoError(t, c.BatchSet(entries))

	ctx := context.Background()
	batchCh, errCh := c.IterateBatches(ctx, 500) // 500 entries per batch = 10 batches

	// Track results
	readCount := 0
	writeCount := 0
	var wg sync.WaitGroup
	var mu sync.Mutex

	// Consumer goroutine: reads batches and writes updates concurrently
	wg.Add(1)
	go func() {
		defer wg.Done()
		updates := make(map[string]model.FileMeta, 100)

		for {
			select {
			case err, ok := <-errCh:
				if ok && err != nil {
					t.Errorf("Iteration error: %v", err)
					return
				}
			case batch, ok := <-batchCh:
				if !ok {
					// Final write
					if len(updates) > 0 {
						if err := c.BatchSet(updates); err != nil {
							t.Errorf("Final BatchSet failed: %v", err)
							return
						}
						mu.Lock()
						writeCount += len(updates)
						mu.Unlock()
					}
					return
				}

				mu.Lock()
				readCount += len(batch)
				mu.Unlock()

				// Simulate what prepareForScan does: change SYNCED -> TEMP_DELETED
				for key, meta := range batch {
					if meta.Status == model.StatusSynced {
						meta.Status = model.StatusTempDeleted
						updates[key] = meta
					}

					// Write every 100 entries (this happens DURING iteration!)
					if len(updates) >= 100 {
						if err := c.BatchSet(updates); err != nil {
							t.Errorf("BatchSet failed: %v", err)
							return
						}
						mu.Lock()
						writeCount += len(updates)
						mu.Unlock()
						updates = make(map[string]model.FileMeta, 100)
					}
				}
			}
		}
	}()

	// Wait for completion
	wg.Wait()

	// Verify: All 5000 entries were read
	require.Equal(t, 5000, readCount, "Should read all 5000 entries")

	// Verify: All 5000 entries were written
	require.Equal(t, 5000, writeCount, "Should write all 5000 entries")

	// Verify: All entries are now TEMP_DELETED
	allEntries, err := c.DumpAll()
	require.NoError(t, err)
	require.Len(t, allEntries, 5000)

	for key, meta := range allEntries {
		require.Equal(t, model.StatusTempDeleted, meta.Status,
			"Entry %s should be TEMP_DELETED", key)
	}
}

// TestIterateBatches_EntryDeletedDuringIteration verifies that deleting entries
// during iteration doesn't cause skips or panics
func TestIterateBatches_EntryDeletedDuringIteration(t *testing.T) {
	c, cleanup := newTestBboltCache(t)
	defer cleanup()

	// Setup: Add 1000 entries
	entries := make(map[string]model.FileMeta)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("file%04d", i)
		entries[key] = model.FileMeta{
			Hash:    fmt.Sprintf("hash%d", i),
			ModTime: int64(i),
			Size:    int64(i),
			Status:  model.StatusNew,
		}
	}
	require.NoError(t, c.BatchSet(entries))

	ctx := context.Background()
	batchCh, errCh := c.IterateBatches(ctx, 100)

	// Track which keys we've seen
	seenKeys := make(map[string]bool)
	var mu sync.Mutex
	var wg sync.WaitGroup

	// Consumer: delete every 5th entry during iteration
	wg.Add(1)
	go func() {
		defer wg.Done()
		deleteCount := 0

		for {
			select {
			case err, ok := <-errCh:
				if ok && err != nil {
					t.Errorf("Iteration error: %v", err)
					return
				}
			case batch, ok := <-batchCh:
				if !ok {
					t.Logf("Deleted %d entries during iteration", deleteCount)
					return
				}

				for key := range batch {
					mu.Lock()
					seenKeys[key] = true
					mu.Unlock()

					// Delete every 5th entry
					if deleteCount%5 == 0 {
						// Note: This may fail if entry already deleted, ignore error
						_ = c.Delete(key)
						deleteCount++
					}
				}
			}
		}
	}()

	wg.Wait()

	// Verify: We should have seen all 1000 entries (even if some were deleted after being read)
	mu.Lock()
	seenCount := len(seenKeys)
	mu.Unlock()

	require.Equal(t, 1000, seenCount, "Should have seen all 1000 entries during iteration")

	// Verify: Some entries were actually deleted
	finalEntries, err := c.DumpAll()
	require.NoError(t, err)
	require.Less(t, len(finalEntries), 1000, "Some entries should have been deleted")
}

// TestIterateBatches_LargeDataset tests iteration with a larger dataset
// to verify performance and memory efficiency
func TestIterateBatches_LargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large dataset test in short mode")
	}

	c, cleanup := newTestBboltCache(t)
	defer cleanup()

	// Setup: Add 100k entries
	const totalEntries = 100000
	t.Logf("Creating %d entries...", totalEntries)

	batchSize := 1000
	for i := 0; i < totalEntries; i += batchSize {
		batch := make(map[string]model.FileMeta, batchSize)
		for j := 0; j < batchSize && i+j < totalEntries; j++ {
			key := fmt.Sprintf("file%06d", i+j)
			batch[key] = model.FileMeta{
				Hash:    fmt.Sprintf("hash%d", i+j),
				ModTime: int64(i + j),
				Size:    int64((i + j) * 10),
				Status:  model.StatusSynced,
			}
		}
		require.NoError(t, c.BatchSet(batch))
	}

	t.Logf("Starting iteration and concurrent writes...")
	start := time.Now()

	ctx := context.Background()
	batchCh, errCh := c.IterateBatches(ctx, 10000) // 10k per batch

	readCount := 0
	writeCount := 0

	// Process batches with concurrent writes
	updates := make(map[string]model.FileMeta, 1000)
	for {
		select {
		case err, ok := <-errCh:
			if ok && err != nil {
				t.Fatalf("Iteration error: %v", err)
			}
		case batch, ok := <-batchCh:
			if !ok {
				// Final write
				if len(updates) > 0 {
					require.NoError(t, c.BatchSet(updates))
					writeCount += len(updates)
				}
				goto done
			}

			readCount += len(batch)

			// Update entries
			for key, meta := range batch {
				meta.Status = model.StatusTempDeleted
				updates[key] = meta

				if len(updates) >= 1000 {
					require.NoError(t, c.BatchSet(updates))
					writeCount += len(updates)
					updates = make(map[string]model.FileMeta, 1000)
				}
			}
		}
	}

done:
	elapsed := time.Since(start)
	t.Logf("Processed %d entries in %v (%.0f entries/sec)",
		readCount, elapsed, float64(readCount)/elapsed.Seconds())

	require.Equal(t, totalEntries, readCount, "Should read all entries")
	require.Equal(t, totalEntries, writeCount, "Should write all entries")

	// Performance check: should handle 100k entries in reasonable time
	require.Less(t, elapsed, 10*time.Second, "Should complete in under 10 seconds")
}

// TestIterateBatches_ConcurrentReads verifies multiple concurrent iterations
// don't interfere with each other
func TestIterateBatches_ConcurrentReads(t *testing.T) {
	c, cleanup := newTestBboltCache(t)
	defer cleanup()

	// Setup: Add 1000 entries
	entries := make(map[string]model.FileMeta)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("file%04d", i)
		entries[key] = model.FileMeta{
			Hash:    fmt.Sprintf("hash%d", i),
			ModTime: int64(i),
			Size:    int64(i),
			Status:  model.StatusSynced,
		}
	}
	require.NoError(t, c.BatchSet(entries))

	ctx := context.Background()
	var wg sync.WaitGroup
	results := make([]int, 3)

	// Start 3 concurrent iterations
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			batchCh, errCh := c.IterateBatches(ctx, 100)
			count := 0

			for {
				select {
				case err, ok := <-errCh:
					if ok && err != nil {
						t.Errorf("Reader %d error: %v", index, err)
						return
					}
				case batch, ok := <-batchCh:
					if !ok {
						results[index] = count
						return
					}
					count += len(batch)
				}
			}
		}(i)
	}

	wg.Wait()

	// All three readers should have seen all 1000 entries
	for i, count := range results {
		require.Equal(t, 1000, count, "Reader %d should see all entries", i)
	}
}

// TestIterateBatches_WritesDuringReadSameKeys verifies that updating the same keys
// being read doesn't cause corruption
func TestIterateBatches_WritesDuringReadSameKeys(t *testing.T) {
	c, cleanup := newTestBboltCache(t)
	defer cleanup()

	// Setup: Add 500 SYNCED entries
	entries := make(map[string]model.FileMeta)
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("file%03d", i)
		entries[key] = model.FileMeta{
			Hash:    fmt.Sprintf("original_hash%d", i),
			ModTime: int64(i),
			Size:    int64(i),
			Status:  model.StatusSynced,
		}
	}
	require.NoError(t, c.BatchSet(entries))

	ctx := context.Background()
	batchCh, errCh := c.IterateBatches(ctx, 50)

	readEntries := make(map[string]model.FileMeta)
	var mu sync.Mutex

	// Read and update concurrently
	for {
		select {
		case err, ok := <-errCh:
			if ok && err != nil {
				t.Fatalf("Error: %v", err)
			}
		case batch, ok := <-batchCh:
			if !ok {
				goto done
			}

			// Store what we read
			mu.Lock()
			for k, v := range batch {
				readEntries[k] = v
			}
			mu.Unlock()

			// Immediately update the same entries
			updates := make(map[string]model.FileMeta)
			for key, meta := range batch {
				meta.Status = model.StatusTempDeleted
				meta.Hash = fmt.Sprintf("updated_hash_%s", key)
				updates[key] = meta
			}
			require.NoError(t, c.BatchSet(updates))
		}
	}

done:
	// Verify: We read all 500 entries
	require.Len(t, readEntries, 500)

	// Verify: All entries are now updated in the database
	finalEntries, err := c.DumpAll()
	require.NoError(t, err)
	require.Len(t, finalEntries, 500)

	for _, meta := range finalEntries {
		require.Equal(t, model.StatusTempDeleted, meta.Status)
		require.Contains(t, meta.Hash, "updated_hash")
	}

	// The read entries might show either old or new state (due to timing),
	// but they should all exist
	for key := range readEntries {
		_, exists := finalEntries[key]
		require.True(t, exists, "Entry %s should still exist", key)
	}
}

package cache

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/olegkotsar/yomins-sync/config"
	"github.com/olegkotsar/yomins-sync/model"
	"github.com/stretchr/testify/require"
)

func newTestBboltCache(t *testing.T) (*BboltCache, func()) {
	t.Helper()
	tmpFile, err := os.CreateTemp("", "bolt-*.db")
	require.NoError(t, err)

	cfg := &config.BboltConfig{
		Path: tmpFile.Name(),
	}
	c, err := NewBboltCache(cfg)
	require.NoError(t, err)

	// Cleanup function
	return c, func() {
		c.Close()
		os.Remove(tmpFile.Name())
	}
}

func TestOpenInvalidPath(t *testing.T) {
	cfg := &config.BboltConfig{
		Path: "/invalid/path.db",
	}
	_, err := NewBboltCache(cfg)
	require.Error(t, err)
}

func TestSetAndGet(t *testing.T) {
	c, cleanup := newTestBboltCache(t)
	defer cleanup()

	meta := model.FileMeta{
		Hash:    "abc123",
		ModTime: 12345,
		Size:    100,
	}

	err := c.Set("file1", meta)
	require.NoError(t, err)

	got, err := c.Get("file1")
	require.NoError(t, err)
	require.Equal(t, meta, *got)

	// Key not found
	_, err = c.Get("missing")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

func TestBatchSet(t *testing.T) {
	c, cleanup := newTestBboltCache(t)
	defer cleanup()

	entries := map[string]model.FileMeta{
		"file1": {Hash: "hash1", ModTime: 100, Size: 10},
		"file2": {Hash: "hash2", ModTime: 200, Size: 20},
	}

	err := c.BatchSet(entries)
	require.NoError(t, err)

	for k, v := range entries {
		got, err := c.Get(k)
		require.NoError(t, err)
		require.Equal(t, v, *got)
	}
}

func TestGetByPrefix(t *testing.T) {
	c, cleanup := newTestBboltCache(t)
	defer cleanup()

	entries := map[string]model.FileMeta{
		"dir/file1": {Hash: "h1", ModTime: 1, Size: 11},
		"dir/file2": {Hash: "h2", ModTime: 2, Size: 22},
		"other":     {Hash: "h3", ModTime: 3, Size: 33},
	}
	require.NoError(t, c.BatchSet(entries))

	results, err := c.GetByPrefix("dir/")
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.Contains(t, results, "dir/file1")
	require.Contains(t, results, "dir/file2")
}

func TestDumpAll(t *testing.T) {
	c, cleanup := newTestBboltCache(t)
	defer cleanup()

	// Add a test entry
	require.NoError(t, c.Set("file1", model.FileMeta{Hash: "abc", ModTime: 123, Size: 50}))

	// Call DumpAll and check result
	results, err := c.DumpAll()
	require.NoError(t, err)

	// Validate that "file1" exists in results
	meta, ok := results["file1"]
	require.True(t, ok, "expected key 'file1' in results")

	// Validate meta fields
	require.Equal(t, "abc", meta.Hash)
	require.Equal(t, int64(123), meta.ModTime)
	require.Equal(t, int64(50), meta.Size)
}

func TestIterateBatches_Empty(t *testing.T) {
	c, cleanup := newTestBboltCache(t)
	defer cleanup()

	ctx := context.Background()
	batchCh, errCh := c.IterateBatches(ctx, 10)

	batchCount := 0
	for {
		select {
		case batch, ok := <-batchCh:
			if !ok {
				// Channel closed, iteration complete
				goto done
			}
			batchCount++
			require.NotNil(t, batch)
		case err, ok := <-errCh:
			if ok {
				require.NoError(t, err)
			}
		}
	}
done:
	require.Equal(t, 0, batchCount, "empty cache should not send any batches")
}

func TestIterateBatches_LessThanBatchSize(t *testing.T) {
	c, cleanup := newTestBboltCache(t)
	defer cleanup()

	// Add 3 entries
	entries := map[string]model.FileMeta{
		"file1": {Hash: "hash1", ModTime: 100, Size: 10, Status: model.StatusNew},
		"file2": {Hash: "hash2", ModTime: 200, Size: 20, Status: model.StatusSynced},
		"file3": {Hash: "hash3", ModTime: 300, Size: 30, Status: model.StatusTempDeleted},
	}
	require.NoError(t, c.BatchSet(entries))

	ctx := context.Background()
	batchCh, errCh := c.IterateBatches(ctx, 10) // batch size larger than entries

	allReceived := make(map[string]model.FileMeta)
	batchCount := 0

	for {
		select {
		case batch, ok := <-batchCh:
			if !ok {
				goto done
			}
			batchCount++
			for k, v := range batch {
				allReceived[k] = v
			}
		case err, ok := <-errCh:
			if ok {
				require.NoError(t, err)
			}
		}
	}
done:
	require.Equal(t, 1, batchCount, "should send exactly one batch")
	require.Len(t, allReceived, 3, "should receive all 3 entries")
	require.Equal(t, entries, allReceived)
}

func TestIterateBatches_ExactlyBatchSize(t *testing.T) {
	c, cleanup := newTestBboltCache(t)
	defer cleanup()

	// Add exactly 5 entries
	entries := map[string]model.FileMeta{
		"file1": {Hash: "hash1", ModTime: 100, Size: 10},
		"file2": {Hash: "hash2", ModTime: 200, Size: 20},
		"file3": {Hash: "hash3", ModTime: 300, Size: 30},
		"file4": {Hash: "hash4", ModTime: 400, Size: 40},
		"file5": {Hash: "hash5", ModTime: 500, Size: 50},
	}
	require.NoError(t, c.BatchSet(entries))

	ctx := context.Background()
	batchCh, errCh := c.IterateBatches(ctx, 5) // batch size equals number of entries

	allReceived := make(map[string]model.FileMeta)
	batchCount := 0

	for {
		select {
		case batch, ok := <-batchCh:
			if !ok {
				goto done
			}
			batchCount++
			for k, v := range batch {
				allReceived[k] = v
			}
		case err, ok := <-errCh:
			if ok {
				require.NoError(t, err)
			}
		}
	}
done:
	require.Equal(t, 1, batchCount, "should send exactly one batch")
	require.Len(t, allReceived, 5, "should receive all 5 entries")
	require.Equal(t, entries, allReceived)
}

func TestIterateBatches_MultipleBatches(t *testing.T) {
	c, cleanup := newTestBboltCache(t)
	defer cleanup()

	// Add 25 entries
	entries := make(map[string]model.FileMeta)
	for i := 0; i < 25; i++ {
		key := fmt.Sprintf("file%02d", i)
		entries[key] = model.FileMeta{
			Hash:    fmt.Sprintf("hash%d", i),
			ModTime: int64(i * 100),
			Size:    int64(i * 10),
			Status:  model.StatusSynced,
		}
	}
	require.NoError(t, c.BatchSet(entries))

	ctx := context.Background()
	batchCh, errCh := c.IterateBatches(ctx, 10) // batch size 10, expect 3 batches (10, 10, 5)

	allReceived := make(map[string]model.FileMeta)
	batchCount := 0

	for {
		select {
		case batch, ok := <-batchCh:
			if !ok {
				goto done
			}
			batchCount++
			require.NotEmpty(t, batch, "batch should not be empty")
			for k, v := range batch {
				allReceived[k] = v
			}
		case err, ok := <-errCh:
			if ok {
				require.NoError(t, err)
			}
		}
	}
done:
	require.Equal(t, 3, batchCount, "should send 3 batches (10, 10, 5)")
	require.Len(t, allReceived, 25, "should receive all 25 entries")
	require.Equal(t, entries, allReceived)
}

func TestIterateBatches_ContextCancellation(t *testing.T) {
	c, cleanup := newTestBboltCache(t)
	defer cleanup()

	// Add 100 entries
	entries := make(map[string]model.FileMeta)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("file%03d", i)
		entries[key] = model.FileMeta{
			Hash:    fmt.Sprintf("hash%d", i),
			ModTime: int64(i),
			Size:    int64(i),
		}
	}
	require.NoError(t, c.BatchSet(entries))

	ctx, cancel := context.WithCancel(context.Background())
	batchCh, errCh := c.IterateBatches(ctx, 10)

	batchCount := 0
	for {
		select {
		case _, ok := <-batchCh:
			if !ok {
				goto done
			}
			batchCount++
			if batchCount == 2 {
				// Cancel after receiving 2 batches
				cancel()
			}
		case err, ok := <-errCh:
			if ok && err != nil {
				require.ErrorIs(t, err, context.Canceled)
				goto done
			}
		}
	}
done:
	require.LessOrEqual(t, batchCount, 3, "should stop iteration after context cancellation")
}

func TestIterateBatches_PreserveStatuses(t *testing.T) {
	c, cleanup := newTestBboltCache(t)
	defer cleanup()

	// Add entries with different statuses
	entries := map[string]model.FileMeta{
		"new_file":     {Hash: "h1", ModTime: 1, Size: 1, Status: model.StatusNew},
		"synced_file":  {Hash: "h2", ModTime: 2, Size: 2, Status: model.StatusSynced},
		"deleted_file": {Hash: "h3", ModTime: 3, Size: 3, Status: model.StatusDeletedInSource},
		"temp_del":     {Hash: "h4", ModTime: 4, Size: 4, Status: model.StatusTempDeleted},
		"error_file":   {Hash: "h5", ModTime: 5, Size: 5, Status: model.StatusError},
	}
	require.NoError(t, c.BatchSet(entries))

	ctx := context.Background()
	batchCh, errCh := c.IterateBatches(ctx, 10)

	allReceived := make(map[string]model.FileMeta)
	for {
		select {
		case batch, ok := <-batchCh:
			if !ok {
				goto done
			}
			for k, v := range batch {
				allReceived[k] = v
			}
		case err, ok := <-errCh:
			if ok {
				require.NoError(t, err)
			}
		}
	}
done:
	require.Len(t, allReceived, 5)

	// Verify all statuses are preserved
	require.Equal(t, model.StatusNew, allReceived["new_file"].Status)
	require.Equal(t, model.StatusSynced, allReceived["synced_file"].Status)
	require.Equal(t, model.StatusDeletedInSource, allReceived["deleted_file"].Status)
	require.Equal(t, model.StatusTempDeleted, allReceived["temp_del"].Status)
	require.Equal(t, model.StatusError, allReceived["error_file"].Status)
}

func TestDelete(t *testing.T) {
	c, cleanup := newTestBboltCache(t)
	defer cleanup()

	// Add test entries
	entries := map[string]model.FileMeta{
		"file1": {Hash: "hash1", ModTime: 100, Size: 10},
		"file2": {Hash: "hash2", ModTime: 200, Size: 20},
		"file3": {Hash: "hash3", ModTime: 300, Size: 30},
	}
	require.NoError(t, c.BatchSet(entries))

	// Delete file1
	err := c.Delete("file1")
	require.NoError(t, err)

	// Verify file1 is deleted
	_, err = c.Get("file1")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrKeyNotFound)

	// Verify other files still exist
	got, err := c.Get("file2")
	require.NoError(t, err)
	require.Equal(t, entries["file2"], *got)

	got, err = c.Get("file3")
	require.NoError(t, err)
	require.Equal(t, entries["file3"], *got)

	// Delete file2
	err = c.Delete("file2")
	require.NoError(t, err)

	// Verify file2 is deleted
	_, err = c.Get("file2")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrKeyNotFound)

	// Try to delete non-existent key
	err = c.Delete("nonexistent")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrKeyNotFound)

	// Delete remaining file
	err = c.Delete("file3")
	require.NoError(t, err)

	// Verify all files are deleted
	all, err := c.DumpAll()
	require.NoError(t, err)
	require.Empty(t, all)
}

func TestCount(t *testing.T) {
	c, cleanup := newTestBboltCache(t)
	defer cleanup()

	// Initially, count should be 0
	count, err := c.Count()
	require.NoError(t, err)
	require.Equal(t, int64(0), count)

	// Add one entry
	require.NoError(t, c.Set("file1", model.FileMeta{Hash: "h1", ModTime: 1, Size: 1}))

	count, err = c.Count()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	// Add more entries
	entries := map[string]model.FileMeta{
		"file2": {Hash: "h2", ModTime: 2, Size: 2},
		"file3": {Hash: "h3", ModTime: 3, Size: 3},
		"file4": {Hash: "h4", ModTime: 4, Size: 4},
	}
	require.NoError(t, c.BatchSet(entries))

	count, err = c.Count()
	require.NoError(t, err)
	require.Equal(t, int64(4), count)

	// Delete one entry
	require.NoError(t, c.Delete("file2"))

	count, err = c.Count()
	require.NoError(t, err)
	require.Equal(t, int64(3), count)

	// Add many entries
	manyEntries := make(map[string]model.FileMeta)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("bulk_file%d", i)
		manyEntries[key] = model.FileMeta{
			Hash:    fmt.Sprintf("hash%d", i),
			ModTime: int64(i),
			Size:    int64(i * 10),
		}
	}
	require.NoError(t, c.BatchSet(manyEntries))

	count, err = c.Count()
	require.NoError(t, err)
	require.Equal(t, int64(103), count) // 3 remaining + 100 new
}

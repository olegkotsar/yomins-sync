package processor

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/olegkotsar/yomins-sync/model"
	"github.com/stretchr/testify/require"
)

// anyContentSource returns fixed content for any requested key
type anyContentSource struct {
	mockSourceProvider
}

func (s *anyContentSource) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("content")), nil
}

// countingDestination counts operations without accumulating per-file state,
// so memory measurements reflect the processor, not the mock
type countingDestination struct {
	uploads     int64
	deletes     int64
	workerCount int
}

func (d *countingDestination) Upload(ctx context.Context, path string, content io.Reader) error {
	if _, err := io.Copy(io.Discard, content); err != nil {
		return err
	}
	atomic.AddInt64(&d.uploads, 1)
	return nil
}

func (d *countingDestination) Delete(ctx context.Context, path string) error {
	atomic.AddInt64(&d.deletes, 1)
	return nil
}

func (d *countingDestination) FileExists(ctx context.Context, path string) (bool, error) {
	return false, nil
}

func (d *countingDestination) GetWorkerCount() int { return d.workerCount }
func (d *countingDestination) Close() error        { return nil }

func populateCacheWithStatus(t *testing.T, c interface {
	BatchSet(map[string]model.FileMeta) error
}, numFiles int, status model.FileStatus) {
	t.Helper()
	batch := make(map[string]model.FileMeta, 1000)
	for i := 0; i < numFiles; i++ {
		batch[fmt.Sprintf("file%07d", i)] = model.FileMeta{
			Hash:    fmt.Sprintf("hash%d", i),
			Size:    int64(i),
			ModTime: int64(i * 1000),
			Status:  status,
		}
		if len(batch) >= 1000 {
			require.NoError(t, c.BatchSet(batch))
			batch = make(map[string]model.FileMeta, 1000)
		}
	}
	if len(batch) > 0 {
		require.NoError(t, c.BatchSet(batch))
	}
}

// TestSyncNewFilesActual_NoDeadlock_MultipleBatches verifies that streaming
// jobs from IterateBatches while the collector calls BatchSet on the same
// cache completes without deadlock and processes every file exactly once.
func TestSyncNewFilesActual_NoDeadlock_MultipleBatches(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	// More entries than the read batch size (10,000) to force multiple batches
	numFiles := 25000
	populateCacheWithStatus(t, c, numFiles, model.StatusNew)

	mockSrc := &anyContentSource{}
	mockDest := &countingDestination{workerCount: 8}

	r := NewRunner(c, mockSrc, mockDest, nil, false)

	// Use a timeout context to detect deadlocks
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	start := time.Now()
	stats, err := r.syncNewFiles(ctx)
	elapsed := time.Since(start)

	require.NoError(t, err, "syncNewFiles should not deadlock")
	require.NotNil(t, stats)
	require.Equal(t, int64(numFiles), stats.TotalScanned)
	require.Equal(t, int64(numFiles), stats.NewFilesFound)
	require.Equal(t, int64(numFiles), stats.SuccessfullySynced)
	require.Equal(t, int64(0), stats.FailedToSync)
	require.Equal(t, int64(numFiles), atomic.LoadInt64(&mockDest.uploads),
		"each file must be uploaded exactly once")

	t.Logf("syncNewFiles processed %d files in %v (%.0f files/sec)",
		numFiles, elapsed, float64(numFiles)/elapsed.Seconds())

	// Sample verification - all files should now be SYNCED
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("file%07d", i*(numFiles/100))
		meta, err := c.Get(key)
		require.NoError(t, err)
		require.Equal(t, model.StatusSynced, meta.Status, "File %s should be SYNCED", key)
	}
}

// TestDeleteRemovedFilesActual_NoDeadlock_MultipleBatches verifies that
// deleting cache entries while iterating the same cache completes without
// deadlock and removes every file exactly once.
func TestDeleteRemovedFilesActual_NoDeadlock_MultipleBatches(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	// More entries than the read batch size (10,000) to force multiple batches
	numFiles := 25000
	populateCacheWithStatus(t, c, numFiles, model.StatusDeletedInSource)

	mockDest := &countingDestination{workerCount: 8}

	r := NewRunner(c, nil, mockDest, nil, false)

	// Use a timeout context to detect deadlocks
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	start := time.Now()
	stats, err := r.deleteRemovedFiles(ctx)
	elapsed := time.Since(start)

	require.NoError(t, err, "deleteRemovedFiles should not deadlock")
	require.NotNil(t, stats)
	require.Equal(t, int64(numFiles), stats.TotalScanned)
	require.Equal(t, int64(numFiles), stats.DeletedOnS3Found)
	require.Equal(t, int64(numFiles), stats.SuccessfullyDeleted)
	require.Equal(t, int64(0), stats.FailedToDelete)
	require.Equal(t, int64(numFiles), atomic.LoadInt64(&mockDest.deletes),
		"each file must be deleted exactly once")

	t.Logf("deleteRemovedFiles processed %d files in %v (%.0f files/sec)",
		numFiles, elapsed, float64(numFiles)/elapsed.Seconds())

	// All entries must be gone from the cache
	count, err := c.Count()
	require.NoError(t, err)
	require.Equal(t, int64(0), count, "cache should be empty after deletion")
}

// TestSyncNewFilesActual_MemoryUsage verifies the upload phase streams NEW
// files instead of materializing them: heap usage must stay flat regardless
// of how many files are uploaded.
func TestSyncNewFilesActual_MemoryUsage(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory test in short mode")
	}

	c, cleanup := newTestCache(t)
	defer cleanup()

	const totalEntries = 200000
	t.Logf("Creating %d NEW entries...", totalEntries)
	populateCacheWithStatus(t, c, totalEntries, model.StatusNew)

	mockSrc := &anyContentSource{}
	mockDest := &countingDestination{workerCount: 8}
	r := NewRunner(c, mockSrc, mockDest, nil, false)

	// Force GC and get baseline
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)
	baselineAlloc := m1.Alloc

	t.Logf("Baseline memory: %.2f MB", float64(baselineAlloc)/(1024*1024))

	// Capture peak memory during operation
	done := make(chan bool)
	peakAlloc := uint64(0)

	go func() {
		ticker := time.NewTicker(25 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				if m.Alloc > peakAlloc {
					peakAlloc = m.Alloc
				}
			}
		}
	}()

	start := time.Now()
	stats, err := r.syncNewFiles(context.Background())
	elapsed := time.Since(start)
	close(done)

	require.NoError(t, err)
	require.Equal(t, int64(totalEntries), stats.SuccessfullySynced)

	peakDelta := float64(peakAlloc-baselineAlloc) / (1024 * 1024)
	perEntryBytes := (peakDelta * 1024 * 1024) / float64(totalEntries)

	t.Logf("Results for %d entries:", totalEntries)
	t.Logf("  Time: %v (%.0f entries/sec)", elapsed, float64(totalEntries)/elapsed.Seconds())
	t.Logf("  Peak memory: %.2f MB", peakDelta)
	t.Logf("  Memory per entry: %.2f bytes", perEntryBytes)

	// With the old collect-everything implementation 200k entries needed
	// ~60+ MB (map + full-size channels); streaming must stay far below that
	require.Less(t, peakDelta, 30.0, "Peak memory should stay flat (streaming), not grow with file count")
}

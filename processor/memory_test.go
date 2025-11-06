package processor

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/olegkotsar/yomins-sync/cache"
	"github.com/olegkotsar/yomins-sync/config"
	"github.com/olegkotsar/yomins-sync/logger"
	"github.com/olegkotsar/yomins-sync/model"
	"github.com/stretchr/testify/require"
)

// TestPrepareForScan_MemoryUsage measures actual memory consumption
func TestPrepareForScan_MemoryUsage(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory test in short mode")
	}

	// Setup: Create test cache with large dataset
	tmpFile, err := os.CreateTemp("", "memory-test-*.db")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	cacheCfg := &config.BboltConfig{Path: tmpFile.Name()}
	c, err := cache.NewBboltCache(cacheCfg)
	require.NoError(t, err)
	defer c.Close()

	// Add 100k SYNCED entries
	const totalEntries = 100000
	t.Logf("Creating %d SYNCED entries...", totalEntries)

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

	// Force GC and get baseline
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)
	baselineAlloc := m1.Alloc

	t.Logf("Baseline memory: %.2f MB", float64(baselineAlloc)/(1024*1024))

	// Run prepareForScan
	r := NewRunner(c, nil, nil, logger.NewNoOpLogger(), false)
	ctx := context.Background()

	// Capture peak memory during operation
	done := make(chan bool)
	peakAlloc := uint64(0)

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
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
	stats, err := r.prepareForScan(ctx)
	elapsed := time.Since(start)
	close(done)

	require.NoError(t, err)
	require.Equal(t, int64(totalEntries), stats.SyncedToTempDeleted)

	// Get final memory stats
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)

	peakDelta := float64(peakAlloc-baselineAlloc) / (1024 * 1024)
	finalDelta := float64(m2.Alloc-baselineAlloc) / (1024 * 1024)

	t.Logf("Results for %d entries:", totalEntries)
	t.Logf("  Time: %v (%.0f entries/sec)", elapsed, float64(totalEntries)/elapsed.Seconds())
	t.Logf("  Peak memory: %.2f MB", peakDelta)
	t.Logf("  Final memory: %.2f MB", finalDelta)
	t.Logf("  Memory per entry: %.2f bytes", (peakDelta*1024*1024)/float64(totalEntries))

	// Assertions
	perEntryBytes := (peakDelta * 1024 * 1024) / float64(totalEntries)
	require.Less(t, perEntryBytes, 100.0, "Should use less than 100 bytes per entry on average")
	require.Less(t, peakDelta, 100.0, "Peak memory should be less than 100 MB for 100k entries")
}

// TestBatchSizeImpact compares memory usage with different batch sizes
func TestBatchSizeImpact(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping batch size test in short mode")
	}

	testCases := []struct {
		readBatchSize  int
		writeBatchSize int
	}{
		{10000, 1000}, // Current settings
		{5000, 1000},  // Reduced read batch
		{2000, 1000},  // Further reduced
		{1000, 500},   // Matched sizes
	}

	const totalEntries = 50000

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("read=%d_write=%d", tc.readBatchSize, tc.writeBatchSize), func(t *testing.T) {
			// Setup cache
			tmpFile, err := os.CreateTemp("", "batch-test-*.db")
			require.NoError(t, err)
			defer os.Remove(tmpFile.Name())

			cacheCfg := &config.BboltConfig{Path: tmpFile.Name()}
			c, err := cache.NewBboltCache(cacheCfg)
			require.NoError(t, err)
			defer c.Close()

			// Populate
			batch := make(map[string]model.FileMeta, 1000)
			for i := 0; i < totalEntries; i++ {
				key := fmt.Sprintf("file%06d", i)
				batch[key] = model.FileMeta{
					Hash:    fmt.Sprintf("hash%d", i),
					ModTime: int64(i),
					Size:    int64(i * 10),
					Status:  model.StatusSynced,
				}
				if len(batch) >= 1000 {
					require.NoError(t, c.BatchSet(batch))
					batch = make(map[string]model.FileMeta, 1000)
				}
			}
			if len(batch) > 0 {
				require.NoError(t, c.BatchSet(batch))
			}

			// Measure with custom batch sizes
			runtime.GC()
			time.Sleep(50 * time.Millisecond)

			var m1 runtime.MemStats
			runtime.ReadMemStats(&m1)
			baseline := m1.Alloc

			// Run prepareForScan with custom batch sizes
			// Note: We'd need to modify prepareForScan to accept batch size parameters
			// For now, just measure with current implementation
			r := NewRunner(c, nil, nil, logger.NewNoOpLogger(), false)
			ctx := context.Background()

			peakAlloc := uint64(0)
			done := make(chan bool)

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
			_, err = r.prepareForScan(ctx)
			elapsed := time.Since(start)
			close(done)

			require.NoError(t, err)

			runtime.GC()
			time.Sleep(50 * time.Millisecond)

			peakMB := float64(peakAlloc-baseline) / (1024 * 1024)
			throughput := float64(totalEntries) / elapsed.Seconds()

			t.Logf("  Peak memory: %.2f MB | Throughput: %.0f entries/sec",
				peakMB, throughput)
		})
	}
}

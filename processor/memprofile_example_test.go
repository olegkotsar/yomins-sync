package processor

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/olegkotsar/yomins-sync/cache"
	"github.com/olegkotsar/yomins-sync/config"
	"github.com/olegkotsar/yomins-sync/logger"
	"github.com/olegkotsar/yomins-sync/model"
	"github.com/stretchr/testify/require"
)

// TestMemoryProfiling_Example demonstrates the memory profiling output
func TestMemoryProfiling_Example(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory profiling example in short mode")
	}

	// Setup cache with SYNCED entries
	tmpFile, err := os.CreateTemp("", "memprofile-test-*.db")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	cacheCfg := &config.BboltConfig{Path: tmpFile.Name()}
	c, err := cache.NewBboltCache(cacheCfg)
	require.NoError(t, err)
	defer c.Close()

	// Add 5000 SYNCED entries
	const totalEntries = 5000
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

	// Create logger that outputs to console
	logCfg := &config.LoggerConfig{
		Level:      config.LogLevelDebug,
		AddSource:  false,
		TimeFormat: "15:04:05",
	}
	log := logger.NewLogger(logCfg)

	// Run prepareForScan with memory profiling
	r := NewRunner(c, nil, nil, log, false)
	ctx := context.Background()

	t.Logf("Running prepareForScan with %d entries (memory profiling enabled)...", totalEntries)
	stats, err := r.prepareForScan(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(totalEntries), stats.SyncedToTempDeleted)

	t.Logf("Completed: processed=%d", stats.TotalProcessed)
}

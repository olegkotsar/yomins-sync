package processor

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/olegkotsar/yomins-sync/cache"
	"github.com/olegkotsar/yomins-sync/config"
	"github.com/olegkotsar/yomins-sync/model"
	"github.com/olegkotsar/yomins-sync/source"
)

func TestProcessSourceFiles_Performance_ProcessSourceFile(t *testing.T) {
	endpoint := os.Getenv("S3_ENDPOINT")
	accessKey := os.Getenv("S3_ACCESS_KEY")
	secretKey := os.Getenv("S3_SECRET_KEY")
	bucket := os.Getenv("S3_BUCKET")

	if endpoint == "" || accessKey == "" || secretKey == "" || bucket == "" {
		t.Skip("S3 credentials not provided, skipping integration test")
	}

	srcCfg := &config.S3Config{
		Endpoint:        endpoint,
		Bucket:          bucket,
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
	}
	common := &config.CommonSourceConfig{
		WorkerCount:     20,
		MaxRPS:          300,
		ListingStrategy: config.ListingStrategyByLevel,
		ListingLevel:    0,
	}
	src, err := source.NewS3Source(srcCfg, common)
	require.NoError(t, err)

	cachePath := "test-cache.db"
	defer os.Remove(cachePath)

	c, err := cache.NewBboltCache(&config.BboltConfig{
		Path: cachePath,
	})
	require.NoError(t, err)

	r := NewRunner(c, src, nil, nil, false)

	// Start the test
	ctx := context.Background()
	start := time.Now()
	stats, err := r.processSourceFiles(ctx)
	elapsed := time.Since(start)

	require.NoError(t, err)

	totalFiles, err := c.Count()
	require.NoError(t, err)

	fmt.Printf("ProcessSourceFiles completed in %s\n", elapsed)
	fmt.Printf("Total files in cache: %d\n", totalFiles)
	fmt.Printf("Statistics: %s\n", stats.String())
}

func TestProcessSourceFiles_Perfomence_PrepareForScan(t *testing.T) {
	endpoint := os.Getenv("S3_ENDPOINT")
	accessKey := os.Getenv("S3_ACCESS_KEY")
	secretKey := os.Getenv("S3_SECRET_KEY")
	bucket := os.Getenv("S3_BUCKET")

	if endpoint == "" || accessKey == "" || secretKey == "" || bucket == "" {
		t.Skip("S3 credentials not provided, skipping integration test")
	}

	srcCfg := &config.S3Config{
		Endpoint:        endpoint,
		Bucket:          bucket,
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
	}
	common := &config.CommonSourceConfig{
		WorkerCount:     20,
		MaxRPS:          300,
		ListingStrategy: config.ListingStrategyByLevel,
		ListingLevel:    3,
	}
	src, err := source.NewS3Source(srcCfg, common)
	require.NoError(t, err)

	cachePath := "test-cache.db"
	defer os.Remove(cachePath)

	c, err := cache.NewBboltCache(&config.BboltConfig{
		Path: cachePath,
	})
	require.NoError(t, err)

	r := NewRunner(c, src, nil, nil, false)

	// Start the test
	ctx := context.Background()

	start := time.Now()
	stats, err := r.prepareForScan(ctx)
	elapsed := time.Since(start)

	require.NoError(t, err)
	totalFiles, err := c.Count()
	require.NoError(t, err)
	fmt.Printf("PrepareForScan completed in %s\n", elapsed)
	fmt.Printf("Total files in cache: %d\n", totalFiles)
	fmt.Printf("Statistics: %s\n", stats.String())
}

// Unit tests for statistics tracking

func newTestCache(t *testing.T) (cache.CacheProvider, func()) {
	t.Helper()
	tmpFile, err := os.CreateTemp("", "test-cache-*.db")
	require.NoError(t, err)

	c, err := cache.NewBboltCache(&config.BboltConfig{
		Path: tmpFile.Name(),
	})
	require.NoError(t, err)

	return c, func() {
		c.Close()
		os.Remove(tmpFile.Name())
	}
}

func TestPrepareForScan_EmptyCache(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	r := NewRunner(c, nil, nil, nil, false)
	ctx := context.Background()

	stats, err := r.prepareForScan(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, int64(0), stats.TotalProcessed)
	require.Equal(t, int64(0), stats.SyncedToTempDeleted)
	require.Equal(t, int64(0), stats.NewRemained)
	require.Equal(t, int64(0), stats.DeletedOnS3Remained)
}

func TestPrepareForScan_OnlySynced(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	// Add 10 SYNCED files
	entries := make(map[string]model.FileMeta)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("file%d", i)
		entries[key] = model.FileMeta{
			Hash:    fmt.Sprintf("hash%d", i),
			Size:    int64(i * 100),
			ModTime: int64(i * 1000),
			Status:  model.StatusSynced,
		}
	}
	require.NoError(t, c.BatchSet(entries))

	r := NewRunner(c, nil, nil, nil, false)
	ctx := context.Background()

	stats, err := r.prepareForScan(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, int64(10), stats.TotalProcessed)
	require.Equal(t, int64(10), stats.SyncedToTempDeleted)
	require.Equal(t, int64(0), stats.NewRemained)
	require.Equal(t, int64(0), stats.DeletedOnS3Remained)

	// Verify that all files are now TEMP_DELETED
	for key := range entries {
		meta, err := c.Get(key)
		require.NoError(t, err)
		require.Equal(t, model.StatusTempDeleted, meta.Status)
	}
}

func TestPrepareForScan_MixedStatuses(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	entries := map[string]model.FileMeta{
		"synced1":  {Hash: "h1", Size: 10, ModTime: 100, Status: model.StatusSynced},
		"synced2":  {Hash: "h2", Size: 20, ModTime: 200, Status: model.StatusSynced},
		"synced3":  {Hash: "h3", Size: 30, ModTime: 300, Status: model.StatusSynced},
		"new1":     {Hash: "h4", Size: 40, ModTime: 400, Status: model.StatusNew},
		"new2":     {Hash: "h5", Size: 50, ModTime: 500, Status: model.StatusNew},
		"deleted1": {Hash: "h6", Size: 60, ModTime: 600, Status: model.StatusDeletedInSource},
		"deleted2": {Hash: "h7", Size: 70, ModTime: 700, Status: model.StatusDeletedInSource},
		"temp1":    {Hash: "h8", Size: 80, ModTime: 800, Status: model.StatusTempDeleted},
		"error1":   {Hash: "h9", Size: 90, ModTime: 900, Status: model.StatusError},
	}
	require.NoError(t, c.BatchSet(entries))

	r := NewRunner(c, nil, nil, nil, false)
	ctx := context.Background()

	stats, err := r.prepareForScan(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, int64(9), stats.TotalProcessed)
	require.Equal(t, int64(3), stats.SyncedToTempDeleted)
	require.Equal(t, int64(2), stats.NewRemained)
	require.Equal(t, int64(2), stats.DeletedOnS3Remained)
	require.Equal(t, int64(1), stats.TempDeletedRemained)
	require.Equal(t, int64(1), stats.ErrorRemained)

	// Verify SYNCED files became TEMP_DELETED
	for key, original := range entries {
		meta, err := c.Get(key)
		require.NoError(t, err)

		if original.Status == model.StatusSynced {
			require.Equal(t, model.StatusTempDeleted, meta.Status)
		} else {
			require.Equal(t, original.Status, meta.Status)
		}
	}
}

func TestFinalizeStatesAfterScan_EmptyCache(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	r := NewRunner(c, nil, nil, nil, false)
	ctx := context.Background()

	stats, err := r.finalizeStatesAfterScan(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, int64(0), stats.TotalProcessed)
	require.Equal(t, int64(0), stats.TempDeletedToDeletedS3)
	require.Equal(t, int64(0), stats.SyncedRemained)
	require.Equal(t, int64(0), stats.NewRemained)
}

func TestFinalizeStatesAfterScan_OnlyTempDeleted(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	// Add 10 TEMP_DELETED files
	entries := make(map[string]model.FileMeta)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("file%d", i)
		entries[key] = model.FileMeta{
			Hash:    fmt.Sprintf("hash%d", i),
			Size:    int64(i * 100),
			ModTime: int64(i * 1000),
			Status:  model.StatusTempDeleted,
		}
	}
	require.NoError(t, c.BatchSet(entries))

	r := NewRunner(c, nil, nil, nil, false)
	ctx := context.Background()

	stats, err := r.finalizeStatesAfterScan(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, int64(10), stats.TotalProcessed)
	require.Equal(t, int64(10), stats.TempDeletedToDeletedS3)
	require.Equal(t, int64(0), stats.SyncedRemained)
	require.Equal(t, int64(0), stats.NewRemained)

	// Verify that all files are now DELETED_ON_S3
	for key := range entries {
		meta, err := c.Get(key)
		require.NoError(t, err)
		require.Equal(t, model.StatusDeletedInSource, meta.Status)
	}
}

func TestFinalizeStatesAfterScan_MixedStatuses(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	entries := map[string]model.FileMeta{
		"temp1":    {Hash: "h1", Size: 10, ModTime: 100, Status: model.StatusTempDeleted},
		"temp2":    {Hash: "h2", Size: 20, ModTime: 200, Status: model.StatusTempDeleted},
		"temp3":    {Hash: "h3", Size: 30, ModTime: 300, Status: model.StatusTempDeleted},
		"synced1":  {Hash: "h4", Size: 40, ModTime: 400, Status: model.StatusSynced},
		"synced2":  {Hash: "h5", Size: 50, ModTime: 500, Status: model.StatusSynced},
		"new1":     {Hash: "h6", Size: 60, ModTime: 600, Status: model.StatusNew},
		"new2":     {Hash: "h7", Size: 70, ModTime: 700, Status: model.StatusNew},
		"deleted1": {Hash: "h8", Size: 80, ModTime: 800, Status: model.StatusDeletedInSource},
		"error1":   {Hash: "h9", Size: 90, ModTime: 900, Status: model.StatusError},
	}
	require.NoError(t, c.BatchSet(entries))

	r := NewRunner(c, nil, nil, nil, false)
	ctx := context.Background()

	stats, err := r.finalizeStatesAfterScan(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, int64(9), stats.TotalProcessed)
	require.Equal(t, int64(3), stats.TempDeletedToDeletedS3)
	require.Equal(t, int64(2), stats.SyncedRemained)
	require.Equal(t, int64(2), stats.NewRemained)
	require.Equal(t, int64(1), stats.DeletedOnS3Remained)
	require.Equal(t, int64(1), stats.ErrorRemained)

	// Verify TEMP_DELETED files became DELETED_ON_S3
	for key, original := range entries {
		meta, err := c.Get(key)
		require.NoError(t, err)

		if original.Status == model.StatusTempDeleted {
			require.Equal(t, model.StatusDeletedInSource, meta.Status, "File %s should be DELETED_ON_S3", key)
		} else {
			require.Equal(t, original.Status, meta.Status, "File %s should remain unchanged", key)
		}
	}
}

func TestFinalizeStatesAfterScan_NoTempDeleted(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	// Add files with various statuses but NO TEMP_DELETED
	entries := map[string]model.FileMeta{
		"synced1":  {Hash: "h1", Size: 10, ModTime: 100, Status: model.StatusSynced},
		"new1":     {Hash: "h2", Size: 20, ModTime: 200, Status: model.StatusNew},
		"deleted1": {Hash: "h3", Size: 30, ModTime: 300, Status: model.StatusDeletedInSource},
		"error1":   {Hash: "h4", Size: 40, ModTime: 400, Status: model.StatusError},
	}
	require.NoError(t, c.BatchSet(entries))

	r := NewRunner(c, nil, nil, nil, false)
	ctx := context.Background()

	stats, err := r.finalizeStatesAfterScan(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, int64(4), stats.TotalProcessed)
	require.Equal(t, int64(0), stats.TempDeletedToDeletedS3, "No TEMP_DELETED files should be converted")
	require.Equal(t, int64(1), stats.SyncedRemained)
	require.Equal(t, int64(1), stats.NewRemained)
	require.Equal(t, int64(1), stats.DeletedOnS3Remained)
	require.Equal(t, int64(1), stats.ErrorRemained)

	// Verify all statuses remain unchanged
	for key, original := range entries {
		meta, err := c.Get(key)
		require.NoError(t, err)
		require.Equal(t, original.Status, meta.Status)
	}
}

func TestFinalizeStatesAfterScan_LargeBatch(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	// Add 1000 TEMP_DELETED files to test batch processing
	entries := make(map[string]model.FileMeta)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("file%04d", i)
		entries[key] = model.FileMeta{
			Hash:    fmt.Sprintf("hash%d", i),
			Size:    int64(i),
			ModTime: int64(i * 1000),
			Status:  model.StatusTempDeleted,
		}
	}
	require.NoError(t, c.BatchSet(entries))

	r := NewRunner(c, nil, nil, nil, false)
	ctx := context.Background()

	stats, err := r.finalizeStatesAfterScan(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, int64(1000), stats.TotalProcessed)
	require.Equal(t, int64(1000), stats.TempDeletedToDeletedS3)

	// Verify all files are now DELETED_ON_S3
	for key := range entries {
		meta, err := c.Get(key)
		require.NoError(t, err)
		require.Equal(t, model.StatusDeletedInSource, meta.Status)
	}
}

func TestSyncNewFiles_EmptyCache(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	r := NewRunner(c, nil, nil, nil, false)
	ctx := context.Background()

	stats, err := r.syncNewFiles(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, int64(0), stats.TotalScanned)
	require.Equal(t, int64(0), stats.NewFilesFound)
	require.Equal(t, int64(0), stats.WouldBeSynced)
	require.Equal(t, int64(0), stats.TotalSizeBytes)
}

func TestSyncNewFiles_OnlyNewFiles(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	// Add 5 NEW files
	entries := make(map[string]model.FileMeta)
	totalSize := int64(0)
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("file%d", i)
		size := int64((i + 1) * 1000)
		entries[key] = model.FileMeta{
			Hash:    fmt.Sprintf("hash%d", i),
			Size:    size,
			ModTime: int64(i * 1000),
			Status:  model.StatusNew,
		}
		totalSize += size
	}
	require.NoError(t, c.BatchSet(entries))

	r := NewRunner(c, nil, nil, nil, false)
	ctx := context.Background()

	stats, err := r.syncNewFiles(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, int64(5), stats.TotalScanned)
	require.Equal(t, int64(5), stats.NewFilesFound)
	require.Equal(t, int64(5), stats.WouldBeSynced)
	require.Equal(t, totalSize, stats.TotalSizeBytes)
	require.Equal(t, int64(0), stats.OtherStatuses)
}

func TestSyncNewFiles_MixedStatuses(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	entries := map[string]model.FileMeta{
		"new1":     {Hash: "h1", Size: 1000, ModTime: 100, Status: model.StatusNew},
		"new2":     {Hash: "h2", Size: 2000, ModTime: 200, Status: model.StatusNew},
		"new3":     {Hash: "h3", Size: 3000, ModTime: 300, Status: model.StatusNew},
		"synced1":  {Hash: "h4", Size: 4000, ModTime: 400, Status: model.StatusSynced},
		"synced2":  {Hash: "h5", Size: 5000, ModTime: 500, Status: model.StatusSynced},
		"deleted1": {Hash: "h6", Size: 6000, ModTime: 600, Status: model.StatusDeletedInSource},
		"temp1":    {Hash: "h7", Size: 7000, ModTime: 700, Status: model.StatusTempDeleted},
		"error1":   {Hash: "h8", Size: 8000, ModTime: 800, Status: model.StatusError},
	}
	require.NoError(t, c.BatchSet(entries))

	r := NewRunner(c, nil, nil, nil, false)
	ctx := context.Background()

	stats, err := r.syncNewFiles(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, int64(8), stats.TotalScanned)
	require.Equal(t, int64(3), stats.NewFilesFound)
	require.Equal(t, int64(3), stats.WouldBeSynced)
	require.Equal(t, int64(6000), stats.TotalSizeBytes) // 1000 + 2000 + 3000
	require.Equal(t, int64(5), stats.OtherStatuses)
}

func TestSyncNewFiles_NoNewFiles(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	// Add files with various statuses but NO NEW
	entries := map[string]model.FileMeta{
		"synced1":  {Hash: "h1", Size: 1000, ModTime: 100, Status: model.StatusSynced},
		"deleted1": {Hash: "h2", Size: 2000, ModTime: 200, Status: model.StatusDeletedInSource},
		"temp1":    {Hash: "h3", Size: 3000, ModTime: 300, Status: model.StatusTempDeleted},
		"error1":   {Hash: "h4", Size: 4000, ModTime: 400, Status: model.StatusError},
	}
	require.NoError(t, c.BatchSet(entries))

	r := NewRunner(c, nil, nil, nil, false)
	ctx := context.Background()

	stats, err := r.syncNewFiles(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, int64(4), stats.TotalScanned)
	require.Equal(t, int64(0), stats.NewFilesFound, "No NEW files should be found")
	require.Equal(t, int64(0), stats.WouldBeSynced)
	require.Equal(t, int64(0), stats.TotalSizeBytes)
	require.Equal(t, int64(4), stats.OtherStatuses)
}

func TestSyncNewFiles_LargeBatch(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	// Add 100 NEW files to test batch processing
	entries := make(map[string]model.FileMeta)
	totalSize := int64(0)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("file%03d", i)
		size := int64((i + 1) * 100)
		entries[key] = model.FileMeta{
			Hash:    fmt.Sprintf("hash%d", i),
			Size:    size,
			ModTime: int64(i * 1000),
			Status:  model.StatusNew,
		}
		totalSize += size
	}
	require.NoError(t, c.BatchSet(entries))

	r := NewRunner(c, nil, nil, nil, false)
	ctx := context.Background()

	stats, err := r.syncNewFiles(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, int64(100), stats.TotalScanned)
	require.Equal(t, int64(100), stats.NewFilesFound)
	require.Equal(t, int64(100), stats.WouldBeSynced)
	require.Equal(t, totalSize, stats.TotalSizeBytes)
	require.Equal(t, int64(0), stats.OtherStatuses)
}

func TestSyncNewFiles_SizeCalculation(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	// Add NEW files with specific sizes to test size calculation
	entries := map[string]model.FileMeta{
		"small":  {Hash: "h1", Size: 100, ModTime: 100, Status: model.StatusNew},
		"medium": {Hash: "h2", Size: 1024 * 1024, ModTime: 200, Status: model.StatusNew},       // 1 MB
		"large":  {Hash: "h3", Size: 100 * 1024 * 1024, ModTime: 300, Status: model.StatusNew}, // 100 MB
	}
	require.NoError(t, c.BatchSet(entries))

	r := NewRunner(c, nil, nil, nil, false)
	ctx := context.Background()

	stats, err := r.syncNewFiles(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, int64(3), stats.NewFilesFound)

	expectedSize := int64(100 + 1024*1024 + 100*1024*1024)
	require.Equal(t, expectedSize, stats.TotalSizeBytes)

	// Verify String() method formats MB correctly
	output := stats.String()
	require.Contains(t, output, "NEW_files=3")
	require.Contains(t, output, "would_sync=3")
}

// Tests for deleteRemovedFiles functions

func TestDeleteRemovedFiles_EmptyCache(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	r := NewRunner(c, nil, nil, nil, false)
	ctx := context.Background()

	stats, err := r.deleteRemovedFiles(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, int64(0), stats.TotalScanned)
	require.Equal(t, int64(0), stats.DeletedOnS3Found)
	require.Equal(t, int64(0), stats.WouldBeDeleted)
}

func TestDeleteRemovedFiles_OnlyDeletedOnS3(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	// Add 5 DELETED_ON_S3 files
	entries := make(map[string]model.FileMeta)
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("file%d", i)
		entries[key] = model.FileMeta{
			Hash:    fmt.Sprintf("hash%d", i),
			Size:    int64(i * 100),
			ModTime: int64(i * 1000),
			Status:  model.StatusDeletedInSource,
		}
	}
	require.NoError(t, c.BatchSet(entries))

	r := NewRunner(c, nil, nil, nil, false)
	ctx := context.Background()

	stats, err := r.deleteRemovedFiles(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, int64(5), stats.TotalScanned)
	require.Equal(t, int64(5), stats.DeletedOnS3Found)
	require.Equal(t, int64(5), stats.WouldBeDeleted)
	require.Equal(t, int64(0), stats.OtherStatuses)
}

func TestDeleteRemovedFiles_MixedStatuses(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	entries := map[string]model.FileMeta{
		"deleted1": {Hash: "h1", Size: 1000, ModTime: 100, Status: model.StatusDeletedInSource},
		"deleted2": {Hash: "h2", Size: 2000, ModTime: 200, Status: model.StatusDeletedInSource},
		"deleted3": {Hash: "h3", Size: 3000, ModTime: 300, Status: model.StatusDeletedInSource},
		"synced1":  {Hash: "h4", Size: 4000, ModTime: 400, Status: model.StatusSynced},
		"synced2":  {Hash: "h5", Size: 5000, ModTime: 500, Status: model.StatusSynced},
		"new1":     {Hash: "h6", Size: 6000, ModTime: 600, Status: model.StatusNew},
		"temp1":    {Hash: "h7", Size: 7000, ModTime: 700, Status: model.StatusTempDeleted},
		"error1":   {Hash: "h8", Size: 8000, ModTime: 800, Status: model.StatusError},
	}
	require.NoError(t, c.BatchSet(entries))

	r := NewRunner(c, nil, nil, nil, false)
	ctx := context.Background()

	stats, err := r.deleteRemovedFiles(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, int64(8), stats.TotalScanned)
	require.Equal(t, int64(3), stats.DeletedOnS3Found)
	require.Equal(t, int64(3), stats.WouldBeDeleted)
	require.Equal(t, int64(5), stats.OtherStatuses)
}

func TestDeleteRemovedFiles_NoDeletedOnS3(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	// Add files with various statuses but NO DELETED_ON_S3
	entries := map[string]model.FileMeta{
		"synced1": {Hash: "h1", Size: 1000, ModTime: 100, Status: model.StatusSynced},
		"new1":    {Hash: "h2", Size: 2000, ModTime: 200, Status: model.StatusNew},
		"temp1":   {Hash: "h3", Size: 3000, ModTime: 300, Status: model.StatusTempDeleted},
		"error1":  {Hash: "h4", Size: 4000, ModTime: 400, Status: model.StatusError},
	}
	require.NoError(t, c.BatchSet(entries))

	r := NewRunner(c, nil, nil, nil, false)
	ctx := context.Background()

	stats, err := r.deleteRemovedFiles(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, int64(4), stats.TotalScanned)
	require.Equal(t, int64(0), stats.DeletedOnS3Found, "No DELETED_ON_S3 files should be found")
	require.Equal(t, int64(0), stats.WouldBeDeleted)
	require.Equal(t, int64(4), stats.OtherStatuses)

	// Verify all files still exist in cache (dry-run mode doesn't delete)
	for key := range entries {
		_, err := c.Get(key)
		require.NoError(t, err)
	}
}

func TestDeleteRemovedFiles_LargeBatch(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	// Add 100 DELETED_ON_S3 files to test batch processing
	entries := make(map[string]model.FileMeta)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("file%03d", i)
		entries[key] = model.FileMeta{
			Hash:    fmt.Sprintf("hash%d", i),
			Size:    int64(i * 100),
			ModTime: int64(i * 1000),
			Status:  model.StatusDeletedInSource,
		}
	}
	require.NoError(t, c.BatchSet(entries))

	r := NewRunner(c, nil, nil, nil, false)
	ctx := context.Background()

	stats, err := r.deleteRemovedFiles(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, int64(100), stats.TotalScanned)
	require.Equal(t, int64(100), stats.DeletedOnS3Found)
	require.Equal(t, int64(100), stats.WouldBeDeleted)
	require.Equal(t, int64(0), stats.OtherStatuses)
}

func TestDeleteRemovedFiles_StringFormatting(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	// Add DELETED_ON_S3 files
	entries := map[string]model.FileMeta{
		"deleted1": {Hash: "h1", Size: 1000, ModTime: 100, Status: model.StatusDeletedInSource},
		"deleted2": {Hash: "h2", Size: 2000, ModTime: 200, Status: model.StatusDeletedInSource},
		"synced1":  {Hash: "h3", Size: 3000, ModTime: 300, Status: model.StatusSynced},
	}
	require.NoError(t, c.BatchSet(entries))

	r := NewRunner(c, nil, nil, nil, false)
	ctx := context.Background()

	stats, err := r.deleteRemovedFiles(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)

	// Verify String() method formats correctly
	output := stats.String()
	require.Contains(t, output, "DELETED_ON_S3=2")
	require.Contains(t, output, "would_delete=2")
	require.Contains(t, output, "dry-run")
}

// Tests for processSourceFiles (unit tests with mock source)

type mockSourceProvider struct {
	files []model.RemoteFile
}

func (m *mockSourceProvider) ListObjectsFlat(ctx context.Context, prefix string) ([]model.RemoteFile, error) {
	return m.files, nil
}

func (m *mockSourceProvider) ListObjectsRecursiveStream(ctx context.Context) (<-chan model.RemoteFile, <-chan error) {
	filesCh := make(chan model.RemoteFile)
	errCh := make(chan error)
	go func() {
		defer close(filesCh)
		defer close(errCh)
		for _, f := range m.files {
			filesCh <- f
		}
	}()
	return filesCh, errCh
}

func (m *mockSourceProvider) ListObjectsByLevelStream(ctx context.Context, level int) (<-chan model.RemoteFile, <-chan error) {
	return m.ListObjectsRecursiveStream(ctx)
}

func (m *mockSourceProvider) ListObjectsStream(ctx context.Context) (<-chan model.RemoteFile, <-chan error) {
	return m.ListObjectsRecursiveStream(ctx)
}

func (m *mockSourceProvider) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	return nil, nil
}

func (m *mockSourceProvider) GetCurrentRPS() int64 {
	return 0
}

func TestProcessSourceFiles_NewFiles(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	// Create mock source with 3 new files
	mockSrc := &mockSourceProvider{
		files: []model.RemoteFile{
			{Key: "file1.txt", Hash: "hash1", Size: 100, ModTime: 1000},
			{Key: "file2.txt", Hash: "hash2", Size: 200, ModTime: 2000},
			{Key: "file3.txt", Hash: "hash3", Size: 300, ModTime: 3000},
		},
	}

	r := NewRunner(c, mockSrc, nil, nil, false)
	ctx := context.Background()

	stats, err := r.processSourceFiles(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, int64(3), stats.TotalProcessed)
	require.Equal(t, int64(3), stats.NewFiles)

	// Verify files are in cache with NEW status
	for _, f := range mockSrc.files {
		meta, err := c.Get(f.Key)
		require.NoError(t, err)
		require.Equal(t, model.StatusNew, meta.Status)
		require.Equal(t, f.Hash, meta.Hash)
		require.Equal(t, f.Size, meta.Size)
	}
}

func TestProcessSourceFiles_TempDeletedToSynced(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	// Pre-populate cache with TEMP_DELETED files
	entries := map[string]model.FileMeta{
		"file1.txt": {Hash: "hash1", Size: 100, ModTime: 1000, Status: model.StatusTempDeleted},
		"file2.txt": {Hash: "hash2", Size: 200, ModTime: 2000, Status: model.StatusTempDeleted},
	}
	require.NoError(t, c.BatchSet(entries))

	// Mock source returns same files with same hashes
	mockSrc := &mockSourceProvider{
		files: []model.RemoteFile{
			{Key: "file1.txt", Hash: "hash1", Size: 100, ModTime: 1000},
			{Key: "file2.txt", Hash: "hash2", Size: 200, ModTime: 2000},
		},
	}

	r := NewRunner(c, mockSrc, nil, nil, false)
	ctx := context.Background()

	stats, err := r.processSourceFiles(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, int64(2), stats.TotalProcessed)
	require.Equal(t, int64(2), stats.TempDeletedToSynced)
	require.Equal(t, int64(0), stats.NewFiles)

	// Verify files are now SYNCED
	for _, f := range mockSrc.files {
		meta, err := c.Get(f.Key)
		require.NoError(t, err)
		require.Equal(t, model.StatusSynced, meta.Status)
	}
}

func TestProcessSourceFiles_TempDeletedToNew(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	// Pre-populate cache with TEMP_DELETED files
	entries := map[string]model.FileMeta{
		"file1.txt": {Hash: "old_hash1", Size: 50, ModTime: 500, Status: model.StatusTempDeleted},
		"file2.txt": {Hash: "old_hash2", Size: 60, ModTime: 600, Status: model.StatusTempDeleted},
	}
	require.NoError(t, c.BatchSet(entries))

	// Mock source returns same files with DIFFERENT hashes (modified files)
	mockSrc := &mockSourceProvider{
		files: []model.RemoteFile{
			{Key: "file1.txt", Hash: "new_hash1", Size: 100, ModTime: 1000},
			{Key: "file2.txt", Hash: "new_hash2", Size: 200, ModTime: 2000},
		},
	}

	r := NewRunner(c, mockSrc, nil, nil, false)
	ctx := context.Background()

	stats, err := r.processSourceFiles(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, int64(2), stats.TotalProcessed)
	require.Equal(t, int64(2), stats.TempDeletedToNew)
	require.Equal(t, int64(0), stats.TempDeletedToSynced)

	// Verify files are now NEW with updated metadata
	for _, f := range mockSrc.files {
		meta, err := c.Get(f.Key)
		require.NoError(t, err)
		require.Equal(t, model.StatusNew, meta.Status)
		require.Equal(t, f.Hash, meta.Hash)
		require.Equal(t, f.Size, meta.Size)
		require.Equal(t, f.ModTime, meta.ModTime)
	}
}

func TestProcessSourceFiles_DeletedOnS3ToNew(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	// Pre-populate cache with DELETED_ON_S3 files
	entries := map[string]model.FileMeta{
		"file1.txt": {Hash: "hash1", Size: 100, ModTime: 1000, Status: model.StatusDeletedInSource},
	}
	require.NoError(t, c.BatchSet(entries))

	// Mock source returns the file again (file re-appeared in S3)
	mockSrc := &mockSourceProvider{
		files: []model.RemoteFile{
			{Key: "file1.txt", Hash: "new_hash", Size: 200, ModTime: 2000},
		},
	}

	r := NewRunner(c, mockSrc, nil, nil, false)
	ctx := context.Background()

	stats, err := r.processSourceFiles(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, int64(1), stats.TotalProcessed)
	require.Equal(t, int64(1), stats.DeletedOnS3ToNew)

	// Verify file is now NEW
	meta, err := c.Get("file1.txt")
	require.NoError(t, err)
	require.Equal(t, model.StatusNew, meta.Status)
	require.Equal(t, "new_hash", meta.Hash)
}

func TestProcessSourceFiles_EmptySource(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	// Mock source with no files
	mockSrc := &mockSourceProvider{
		files: []model.RemoteFile{},
	}

	r := NewRunner(c, mockSrc, nil, nil, false)
	ctx := context.Background()

	stats, err := r.processSourceFiles(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, int64(0), stats.TotalProcessed)
	require.Equal(t, int64(0), stats.NewFiles)
}

// Tests for syncNewFilesActual (actual upload mode)

type mockDestinationProvider struct {
	uploadedFiles   []string
	failOnUpload    map[string]error // Map of files that should fail to upload
	deletedFiles    []string
	failOnDelete    map[string]error // Map of files that should fail to delete
	workerCount     int
}

func (m *mockDestinationProvider) Upload(ctx context.Context, key string, reader io.Reader) error {
	if err, shouldFail := m.failOnUpload[key]; shouldFail {
		return err
	}
	m.uploadedFiles = append(m.uploadedFiles, key)
	// Read all content to simulate actual upload
	_, err := io.ReadAll(reader)
	return err
}

func (m *mockDestinationProvider) Delete(ctx context.Context, key string) error {
	if err, shouldFail := m.failOnDelete[key]; shouldFail {
		return err
	}
	m.deletedFiles = append(m.deletedFiles, key)
	return nil
}

func (m *mockDestinationProvider) FileExists(ctx context.Context, key string) (bool, error) {
	for _, f := range m.uploadedFiles {
		if f == key {
			return true, nil
		}
	}
	return false, nil
}

func (m *mockDestinationProvider) GetWorkerCount() int {
	if m.workerCount > 0 {
		return m.workerCount
	}
	return 5 // default
}

func (m *mockDestinationProvider) Close() error {
	return nil
}

type mockSourceWithContent struct {
	mockSourceProvider
	fileContents map[string]string
}

func (m *mockSourceWithContent) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	if content, ok := m.fileContents[key]; ok {
		return io.NopCloser(strings.NewReader(content)), nil
	}
	return nil, fmt.Errorf("file not found: %s", key)
}

func TestSyncNewFilesActual_Success(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	// Add NEW files to cache
	entries := map[string]model.FileMeta{
		"file1.txt": {Hash: "hash1", Size: 100, ModTime: 1000, Status: model.StatusNew},
		"file2.txt": {Hash: "hash2", Size: 200, ModTime: 2000, Status: model.StatusNew},
		"file3.txt": {Hash: "hash3", Size: 300, ModTime: 3000, Status: model.StatusNew},
	}
	require.NoError(t, c.BatchSet(entries))

	// Create mock source with content
	mockSrc := &mockSourceWithContent{
		mockSourceProvider: mockSourceProvider{
			files: []model.RemoteFile{},
		},
		fileContents: map[string]string{
			"file1.txt": "content1",
			"file2.txt": "content2",
			"file3.txt": "content3",
		},
	}

	// Create mock destination
	mockDest := &mockDestinationProvider{
		uploadedFiles: []string{},
		failOnUpload:  map[string]error{},
		workerCount:   2,
	}

	r := NewRunner(c, mockSrc, mockDest, nil, false)
	ctx := context.Background()

	stats, err := r.syncNewFiles(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, int64(3), stats.TotalScanned)
	require.Equal(t, int64(3), stats.NewFilesFound)
	require.Equal(t, int64(3), stats.SuccessfullySynced)
	require.Equal(t, int64(0), stats.FailedToSync)

	// Verify all files were uploaded
	require.Len(t, mockDest.uploadedFiles, 3)

	// Verify files are now SYNCED in cache
	for key := range entries {
		meta, err := c.Get(key)
		require.NoError(t, err)
		require.Equal(t, model.StatusSynced, meta.Status, "File %s should be SYNCED", key)
	}
}

func TestSyncNewFilesActual_PartialFailure(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	// Add NEW files to cache
	entries := map[string]model.FileMeta{
		"file1.txt": {Hash: "hash1", Size: 100, ModTime: 1000, Status: model.StatusNew},
		"file2.txt": {Hash: "hash2", Size: 200, ModTime: 2000, Status: model.StatusNew},
		"file3.txt": {Hash: "hash3", Size: 300, ModTime: 3000, Status: model.StatusNew},
	}
	require.NoError(t, c.BatchSet(entries))

	// Create mock source with content
	mockSrc := &mockSourceWithContent{
		mockSourceProvider: mockSourceProvider{
			files: []model.RemoteFile{},
		},
		fileContents: map[string]string{
			"file1.txt": "content1",
			"file2.txt": "content2",
			"file3.txt": "content3",
		},
	}

	// Create mock destination that fails on file2.txt
	mockDest := &mockDestinationProvider{
		uploadedFiles: []string{},
		failOnUpload: map[string]error{
			"file2.txt": fmt.Errorf("upload failed"),
		},
		workerCount: 2,
	}

	r := NewRunner(c, mockSrc, mockDest, nil, false)
	ctx := context.Background()

	stats, err := r.syncNewFiles(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, int64(3), stats.NewFilesFound)
	require.Equal(t, int64(2), stats.SuccessfullySynced)
	require.Equal(t, int64(1), stats.FailedToSync)

	// Verify only 2 files were uploaded
	require.Len(t, mockDest.uploadedFiles, 2)

	// Verify file statuses in cache
	meta1, err := c.Get("file1.txt")
	require.NoError(t, err)
	require.Equal(t, model.StatusSynced, meta1.Status)

	meta2, err := c.Get("file2.txt")
	require.NoError(t, err)
	require.Equal(t, model.StatusError, meta2.Status, "Failed file should be marked as ERROR")

	meta3, err := c.Get("file3.txt")
	require.NoError(t, err)
	require.Equal(t, model.StatusSynced, meta3.Status)
}

func TestSyncNewFilesActual_EmptyCache(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	mockSrc := &mockSourceWithContent{
		mockSourceProvider: mockSourceProvider{
			files: []model.RemoteFile{},
		},
		fileContents: map[string]string{},
	}

	mockDest := &mockDestinationProvider{
		uploadedFiles: []string{},
		failOnUpload:  map[string]error{},
		workerCount:   2,
	}

	r := NewRunner(c, mockSrc, mockDest, nil, false)
	ctx := context.Background()

	stats, err := r.syncNewFiles(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, int64(0), stats.NewFilesFound)
	require.Equal(t, int64(0), stats.SuccessfullySynced)
	require.Equal(t, int64(0), stats.FailedToSync)

	// Verify nothing was uploaded
	require.Len(t, mockDest.uploadedFiles, 0)
}

func TestSyncNewFilesActual_OnlyOtherStatuses(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	// Add files with non-NEW statuses
	entries := map[string]model.FileMeta{
		"synced1":  {Hash: "h1", Size: 100, ModTime: 1000, Status: model.StatusSynced},
		"deleted1": {Hash: "h2", Size: 200, ModTime: 2000, Status: model.StatusDeletedInSource},
	}
	require.NoError(t, c.BatchSet(entries))

	mockSrc := &mockSourceWithContent{
		mockSourceProvider: mockSourceProvider{
			files: []model.RemoteFile{},
		},
		fileContents: map[string]string{},
	}

	mockDest := &mockDestinationProvider{
		uploadedFiles: []string{},
		failOnUpload:  map[string]error{},
		workerCount:   2,
	}

	r := NewRunner(c, mockSrc, mockDest, nil, false)
	ctx := context.Background()

	stats, err := r.syncNewFiles(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, int64(2), stats.TotalScanned)
	require.Equal(t, int64(0), stats.NewFilesFound)
	require.Equal(t, int64(2), stats.OtherStatuses)

	// Verify nothing was uploaded
	require.Len(t, mockDest.uploadedFiles, 0)
}

// Tests for deleteRemovedFilesActual (actual deletion mode)

func TestDeleteRemovedFilesActual_Success(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	// Add DELETED_ON_S3 files to cache
	entries := map[string]model.FileMeta{
		"file1.txt": {Hash: "hash1", Size: 100, ModTime: 1000, Status: model.StatusDeletedInSource},
		"file2.txt": {Hash: "hash2", Size: 200, ModTime: 2000, Status: model.StatusDeletedInSource},
		"file3.txt": {Hash: "hash3", Size: 300, ModTime: 3000, Status: model.StatusDeletedInSource},
	}
	require.NoError(t, c.BatchSet(entries))

	mockDest := &mockDestinationProvider{
		deletedFiles: []string{},
		failOnDelete: map[string]error{},
	}

	r := NewRunner(c, nil, mockDest, nil, false)
	ctx := context.Background()

	stats, err := r.deleteRemovedFiles(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, int64(3), stats.TotalScanned)
	require.Equal(t, int64(3), stats.DeletedOnS3Found)
	require.Equal(t, int64(3), stats.SuccessfullyDeleted)
	require.Equal(t, int64(0), stats.FailedToDelete)

	// Verify all files were deleted from destination
	require.Len(t, mockDest.deletedFiles, 3)

	// Verify files were removed from cache
	for key := range entries {
		_, err := c.Get(key)
		require.Error(t, err, "File %s should be removed from cache", key)
	}
}

func TestDeleteRemovedFilesActual_PartialFailure(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	// Add DELETED_ON_S3 files to cache
	entries := map[string]model.FileMeta{
		"file1.txt": {Hash: "hash1", Size: 100, ModTime: 1000, Status: model.StatusDeletedInSource},
		"file2.txt": {Hash: "hash2", Size: 200, ModTime: 2000, Status: model.StatusDeletedInSource},
		"file3.txt": {Hash: "hash3", Size: 300, ModTime: 3000, Status: model.StatusDeletedInSource},
	}
	require.NoError(t, c.BatchSet(entries))

	// Mock destination that fails on file2.txt
	mockDest := &mockDestinationProvider{
		deletedFiles: []string{},
		failOnDelete: map[string]error{
			"file2.txt": fmt.Errorf("delete failed"),
		},
	}

	r := NewRunner(c, nil, mockDest, nil, false)
	ctx := context.Background()

	stats, err := r.deleteRemovedFiles(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, int64(3), stats.DeletedOnS3Found)
	require.Equal(t, int64(2), stats.SuccessfullyDeleted)
	require.Equal(t, int64(1), stats.FailedToDelete)

	// Verify only 2 files were deleted from destination
	require.Len(t, mockDest.deletedFiles, 2)

	// Verify only successful deletes were removed from cache
	_, err = c.Get("file1.txt")
	require.Error(t, err, "file1.txt should be removed from cache")

	_, err = c.Get("file2.txt")
	require.NoError(t, err, "file2.txt should remain in cache (deletion failed)")

	_, err = c.Get("file3.txt")
	require.Error(t, err, "file3.txt should be removed from cache")
}

func TestDeleteRemovedFilesActual_EmptyCache(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	mockDest := &mockDestinationProvider{
		deletedFiles: []string{},
		failOnDelete: map[string]error{},
	}

	r := NewRunner(c, nil, mockDest, nil, false)
	ctx := context.Background()

	stats, err := r.deleteRemovedFiles(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, int64(0), stats.DeletedOnS3Found)
	require.Equal(t, int64(0), stats.SuccessfullyDeleted)
	require.Equal(t, int64(0), stats.FailedToDelete)

	// Verify nothing was deleted
	require.Len(t, mockDest.deletedFiles, 0)
}

func TestDeleteRemovedFilesActual_OnlyOtherStatuses(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	// Add files with non-DELETED_ON_S3 statuses
	entries := map[string]model.FileMeta{
		"synced1": {Hash: "h1", Size: 100, ModTime: 1000, Status: model.StatusSynced},
		"new1":    {Hash: "h2", Size: 200, ModTime: 2000, Status: model.StatusNew},
	}
	require.NoError(t, c.BatchSet(entries))

	mockDest := &mockDestinationProvider{
		deletedFiles: []string{},
		failOnDelete: map[string]error{},
	}

	r := NewRunner(c, nil, mockDest, nil, false)
	ctx := context.Background()

	stats, err := r.deleteRemovedFiles(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, int64(2), stats.TotalScanned)
	require.Equal(t, int64(0), stats.DeletedOnS3Found)
	require.Equal(t, int64(2), stats.OtherStatuses)

	// Verify nothing was deleted
	require.Len(t, mockDest.deletedFiles, 0)

	// Verify files still exist in cache
	for key := range entries {
		_, err := c.Get(key)
		require.NoError(t, err, "File %s should remain in cache", key)
	}
}

// Tests for Run (full pipeline integration)

func TestRun_EmptySource(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	mockSrc := &mockSourceWithContent{
		mockSourceProvider: mockSourceProvider{
			files: []model.RemoteFile{},
		},
		fileContents: map[string]string{},
	}

	mockDest := &mockDestinationProvider{
		uploadedFiles: []string{},
		deletedFiles:  []string{},
		failOnUpload:  map[string]error{},
		failOnDelete:  map[string]error{},
		workerCount:   2,
	}

	r := NewRunner(c, mockSrc, mockDest, nil, false)
	ctx := context.Background()

	err := r.Run(ctx)
	require.NoError(t, err)

	// Verify no files were uploaded or deleted
	require.Len(t, mockDest.uploadedFiles, 0)
	require.Len(t, mockDest.deletedFiles, 0)
}

func TestRun_NewFilesFlow(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	// Mock source with 3 new files
	mockSrc := &mockSourceWithContent{
		mockSourceProvider: mockSourceProvider{
			files: []model.RemoteFile{
				{Key: "file1.txt", Hash: "hash1", Size: 100, ModTime: 1000},
				{Key: "file2.txt", Hash: "hash2", Size: 200, ModTime: 2000},
				{Key: "file3.txt", Hash: "hash3", Size: 300, ModTime: 3000},
			},
		},
		fileContents: map[string]string{
			"file1.txt": "content1",
			"file2.txt": "content2",
			"file3.txt": "content3",
		},
	}

	mockDest := &mockDestinationProvider{
		uploadedFiles: []string{},
		deletedFiles:  []string{},
		failOnUpload:  map[string]error{},
		failOnDelete:  map[string]error{},
		workerCount:   2,
	}

	r := NewRunner(c, mockSrc, mockDest, nil, false)
	ctx := context.Background()

	err := r.Run(ctx)
	require.NoError(t, err)

	// Verify all files were uploaded
	require.Len(t, mockDest.uploadedFiles, 3)

	// Verify files are SYNCED in cache
	for _, file := range mockSrc.files {
		meta, err := c.Get(file.Key)
		require.NoError(t, err)
		require.Equal(t, model.StatusSynced, meta.Status)
	}
}

func TestRun_DeletedFilesFlow(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	// Pre-populate cache with SYNCED files
	entries := map[string]model.FileMeta{
		"file1.txt": {Hash: "hash1", Size: 100, ModTime: 1000, Status: model.StatusSynced},
		"file2.txt": {Hash: "hash2", Size: 200, ModTime: 2000, Status: model.StatusSynced},
	}
	require.NoError(t, c.BatchSet(entries))

	// Mock source with NO files (simulating deletion from S3)
	mockSrc := &mockSourceWithContent{
		mockSourceProvider: mockSourceProvider{
			files: []model.RemoteFile{},
		},
		fileContents: map[string]string{},
	}

	mockDest := &mockDestinationProvider{
		uploadedFiles: []string{},
		deletedFiles:  []string{},
		failOnUpload:  map[string]error{},
		failOnDelete:  map[string]error{},
		workerCount:   2,
	}

	r := NewRunner(c, mockSrc, mockDest, nil, false)
	ctx := context.Background()

	err := r.Run(ctx)
	require.NoError(t, err)

	// Verify files were deleted from destination
	require.Len(t, mockDest.deletedFiles, 2)

	// Verify files were removed from cache
	_, err = c.Get("file1.txt")
	require.Error(t, err)
	_, err = c.Get("file2.txt")
	require.Error(t, err)
}

func TestRun_MixedFlow(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	// Pre-populate cache:
	// - file1.txt: SYNCED (will remain in source, stays SYNCED)
	// - file2.txt: SYNCED (will be removed from source, gets DELETED)
	// - file3.txt: doesn't exist (will be added to source, gets NEW then SYNCED)
	entries := map[string]model.FileMeta{
		"file1.txt": {Hash: "hash1", Size: 100, ModTime: 1000, Status: model.StatusSynced},
		"file2.txt": {Hash: "hash2", Size: 200, ModTime: 2000, Status: model.StatusSynced},
	}
	require.NoError(t, c.BatchSet(entries))

	// Mock source with file1 (unchanged) and file3 (new)
	mockSrc := &mockSourceWithContent{
		mockSourceProvider: mockSourceProvider{
			files: []model.RemoteFile{
				{Key: "file1.txt", Hash: "hash1", Size: 100, ModTime: 1000}, // unchanged
				{Key: "file3.txt", Hash: "hash3", Size: 300, ModTime: 3000}, // new
			},
		},
		fileContents: map[string]string{
			"file1.txt": "content1",
			"file3.txt": "content3",
		},
	}

	mockDest := &mockDestinationProvider{
		uploadedFiles: []string{},
		deletedFiles:  []string{},
		failOnUpload:  map[string]error{},
		failOnDelete:  map[string]error{},
		workerCount:   2,
	}

	r := NewRunner(c, mockSrc, mockDest, nil, false)
	ctx := context.Background()

	err := r.Run(ctx)
	require.NoError(t, err)

	// Verify file3 was uploaded (new file)
	require.Contains(t, mockDest.uploadedFiles, "file3.txt")

	// Verify file2 was deleted (removed from source)
	require.Contains(t, mockDest.deletedFiles, "file2.txt")

	// Verify cache states
	meta1, err := c.Get("file1.txt")
	require.NoError(t, err)
	require.Equal(t, model.StatusSynced, meta1.Status, "file1 should remain SYNCED")

	_, err = c.Get("file2.txt")
	require.Error(t, err, "file2 should be removed from cache")

	meta3, err := c.Get("file3.txt")
	require.NoError(t, err)
	require.Equal(t, model.StatusSynced, meta3.Status, "file3 should be SYNCED after upload")
}

func TestRun_DryRunMode(t *testing.T) {
	c, cleanup := newTestCache(t)
	defer cleanup()

	// Mock source with new files
	mockSrc := &mockSourceWithContent{
		mockSourceProvider: mockSourceProvider{
			files: []model.RemoteFile{
				{Key: "file1.txt", Hash: "hash1", Size: 100, ModTime: 1000},
			},
		},
		fileContents: map[string]string{
			"file1.txt": "content1",
		},
	}

	mockDest := &mockDestinationProvider{
		uploadedFiles: []string{},
		deletedFiles:  []string{},
		failOnUpload:  map[string]error{},
		failOnDelete:  map[string]error{},
		workerCount:   2,
	}

	// Create runner in dry-run mode
	r := NewRunner(c, mockSrc, mockDest, nil, true)
	ctx := context.Background()

	err := r.Run(ctx)
	require.NoError(t, err)

	// Verify nothing was actually uploaded in dry-run mode
	require.Len(t, mockDest.uploadedFiles, 0)

	// Verify file remains NEW in cache (not synced)
	meta, err := c.Get("file1.txt")
	require.NoError(t, err)
	require.Equal(t, model.StatusNew, meta.Status, "file should remain NEW in dry-run mode")
}

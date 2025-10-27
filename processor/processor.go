package processor

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/olegkotsar/yomins-sync/cache"
	"github.com/olegkotsar/yomins-sync/destination"
	"github.com/olegkotsar/yomins-sync/logger"
	"github.com/olegkotsar/yomins-sync/model"
	"github.com/olegkotsar/yomins-sync/source"
)

type Runner struct {
	cache       cache.CacheProvider
	source      source.SourceProvider
	destination destination.DestinationProvider
	logger      logger.Logger
	dryRun      bool
}

// NewRunner creates a new Runner with the provided dependencies
func NewRunner(cache cache.CacheProvider, source source.SourceProvider, destination destination.DestinationProvider, log logger.Logger, dryRun bool) *Runner {
	// Use NoOpLogger if none provided
	if log == nil {
		log = logger.NewNoOpLogger()
	}
	return &Runner{
		cache:       cache,
		source:      source,
		destination: destination,
		logger:      log,
		dryRun:      dryRun,
	}
}

// PrepareForScanStats contains statistics from the prepareForScan operation
type PrepareForScanStats struct {
	TotalProcessed      int64 // Total entries processed from cache
	SyncedToTempDeleted int64 // Number of SYNCED entries changed to TEMP_DELETED
	NewRemained         int64 // Number of NEW entries left unchanged
	DeletedOnS3Remained int64 // Number of DELETED_ON_S3 entries left unchanged
	TempDeletedRemained int64 // Number of TEMP_DELETED entries left unchanged (edge case)
	ErrorRemained       int64 // Number of ERROR entries left unchanged
}

func (s *PrepareForScanStats) String() string {
	return fmt.Sprintf("PrepareForScan: processed=%d, SYNCED→TEMP_DELETED=%d, NEW=%d, DELETED_ON_S3=%d, TEMP_DELETED=%d, ERROR=%d",
		s.TotalProcessed, s.SyncedToTempDeleted, s.NewRemained, s.DeletedOnS3Remained, s.TempDeletedRemained, s.ErrorRemained)
}

// ProcessSourceStats contains statistics from the processSourceFiles operation
type ProcessSourceStats struct {
	TotalProcessed      int64 // Total files processed from source
	NewFiles            int64 // Files not found in cache (newly discovered)
	TempDeletedToSynced int64 // TEMP_DELETED files with matching hash → SYNCED
	TempDeletedToNew    int64 // TEMP_DELETED files with different hash → NEW
	DeletedOnS3ToNew    int64 // DELETED_ON_S3 files that reappeared → NEW
	NewUpdated          int64 // Files that were already NEW (ETag updated)
}

func (s *ProcessSourceStats) String() string {
	return fmt.Sprintf("ProcessSource: total=%d, new=%d, TEMP_DELETED→SYNCED=%d, TEMP_DELETED→NEW=%d, DELETED_ON_S3→NEW=%d, NEW(updated)=%d",
		s.TotalProcessed, s.NewFiles, s.TempDeletedToSynced, s.TempDeletedToNew, s.DeletedOnS3ToNew, s.NewUpdated)
}

// FinalizeStatesStats contains statistics from the finalizeStatesAfterScan operation
type FinalizeStatesStats struct {
	TotalProcessed         int64 // Total entries processed from cache
	TempDeletedToDeletedS3 int64 // Number of TEMP_DELETED entries changed to DELETED_ON_S3
	SyncedRemained         int64 // Number of SYNCED entries left unchanged
	NewRemained            int64 // Number of NEW entries left unchanged
	DeletedOnS3Remained    int64 // Number of DELETED_ON_S3 entries left unchanged
	ErrorRemained          int64 // Number of ERROR entries left unchanged
}

func (s *FinalizeStatesStats) String() string {
	return fmt.Sprintf("FinalizeStates: processed=%d, TEMP_DELETED→DELETED_ON_S3=%d, SYNCED=%d, NEW=%d, DELETED_ON_S3=%d, ERROR=%d",
		s.TotalProcessed, s.TempDeletedToDeletedS3, s.SyncedRemained, s.NewRemained, s.DeletedOnS3Remained, s.ErrorRemained)
}

// SyncNewFilesStats contains statistics from the syncNewFiles operation
type SyncNewFilesStats struct {
	TotalScanned       int64 // Total entries scanned in cache
	NewFilesFound      int64 // Number of NEW files found
	TotalSizeBytes     int64 // Total size of NEW files in bytes
	SuccessfullySynced int64 // Files successfully uploaded (actual mode)
	FailedToSync       int64 // Files that failed to upload (actual mode)
	WouldBeSynced      int64 // Number of files that would be synced (dry-run mode)
	OtherStatuses      int64 // Files with other statuses (SYNCED, DELETED_ON_S3, etc.)
}

func (s *SyncNewFilesStats) String() string {
	sizeMB := float64(s.TotalSizeBytes) / (1024 * 1024)
	if s.SuccessfullySynced > 0 || s.FailedToSync > 0 {
		return fmt.Sprintf("SyncNewFiles: scanned=%d, NEW_files=%d, synced=%d, failed=%d, total_size=%d bytes (%.2f MB)",
			s.TotalScanned, s.NewFilesFound, s.SuccessfullySynced, s.FailedToSync, s.TotalSizeBytes, sizeMB)
	}
	return fmt.Sprintf("SyncNewFiles (dry-run): scanned=%d, NEW_files=%d, would_sync=%d, total_size=%d bytes (%.2f MB)",
		s.TotalScanned, s.NewFilesFound, s.WouldBeSynced, s.TotalSizeBytes, sizeMB)
}

// DeleteRemovedFilesStats contains statistics from the deleteRemovedFiles operation
type DeleteRemovedFilesStats struct {
	TotalScanned        int64 // Total entries scanned in cache
	DeletedOnS3Found    int64 // Number of DELETED_ON_S3 files found
	SuccessfullyDeleted int64 // Files successfully deleted from destination (actual mode)
	FailedToDelete      int64 // Files that failed to delete (actual mode)
	WouldBeDeleted      int64 // Number of files that would be deleted (dry-run mode)
	OtherStatuses       int64 // Files with other statuses
}

func (s *DeleteRemovedFilesStats) String() string {
	if s.SuccessfullyDeleted > 0 || s.FailedToDelete > 0 {
		return fmt.Sprintf("DeleteRemovedFiles: scanned=%d, DELETED_ON_S3=%d, deleted=%d, failed=%d",
			s.TotalScanned, s.DeletedOnS3Found, s.SuccessfullyDeleted, s.FailedToDelete)
	}
	return fmt.Sprintf("DeleteRemovedFiles (dry-run): scanned=%d, DELETED_ON_S3=%d, would_delete=%d",
		s.TotalScanned, s.DeletedOnS3Found, s.WouldBeDeleted)
}

func (r *Runner) Run(ctx context.Context) error {
	r.logger.Info("Starting synchronization run")

	// 1. Prepare cache for scan
	r.logger.Debug("Step 1: Preparing cache for scan")
	prepareStats, err := r.prepareForScan(ctx)
	if err != nil {
		r.logger.Error("Failed to prepare cache for scan: %v", err)
		return err
	}
	r.logger.Info(prepareStats.String())

	// 2. Process source files
	r.logger.Debug("Step 2: Processing source files")
	processStats, err := r.processSourceFiles(ctx)
	if err != nil {
		r.logger.Error("Failed to process source files: %v", err)
		return err
	}
	r.logger.Info(processStats.String())

	// 3. Finalize states
	r.logger.Debug("Step 3: Finalizing states after scan")
	finalizeStats, err := r.finalizeStatesAfterScan(ctx)
	if err != nil {
		r.logger.Error("Failed to finalize states: %v", err)
		return err
	}
	r.logger.Info(finalizeStats.String())

	// 4. Sync NEW files
	if r.dryRun || r.destination == nil {
		r.logger.Debug("Step 4: Syncing NEW files to destination (dry-run mode)")
	} else {
		r.logger.Debug("Step 4: Syncing NEW files to destination")
	}
	syncStats, err := r.syncNewFiles(ctx)
	if err != nil {
		r.logger.Error("Failed to sync new files: %v", err)
		return err
	}
	r.logger.Info(syncStats.String())

	// 5. Delete DELETED_ON_S3 files
	if r.dryRun || r.destination == nil {
		r.logger.Debug("Step 5: Deleting removed files from destination (dry-run mode)")
	} else {
		r.logger.Debug("Step 5: Deleting removed files from destination")
	}
	deleteStats, err := r.deleteRemovedFiles(ctx)
	if err != nil {
		r.logger.Error("Failed to delete removed files: %v", err)
		return err
	}
	r.logger.Info(deleteStats.String())

	r.logger.Info("Synchronization run completed successfully")
	return nil
}

// ================== CACHE & STATE MANAGEMENT ==================

// prepareForScan sets all SYNCED files to TEMP_DELETED and keeps others as is
func (r *Runner) prepareForScan(ctx context.Context) (*PrepareForScanStats, error) {
	stats := &PrepareForScanStats{}
	batchSize := 10000
	batchCh, errCh := r.cache.IterateBatches(ctx, batchSize)

	// Collect all updates first to avoid deadlock with IterateBatches
	// (IterateBatches holds a read lock, BatchSet needs a write lock)
	allUpdates := make(map[string]model.FileMeta)

	for {
		select {
		case <-ctx.Done():
			return stats, ctx.Err()
		case err, ok := <-errCh:
			if ok && err != nil {
				return stats, err
			}
			// When errCh is closed without error, continue to drain batchCh
		case batch, ok := <-batchCh:
			if !ok {
				// All batches received, now apply updates
				if len(allUpdates) > 0 {
					r.logger.Debug("Applying %d updates to cache...", len(allUpdates))
					if err := r.cache.BatchSet(allUpdates); err != nil {
						return stats, err
					}
				}
				return stats, nil
			}

			// Collect updates from this batch
			for key, meta := range batch {
				stats.TotalProcessed++

				switch meta.Status {
				case model.StatusSynced:
					// Change SYNCED to TEMP_DELETED
					meta.Status = model.StatusTempDeleted
					allUpdates[key] = meta
					stats.SyncedToTempDeleted++
				case model.StatusNew:
					stats.NewRemained++
				case model.StatusDeletedInSource:
					stats.DeletedOnS3Remained++
				case model.StatusTempDeleted:
					stats.TempDeletedRemained++
				case model.StatusError:
					stats.ErrorRemained++
				}
			}
		}
	}
}

// finalizeStatesAfterScan marks TEMP_DELETED as DELETED_ON_S3 if they were not seen in the source
func (r *Runner) finalizeStatesAfterScan(ctx context.Context) (*FinalizeStatesStats, error) {
	stats := &FinalizeStatesStats{}
	batchSize := 10000
	batchCh, errCh := r.cache.IterateBatches(ctx, batchSize)

	// Collect all updates first to avoid deadlock with IterateBatches
	// (IterateBatches holds a read lock, BatchSet needs a write lock)
	allUpdates := make(map[string]model.FileMeta)

	for {
		select {
		case <-ctx.Done():
			return stats, ctx.Err()
		case err, ok := <-errCh:
			if ok && err != nil {
				return stats, err
			}
			// When errCh is closed without error, continue to drain batchCh
		case batch, ok := <-batchCh:
			if !ok {
				// All batches received, now apply updates
				if len(allUpdates) > 0 {
					r.logger.Debug("Applying %d updates to cache...", len(allUpdates))
					if err := r.cache.BatchSet(allUpdates); err != nil {
						return stats, err
					}
				}
				return stats, nil
			}

			// Collect updates from this batch
			for key, meta := range batch {
				stats.TotalProcessed++

				switch meta.Status {
				case model.StatusTempDeleted:
					// Change TEMP_DELETED to DELETED_ON_S3
					// (these files were not found during source scan)
					meta.Status = model.StatusDeletedInSource
					allUpdates[key] = meta
					stats.TempDeletedToDeletedS3++
				case model.StatusSynced:
					stats.SyncedRemained++
				case model.StatusNew:
					stats.NewRemained++
				case model.StatusDeletedInSource:
					stats.DeletedOnS3Remained++
				case model.StatusError:
					stats.ErrorRemained++
				}
			}
		}
	}
}

// ================== PROCESS SOURCE FILES ==================

// ProcessSourceFiles streams files from source and updates cache states
func (r *Runner) processSourceFiles(ctx context.Context) (*ProcessSourceStats, error) {
	stats := &ProcessSourceStats{}
	filesCh, errCh := r.source.ListObjectsStream(ctx)

	batchSize := 1000
	updates := make(map[string]model.FileMeta, batchSize)

	// Start periodic RPS logging
	rpsTicker := time.NewTicker(1 * time.Second)
	defer rpsTicker.Stop()

	rpsCtx, rpsCancel := context.WithCancel(ctx)
	defer rpsCancel()

	go func() {
		for {
			select {
			case <-rpsCtx.Done():
				return
			case <-rpsTicker.C:
				rps := r.source.GetCurrentRPS()
				if rps > 0 {
					r.logger.Info("S3 listing: current RPS = %d req/s", rps)
				}
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return stats, ctx.Err()
		case err, ok := <-errCh:
			if ok {
				return stats, err
			}
		case file, ok := <-filesCh:
			if !ok {
				// Commit remaining updates
				if len(updates) > 0 {
					if err := r.cache.BatchSet(updates); err != nil {
						return stats, err
					}
				}
				return stats, nil
			}

			stats.TotalProcessed++

			meta, err := r.cache.Get(file.Key)
			if err != nil && !errors.Is(err, cache.ErrKeyNotFound) {
				return stats, err
			}

			if meta == nil {
				// NEW file (not in cache)
				stats.NewFiles++
				updates[file.Key] = model.FileMeta{
					Size:    file.Size,
					ModTime: file.ModTime,
					Hash:    file.Hash,
					Status:  model.StatusNew,
				}
			} else {
				switch meta.Status {
				case model.StatusTempDeleted:
					if meta.Hash == file.Hash {
						// Hash matches -> SYNCED (file has not changed)
						stats.TempDeletedToSynced++
						meta.Status = model.StatusSynced
					} else {
						// Hash is different -> NEW (file has changed)
						stats.TempDeletedToNew++
						meta.Status = model.StatusNew
						meta.Size = file.Size
						meta.ModTime = file.ModTime
						meta.Hash = file.Hash
					}
				case model.StatusNew:
					// Remains NEW (update metadata)
					stats.NewUpdated++
					meta.Size = file.Size
					meta.ModTime = file.ModTime
					meta.Hash = file.Hash
				case model.StatusDeletedInSource:
					// File returned -> NEW
					stats.DeletedOnS3ToNew++
					meta.Status = model.StatusNew
					meta.Size = file.Size
					meta.ModTime = file.ModTime
					meta.Hash = file.Hash
				default:
					// If an unknown status is encountered, safely reset to SYNCED
					meta.Status = model.StatusSynced
				}
				updates[file.Key] = *meta
			}

			if len(updates) >= batchSize {
				if err := r.cache.BatchSet(updates); err != nil {
					return stats, err
				}
				updates = make(map[string]model.FileMeta, batchSize)
			}
		}
	}
}

// ================== DESTINATION SYNC (FTP) ==================

// syncNewFiles uploads NEW files to destination and updates their status
// If dryRun is true or destination is nil, operates in dry-run mode (logs what would be synced)
// Otherwise, performs actual parallel upload
func (r *Runner) syncNewFiles(ctx context.Context) (*SyncNewFilesStats, error) {
	stats := &SyncNewFilesStats{}

	// If dry-run mode is enabled or no destination is configured, run in dry-run mode
	if r.dryRun || r.destination == nil {
		return r.syncNewFilesDryRun(ctx, stats)
	}

	// Actual upload mode with parallel workers
	return r.syncNewFilesActual(ctx, stats)
}

// syncNewFilesDryRun scans cache and logs what would be synced
func (r *Runner) syncNewFilesDryRun(ctx context.Context, stats *SyncNewFilesStats) (*SyncNewFilesStats, error) {
	batchSize := 10000
	batchCh, errCh := r.cache.IterateBatches(ctx, batchSize)

	// Collect files to be synced for logging
	var filesToSync []string
	const maxLoggedFiles = 10 // Limit number of files to log individually

	for {
		select {
		case <-ctx.Done():
			return stats, ctx.Err()
		case err, ok := <-errCh:
			if ok && err != nil {
				return stats, err
			}
		case batch, ok := <-batchCh:
			if !ok {
				// All batches processed successfully
				// Log summary of files that would be synced
				if stats.NewFilesFound > 0 {
					r.logger.Info("Dry-run mode: Would sync %d NEW files (total size: %d bytes)", stats.NewFilesFound, stats.TotalSizeBytes)

					if len(filesToSync) > 0 {
						r.logger.Debug("Sample files to be synced:")
						for i, file := range filesToSync {
							if i >= maxLoggedFiles {
								r.logger.Debug("... and %d more files", len(filesToSync)-maxLoggedFiles)
								break
							}
							r.logger.Debug("  - %s", file)
						}
					}
				} else {
					r.logger.Info("No NEW files to sync")
				}

				return stats, nil
			}

			for key, meta := range batch {
				stats.TotalScanned++

				if meta.Status == model.StatusNew {
					stats.NewFilesFound++
					stats.WouldBeSynced++
					stats.TotalSizeBytes += meta.Size

					// Collect files for logging (limit to avoid memory issues)
					if len(filesToSync) < maxLoggedFiles*2 {
						filesToSync = append(filesToSync, key)
					}

					// Log verbose details about each file
					r.logger.Verbose("Would sync: %s (size: %d bytes, hash: %s)", key, meta.Size, meta.Hash)
				} else {
					stats.OtherStatuses++
				}
			}
		}
	}
}

// syncNewFilesActual performs actual parallel upload of NEW files
func (r *Runner) syncNewFilesActual(ctx context.Context, stats *SyncNewFilesStats) (*SyncNewFilesStats, error) {
	// Step 1: Collect all NEW files from cache
	r.logger.Debug("Collecting NEW files from cache...")
	newFiles := make(map[string]model.FileMeta)
	batchSize := 10000
	batchCh, errCh := r.cache.IterateBatches(ctx, batchSize)

	for {
		select {
		case <-ctx.Done():
			return stats, ctx.Err()
		case err, ok := <-errCh:
			if ok && err != nil {
				return stats, err
			}
		case batch, ok := <-batchCh:
			if !ok {
				goto StartUpload
			}

			for key, meta := range batch {
				stats.TotalScanned++

				if meta.Status == model.StatusNew {
					stats.NewFilesFound++
					stats.TotalSizeBytes += meta.Size
					newFiles[key] = meta
				} else {
					stats.OtherStatuses++
				}
			}
		}
	}

StartUpload:
	// Step 2: If no NEW files, return early
	if len(newFiles) == 0 {
		r.logger.Info("No NEW files to sync")
		return stats, nil
	}

	r.logger.Info("Found %d NEW files to sync (total size: %.2f MB)",
		len(newFiles), float64(stats.TotalSizeBytes)/(1024*1024))

	// Step 3: Set up worker pool for parallel uploads
	workerCount := r.destination.GetWorkerCount()
	if workerCount <= 0 {
		workerCount = 5 // Fallback to default
	}

	type uploadJob struct {
		key  string
		meta model.FileMeta
	}

	type uploadResult struct {
		key     string
		success bool
		err     error
	}

	jobs := make(chan uploadJob, len(newFiles))
	results := make(chan uploadResult, len(newFiles))

	// Step 4: Start workers
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	r.logger.Debug("Starting %d upload workers...", workerCount)

	for w := 0; w < workerCount; w++ {
		go func(workerID int) {
			for job := range jobs {
				select {
				case <-workerCtx.Done():
					results <- uploadResult{key: job.key, success: false, err: workerCtx.Err()}
					return
				default:
					// Download from S3
					r.logger.Verbose("[Worker %d] Downloading %s from source...", workerID, job.key)
					reader, err := r.source.GetObject(workerCtx, job.key)
					if err != nil {
						r.logger.Error("[Worker %d] Failed to download %s: %v", workerID, job.key, err)
						results <- uploadResult{key: job.key, success: false, err: err}
						continue
					}

					// Upload to destination
					r.logger.Verbose("[Worker %d] Uploading %s to destination...", workerID, job.key)
					err = r.destination.Upload(workerCtx, job.key, reader)

					// Close the reader if it implements io.Closer
					if closer, ok := reader.(interface{ Close() error }); ok {
						closer.Close()
					}

					if err != nil {
						r.logger.Error("[Worker %d] Failed to upload %s: %v", workerID, job.key, err)
						results <- uploadResult{key: job.key, success: false, err: err}
						continue
					}

					r.logger.Debug("[Worker %d] Successfully uploaded %s", workerID, job.key)
					results <- uploadResult{key: job.key, success: true, err: nil}
				}
			}
		}(w)
	}

	// Step 5: Send jobs to workers
	r.logger.Debug("Sending %d files to upload queue...", len(newFiles))
	for key, meta := range newFiles {
		jobs <- uploadJob{key: key, meta: meta}
	}
	close(jobs)

	// Step 6: Collect results and update cache
	r.logger.Debug("Waiting for upload results...")
	updates := make(map[string]model.FileMeta)

	// Start periodic progress logging
	totalFiles := int64(len(newFiles))
	var processedFiles int64

	progressTicker := time.NewTicker(1 * time.Second)
	defer progressTicker.Stop()

	progressCtx, progressCancel := context.WithCancel(ctx)
	defer progressCancel()

	go func() {
		for {
			select {
			case <-progressCtx.Done():
				return
			case <-progressTicker.C:
				processed := atomic.LoadInt64(&processedFiles)
				if processed > 0 && processed < totalFiles {
					percentage := float64(processed) / float64(totalFiles) * 100
					r.logger.Info("Upload progress: %d/%d files (%.1f%%)", processed, totalFiles, percentage)
				}
			}
		}
	}()

	for i := 0; i < len(newFiles); i++ {
		select {
		case <-ctx.Done():
			cancel()
			return stats, ctx.Err()
		case result := <-results:
			meta := newFiles[result.key]

			if result.success {
				// Update status to SYNCED
				meta.Status = model.StatusSynced
				stats.SuccessfullySynced++
				r.logger.Verbose("Marked %s as SYNCED", result.key)
			} else {
				// Update status to ERROR
				meta.Status = model.StatusError
				stats.FailedToSync++
				r.logger.Warn("Marked %s as ERROR: %v", result.key, result.err)
			}

			updates[result.key] = meta

			// Increment processed files counter for progress tracking
			atomic.AddInt64(&processedFiles, 1)

			// Batch update cache every 100 files
			if len(updates) >= 100 {
				if err := r.cache.BatchSet(updates); err != nil {
					return stats, fmt.Errorf("failed to update cache: %w", err)
				}
				updates = make(map[string]model.FileMeta)
			}
		}
	}

	// Step 7: Final cache update for remaining files
	if len(updates) > 0 {
		if err := r.cache.BatchSet(updates); err != nil {
			return stats, fmt.Errorf("failed to update cache: %w", err)
		}
	}

	r.logger.Info("Upload completed: %d succeeded, %d failed", stats.SuccessfullySynced, stats.FailedToSync)
	return stats, nil
}

// deleteRemovedFiles removes DELETED_ON_S3 files from destination and removes them from cache
// If dryRun is true or destination is nil, operates in dry-run mode (logs what would be deleted)
// Otherwise, performs actual deletion
func (r *Runner) deleteRemovedFiles(ctx context.Context) (*DeleteRemovedFilesStats, error) {
	stats := &DeleteRemovedFilesStats{}

	// If dry-run mode is enabled or no destination is configured, run in dry-run mode
	if r.dryRun || r.destination == nil {
		return r.deleteRemovedFilesDryRun(ctx, stats)
	}

	// Actual delete mode
	return r.deleteRemovedFilesActual(ctx, stats)
}

// deleteRemovedFilesDryRun scans cache and logs what would be deleted
func (r *Runner) deleteRemovedFilesDryRun(ctx context.Context, stats *DeleteRemovedFilesStats) (*DeleteRemovedFilesStats, error) {
	batchSize := 10000
	batchCh, errCh := r.cache.IterateBatches(ctx, batchSize)

	// Collect files to be deleted for logging
	var filesToDelete []string
	const maxLoggedFiles = 10 // Limit number of files to log individually

	for {
		select {
		case <-ctx.Done():
			return stats, ctx.Err()
		case err, ok := <-errCh:
			if ok && err != nil {
				return stats, err
			}
		case batch, ok := <-batchCh:
			if !ok {
				// All batches processed successfully
				// Log summary of files that would be deleted
				if stats.DeletedOnS3Found > 0 {
					r.logger.Info("Dry-run mode: Would delete %d DELETED_ON_S3 files from destination", stats.DeletedOnS3Found)

					if len(filesToDelete) > 0 {
						r.logger.Debug("Sample files to be deleted:")
						for i, file := range filesToDelete {
							if i >= maxLoggedFiles {
								r.logger.Debug("... and %d more files", len(filesToDelete)-maxLoggedFiles)
								break
							}
							r.logger.Debug("  - %s", file)
						}
					}
				} else {
					r.logger.Info("No DELETED_ON_S3 files to delete")
				}

				return stats, nil
			}

			for key, meta := range batch {
				stats.TotalScanned++

				if meta.Status == model.StatusDeletedInSource {
					stats.DeletedOnS3Found++
					stats.WouldBeDeleted++

					// Collect files for logging (limit to avoid memory issues)
					if len(filesToDelete) < maxLoggedFiles*2 {
						filesToDelete = append(filesToDelete, key)
					}

					// Log verbose details about each file
					r.logger.Verbose("Would delete: %s", key)
				} else {
					stats.OtherStatuses++
				}
			}
		}
	}
}

// deleteRemovedFilesActual performs actual deletion of DELETED_ON_S3 files
func (r *Runner) deleteRemovedFilesActual(ctx context.Context, stats *DeleteRemovedFilesStats) (*DeleteRemovedFilesStats, error) {
	// Step 1: Collect all DELETED_ON_S3 files from cache
	r.logger.Debug("Collecting DELETED_ON_S3 files from cache...")
	deletedFiles := make(map[string]model.FileMeta)
	batchSize := 10000
	batchCh, errCh := r.cache.IterateBatches(ctx, batchSize)

	for {
		select {
		case <-ctx.Done():
			return stats, ctx.Err()
		case err, ok := <-errCh:
			if ok && err != nil {
				return stats, err
			}
		case batch, ok := <-batchCh:
			if !ok {
				goto StartDeletion
			}

			for key, meta := range batch {
				stats.TotalScanned++

				if meta.Status == model.StatusDeletedInSource {
					stats.DeletedOnS3Found++
					deletedFiles[key] = meta
				} else {
					stats.OtherStatuses++
				}
			}
		}
	}

StartDeletion:
	// Step 2: If no DELETED_ON_S3 files, return early
	if len(deletedFiles) == 0 {
		r.logger.Info("No DELETED_ON_S3 files to delete")
		return stats, nil
	}

	r.logger.Info("Found %d DELETED_ON_S3 files to delete from destination", len(deletedFiles))

	// Step 3: Delete files from destination and update cache
	keysToRemove := make([]string, 0, len(deletedFiles))

	for key := range deletedFiles {
		select {
		case <-ctx.Done():
			return stats, ctx.Err()
		default:
			// Delete from destination
			r.logger.Verbose("Deleting %s from destination...", key)
			err := r.destination.Delete(ctx, key)

			if err != nil {
				r.logger.Error("Failed to delete %s: %v", key, err)
				stats.FailedToDelete++
			} else {
				r.logger.Debug("Successfully deleted %s from destination", key)
				stats.SuccessfullyDeleted++
				// Mark for removal from cache
				keysToRemove = append(keysToRemove, key)
			}
		}
	}

	// Step 4: Remove successfully deleted files from cache
	if len(keysToRemove) > 0 {
		r.logger.Debug("Removing %d successfully deleted files from cache...", len(keysToRemove))
		for _, key := range keysToRemove {
			if err := r.cache.Delete(key); err != nil {
				r.logger.Warn("Failed to remove %s from cache: %v", key, err)
			}
		}
	}

	r.logger.Info("Deletion completed: %d succeeded, %d failed", stats.SuccessfullyDeleted, stats.FailedToDelete)
	return stats, nil
}

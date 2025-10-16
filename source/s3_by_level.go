package source

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/olegkotsar/yomins-sync/model"
)

// collectPrefixes collects prefixes up to the specified nesting level.
// level 0 means no limit (recursively includes all).
// level 1 means only root-level folders.
// level 2 means root-level folders and their subfolders, and so on.
func (c *S3Source) collectPrefixes(ctx context.Context, level int) ([]string, error) {
	if level == 0 {
		// If level = 0, return only the root prefix
		// and retrieve all files recursively without using a delimiter
		return []string{""}, nil
	}

	prefixes := []string{""}

	for currentLevel := 1; currentLevel <= level; currentLevel++ {
		var nextLevelPrefixes []string

		for _, prefix := range prefixes {
			subPrefixes, err := c.getDirectSubPrefixes(ctx, prefix)
			if err != nil {
				return nil, fmt.Errorf("failed to get sub-prefixes for %s: %v", prefix, err)
			}
			nextLevelPrefixes = append(nextLevelPrefixes, subPrefixes...)
		}

		if len(nextLevelPrefixes) == 0 {
			break // No more subfolders
		}

		prefixes = nextLevelPrefixes
	}

	return prefixes, nil
}

// getDirectSubPrefixes retrieves only direct subfolders for the given prefix
func (c *S3Source) getDirectSubPrefixes(ctx context.Context, prefix string) ([]string, error) {
	subPrefixes := []string{}

	input := &s3.ListObjectsV2Input{
		Bucket:    aws.String(c.config.Bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	}

	for {
		output, err := c.callWithRetry(ctx, func(ctx context.Context) (*s3.ListObjectsV2Output, error) {
			return c.client.ListObjectsV2(ctx, input)
		})
		if err != nil {
			return nil, err
		}

		// Collects only CommonPrefixes (subfolders)
		for _, cp := range output.CommonPrefixes {
			subPrefixes = append(subPrefixes, aws.ToString(cp.Prefix))
		}

		if output.IsTruncated != nil && aws.ToBool(output.IsTruncated) {
			input.ContinuationToken = output.NextContinuationToken
		} else {
			break
		}
	}

	return subPrefixes, nil
}

// ListObjectsByLevelStream - optimized version with preliminary prefix collection
func (c *S3Source) ListObjectsByLevelStream(ctx context.Context, level int) (<-chan model.RemoteFile, <-chan error) {
	filesCh := make(chan model.RemoteFile, c.common.MaxQueueSize)
	errCh := make(chan error, 10)

	go func() {
		defer close(filesCh)
		defer close(errCh)

		// Step 1: Collect prefixes up to the desired level
		prefixes, err := c.collectPrefixes(ctx, level)
		if err != nil {
			select {
			case errCh <- fmt.Errorf("failed to collect prefixes: %v", err):
			case <-ctx.Done():
			}
			return
		}

		// Step 2: Process prefixes in parallel to retrieve files
		prefixCh := make(chan string, len(prefixes))
		var wg sync.WaitGroup
		workerCount := c.common.WorkerCount

		// Start workers
		for i := 0; i < workerCount; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for prefix := range prefixCh {
					if err := c.listAllFilesInPrefix(ctx, prefix, filesCh); err != nil {
						select {
						case errCh <- err:
						case <-ctx.Done():
							return
						}
					}
				}
			}()
		}

		// Send prefixes to the queue
		for _, prefix := range prefixes {
			select {
			case prefixCh <- prefix:
			case <-ctx.Done():
				close(prefixCh)
				wg.Wait()
				return
			}
		}

		close(prefixCh)
		wg.Wait()
	}()

	return filesCh, errCh
}

// listAllFilesInPrefix retrieves ALL files in the prefix recursively WITHOUT using a delimiter
// This allows fetching the maximum number of files per request (up to 1000)
func (c *S3Source) listAllFilesInPrefix(ctx context.Context, prefix string, filesCh chan<- model.RemoteFile) error {
	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(c.config.Bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(1000), // Maximum number of items per request
		// DO NOT use Delimiter to retrieve all files recursively
	}

	for {
		output, err := c.callWithRetry(ctx, func(ctx context.Context) (*s3.ListObjectsV2Output, error) {
			return c.client.ListObjectsV2(ctx, input)
		})
		if err != nil {
			return fmt.Errorf("failed to list objects in prefix %s: %v", prefix, err)
		}

		// Process all found objects
		for _, obj := range output.Contents {
			key := aws.ToString(obj.Key)

			// Skip folders (keys ending with '/')
			if strings.HasSuffix(key, "/") {
				continue
			}

			select {
			case filesCh <- model.RemoteFile{
				Key:     key,
				Size:    aws.ToInt64(obj.Size),
				Hash:    aws.ToString(obj.ETag),
				ModTime: obj.LastModified.Unix(),
			}:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		// Check if there are more pages
		if output.IsTruncated != nil && aws.ToBool(output.IsTruncated) {
			input.ContinuationToken = output.NextContinuationToken
		} else {
			break
		}
	}

	return nil
}

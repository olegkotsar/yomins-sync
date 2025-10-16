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

func (c *S3Source) ListObjectsRecursiveStream(ctx context.Context) (<-chan model.RemoteFile, <-chan error) {
	filesCh := make(chan model.RemoteFile, c.common.MaxQueueSize)
	errCh := make(chan error, 10)
	prefixes := make(chan string, 100000)

	var wg sync.WaitGroup
	var active sync.WaitGroup

	workerCount := c.common.WorkerCount

	// Workers
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for prefix := range prefixes {
				if err := c.listPrefix(ctx, prefix, func(p string) {
					active.Add(1)
					select {
					case prefixes <- p:
					case <-ctx.Done():
						active.Done()
						return
					}
				}, filesCh); err != nil {
					select {
					case errCh <- err:
					case <-ctx.Done():
						return
					}
				}
				active.Done()
			}
		}()
	}

	// Starting
	active.Add(1)
	go func() {
		prefixes <- ""
		active.Wait() // waiting until all prefixes are processed
		close(prefixes)
		wg.Wait()
		close(filesCh)
		close(errCh)
	}()

	return filesCh, errCh
}

// listPrefix lists objects and sub-prefixes under a given prefix using Delimiter.
func (c *S3Source) listPrefix(ctx context.Context, prefix string, addPrefix func(string), filesCh chan<- model.RemoteFile) error {
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
			return fmt.Errorf("failed to list objects in %s: %v", prefix, err)
		}

		// Iterate over files (including zero-byte folder markers)
		for _, obj := range output.Contents {
			key := aws.ToString(obj.Key)

			// Skip folder marker objects (keys ending with '/')
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

		// Enqueue sub‑prefixes for further traversal
		for _, cp := range output.CommonPrefixes {
			addPrefix(aws.ToString(cp.Prefix))
		}

		if output.IsTruncated != nil && aws.ToBool(output.IsTruncated) {
			input.ContinuationToken = output.NextContinuationToken
		} else {
			break
		}
	}

	return nil
}

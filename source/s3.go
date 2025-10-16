package source

import (
	"context"
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/olegkotsar/yomins-sync/config"
	"github.com/olegkotsar/yomins-sync/model"
	"golang.org/x/time/rate"

	s3config "github.com/aws/aws-sdk-go-v2/config"
)

var _ SourceProvider = (*S3Source)(nil)

// I created an interface so the S3 client can be tested by providing a custom implementation.
type S3API interface {
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

type S3Source struct {
	client           S3API
	config           *config.S3Config
	common           *config.CommonSourceConfig
	limiter          *rate.Limiter
	requestCount     int64      // Total requests made
	lastRequestCount int64      // Request count at last RPS calculation
	lastRPS          int64      // Last calculated RPS
	lastRPSTime      time.Time  // Time of last RPS calculation
	mu               sync.Mutex // Protects RPS calculation fields
}

func NewS3Source(cfg *config.S3Config, common *config.CommonSourceConfig) (*S3Source, error) {
	ctx := context.TODO()

	// Apply defaults to common config
	common.ApplyDefaults()

	// default 0
	var limiter *rate.Limiter
	if common.MaxRPS > 0 {
		// Create rate limiter
		limiter = rate.NewLimiter(rate.Limit(common.MaxRPS), int(common.MaxRPS)) // burst = MaxRPS
	}

	// For S3-compatible storage, region is often just a placeholder
	// Use provided region or default to "us-east-1"
	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}

	s3cfg, err := s3config.LoadDefaultConfig(
		ctx,
		s3config.WithRegion(region),
		s3config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, "")),
		// Suppress AWS SDK logging warnings about missing checksums
		s3config.WithClientLogMode(0),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %v", err)
	}

	client := s3.NewFromConfig(s3cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.Endpoint)
		// Use path-style addressing for S3-compatible storage
		o.UsePathStyle = true
	})

	return &S3Source{
		client:      client,
		config:      cfg,
		common:      common,
		limiter:     limiter,
		lastRPSTime: time.Now(),
	}, nil
}

func (c *S3Source) ListRootPrefixes(ctx context.Context, prefix string) ([]string, error) {
	resp, err := c.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:    aws.String(c.config.Bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list objects: %v", err)
	}

	prefixes := []string{}
	for _, cp := range resp.CommonPrefixes {
		prefixes = append(prefixes, *cp.Prefix)
	}

	return prefixes, nil
}

func (c *S3Source) ListSecondLevelPrefixes(ctx context.Context, prefix string) ([]string, error) {
	firstLevelPrefixes, err := c.ListRootPrefixes(ctx, prefix)
	if err != nil {
		return nil, err
	}

	secondLevel := []string{}

	for _, p := range firstLevelPrefixes {
		resp, err := c.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:    aws.String(c.config.Bucket),
			Prefix:    aws.String(p),
			Delimiter: aws.String("/"),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to list sub-prefixes of %s: %v", prefix, err)
		}

		for _, cp := range resp.CommonPrefixes {
			secondLevel = append(secondLevel, *cp.Prefix)
		}
	}

	return secondLevel, nil
}

// ListObjectsFlat retrieves all objects under a prefix without using Delimiter.
func (c *S3Source) ListObjectsFlat(ctx context.Context, prefix string) ([]model.RemoteFile, error) {
	var (
		objects           []model.RemoteFile
		continuationToken *string
	)

	for {
		resp, err := c.callWithRetry(ctx, func(ctx context.Context) (*s3.ListObjectsV2Output, error) {
			return c.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
				Bucket:            aws.String(c.config.Bucket),
				Prefix:            aws.String(prefix),
				ContinuationToken: continuationToken,
			})
		})
		if err != nil {
			return nil, fmt.Errorf("failed to list objects in %s: %v", prefix, err)
		}

		for _, v := range resp.Contents {
			objects = append(objects, model.RemoteFile{
				Key:     *v.Key,
				Hash:    *v.ETag,
				Size:    *v.Size,
				ModTime: v.LastModified.Unix(),
			})
		}

		if resp.IsTruncated != nil && aws.ToBool(resp.IsTruncated) {
			continuationToken = resp.NextContinuationToken
		} else {
			break
		}
	}

	return objects, nil
}

// callWithRetry executes the provided function with retry logic, timeout, and RPS limiting.
func (c *S3Source) callWithRetry(ctx context.Context, fn func(context.Context) (*s3.ListObjectsV2Output, error)) (*s3.ListObjectsV2Output, error) {
	var lastErr error
	if c.common.MaxRetries == 0 {
		c.common.MaxRetries = 1
	}
	for i := 0; i < c.common.MaxRetries; i++ {
		// Rate limiting: wait for token before each attempt
		if c.limiter != nil {
			if err := c.limiter.Wait(ctx); err != nil {
				return nil, fmt.Errorf("rate limiter error: %w", err)
			}
		}
		atomic.AddInt64(&c.requestCount, 1)

		reqCtx, cancel := context.WithTimeout(ctx, time.Duration(c.common.TimeoutSeconds)*time.Second)
		defer cancel()

		resp, err := fn(reqCtx)
		if err == nil {
			return resp, nil
		}

		lastErr = err

		// Exponential backoff before next retry
		backoff := time.Duration(math.Pow(2, float64(i))) * 200 * time.Millisecond
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("all retries failed: %w", lastErr)
}

// ListObjectsStream selects the appropriate listing strategy based on configuration
func (c *S3Source) ListObjectsStream(ctx context.Context) (<-chan model.RemoteFile, <-chan error) {
	switch c.common.ListingStrategy {
	case config.ListingStrategyByLevel:
		return c.ListObjectsByLevelStream(ctx, c.common.ListingLevel)
	case config.ListingStrategyRecursive:
		fallthrough
	default:
		return c.ListObjectsRecursiveStream(ctx)
	}
}

// GetObject downloads a file from S3 and returns a reader
func (c *S3Source) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	// Apply rate limiting
	if c.limiter != nil {
		if err := c.limiter.Wait(ctx); err != nil {
			return nil, fmt.Errorf("rate limiter error: %w", err)
		}
	}
	atomic.AddInt64(&c.requestCount, 1)

	// Create context with timeout
	reqCtx, cancel := context.WithTimeout(ctx, time.Duration(c.common.TimeoutSeconds)*time.Second)
	// Note: We cannot defer cancel() here because the reader needs to stay open
	// The caller is responsible for closing the reader, which will release the context

	// Get the object from S3
	result, err := c.client.GetObject(reqCtx, &s3.GetObjectInput{
		Bucket: aws.String(c.config.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to get object %s: %w", key, err)
	}

	// Wrap the body with a custom closer that also cancels the context
	return &contextAwareReader{
		ReadCloser: result.Body,
		cancel:     cancel,
	}, nil
}

// contextAwareReader wraps an io.ReadCloser and cancels context on close
type contextAwareReader struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (r *contextAwareReader) Close() error {
	defer r.cancel()
	return r.ReadCloser.Close()
}

// GetCurrentRPS calculates and returns the current requests per second rate
// This method is thread-safe and can be called periodically for monitoring
func (c *S3Source) GetCurrentRPS() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(c.lastRPSTime).Seconds()

	// Only recalculate if at least 1 second has passed
	if elapsed >= 1.0 {
		currentCount := atomic.LoadInt64(&c.requestCount)
		requestsDelta := currentCount - c.lastRequestCount

		// Calculate RPS based on the delta and elapsed time
		c.lastRPS = int64(float64(requestsDelta) / elapsed)
		c.lastRequestCount = currentCount
		c.lastRPSTime = now
	}

	return c.lastRPS
}

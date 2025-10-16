package source

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/olegkotsar/yomins-sync/config"
	"github.com/olegkotsar/yomins-sync/model"
	"github.com/stretchr/testify/require"
)

func TestS3Source_ListObjectsFlat(t *testing.T) {
	cfg := getS3ConfigFromEnv()
	if cfg == nil {
		t.Skip("Skipping test because S3 environment variables are not set")
	}

	common := &config.CommonSourceConfig{}
	common.ApplyDefaults()

	source, err := NewS3Source(cfg, common)
	require.NoError(t, err)
	require.NotNil(t, source)

	ctx := context.Background()
	objects, err := source.ListObjectsFlat(ctx, "test1/test1_sub1/")
	require.NoError(t, err)
	require.NotEmpty(t, objects)

	// Check that the list contains the file test1_sub1.txt
	found := false
	for _, obj := range objects {
		if obj.Key == "test1/test1_sub1/test1_sub1.txt" {
			found = true
			break
		}
	}
	require.True(t, found, "expected file test1/test1_sub1/test1_sub1.txt not found")
}

func TestS3Source_ListObjectsFlat_Root(t *testing.T) {
	cfg := getS3ConfigFromEnv()
	if cfg == nil {
		t.Skip("Skipping test because S3 environment variables are not set")
	}

	common := &config.CommonSourceConfig{}
	common.ApplyDefaults()

	source, err := NewS3Source(cfg, common)
	require.NoError(t, err)
	require.NotNil(t, source)

	ctx := context.Background()
	objects, err := source.ListObjectsFlat(ctx, "")
	require.NoError(t, err)
	require.NotEmpty(t, objects)

	// Check that there is a test.txt file in the /
	found := false
	for _, obj := range objects {
		if obj.Key == "test.txt" {
			found = true
			break
		}
	}
	require.True(t, found, "expected file test.txt not found")
}

func TestListObjectsRecursiveStream(t *testing.T) {
	mockClient := &mockS3Client{
		responses: map[string]*s3.ListObjectsV2Output{
			"": {
				Contents: []types.Object{
					{
						Key:          aws.String("file1.txt"),
						Size:         aws.Int64(123),
						ETag:         aws.String("\"etag-file1\""),
						LastModified: aws.Time(time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC)),
					},
				},
				CommonPrefixes: []types.CommonPrefix{
					{Prefix: aws.String("dir1/")},
				},
			},
			"dir1/": {
				Contents: []types.Object{
					{
						Key:          aws.String("dir1/file2.txt"),
						Size:         aws.Int64(456),
						ETag:         aws.String("\"etag-file2\""),
						LastModified: aws.Time(time.Date(2023, 10, 2, 15, 30, 0, 0, time.UTC)),
					},
				},
			},
		},
	}

	src := &S3Source{
		client: mockClient,
		config: &config.S3Config{
			Bucket: "test-bucket",
		},
		common: &config.CommonSourceConfig{
			WorkerCount: 5,
		},
	}

	ctx := context.Background()
	filesCh, errCh := src.ListObjectsRecursiveStream(ctx)

	var results []model.RemoteFile
	var errs []error

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range errCh {
			errs = append(errs, err)
		}
	}()

	for file := range filesCh {
		results = append(results, file)
	}

	wg.Wait()

	require.Len(t, errs, 0, "must be error-free")
	require.Len(t, results, 2, "two files expected")
	require.Equal(t, "file1.txt", results[0].Key)
	require.Equal(t, "dir1/file2.txt", results[1].Key)
}

func TestListPrefix(t *testing.T) {
	mockClient := &mockS3Client{
		responses: map[string]*s3.ListObjectsV2Output{
			"dir2/": {
				Contents: []types.Object{
					{
						Key:          aws.String("dir2/fileA.txt"),
						Size:         aws.Int64(123),
						ETag:         aws.String("\"etag-file1\""),
						LastModified: aws.Time(time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC)),
					},
				},
				CommonPrefixes: []types.CommonPrefix{
					{Prefix: aws.String("dir2/subdir")},
				},
			},
		},
	}

	common := &config.CommonSourceConfig{}
	common.ApplyDefaults()
	src := &S3Source{
		client: mockClient,
		config: &config.S3Config{Bucket: "test-bucket"},
		common: common,
	}

	filesCh := make(chan model.RemoteFile, 10)
	prefixes := make(chan string, 10)

	err := src.listPrefix(context.Background(), "dir2/", func(p string) {
		prefixes <- p
	}, filesCh)
	require.NoError(t, err)

	close(filesCh)
	close(prefixes)

	var files []model.RemoteFile
	for f := range filesCh {
		files = append(files, f)
	}

	var subPrefixes []string
	for p := range prefixes {
		subPrefixes = append(subPrefixes, p)
	}

	require.Len(t, files, 1)
	require.Equal(t, "dir2/fileA.txt", files[0].Key)

	require.Len(t, subPrefixes, 1)
	require.Equal(t, "dir2/subdir", subPrefixes[0])
}

func TestListObjectsRecursiveStream_Integration(t *testing.T) {
	cfg := getS3ConfigFromEnv()
	if cfg == nil {
		t.Skip("Skipping test because S3 environment variables are not set")
	}

	common := &config.CommonSourceConfig{}

	s3src, err := NewS3Source(cfg, common)
	require.NoError(t, err)

	ctx := context.Background()
	filesCh, errCh := s3src.ListObjectsRecursiveStream(ctx)

	var files []string
	var count int64
	var errs []error

	done := make(chan struct{})
	go func() {
		for e := range errCh {
			errs = append(errs, e)
		}
		close(done)
	}()

	//for f := range filesCh {
	//	files = append(files, f.Key)
	//}
	go func() {
		for f := range filesCh {
			atomic.AddInt64(&count, 1)
			files = append(files, f.Key)
		}
	}()

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			fmt.Printf("[DEBUG] Current count: %d\n", atomic.LoadInt64(&count))
		}
	}()

	<-done

	require.Len(t, errs, 0, "There should be no errors")

	// Minimal check: check for key files
	require.Contains(t, files, "test.txt")
	require.Contains(t, files, "test1/test1.txt")
	require.Contains(t, files, "test1/test1_sub1/test1_sub1.txt")
	require.Contains(t, files, "test3/test3_sub3/test3_sub3.txt")
	require.NotContains(t, files, "test1/")
	require.NotContains(t, files, "test1/test1_sub1/")

	t.Logf("Found files: %d", len(files))
}

func TestListPrefix_Integration(t *testing.T) {
	cfg := getS3ConfigFromEnv()
	if cfg == nil {
		t.Skip("Skipping test because S3 environment variables are not set")
	}

	common := &config.CommonSourceConfig{}
	common.ApplyDefaults()

	s3src, err := NewS3Source(cfg, common)
	require.NoError(t, err)

	ctx := context.Background()
	filesCh := make(chan model.RemoteFile, 10)
	prefixes := make(chan string, 10)

	err = s3src.listPrefix(ctx, "test1/", func(p string) {
		prefixes <- p
	}, filesCh)

	require.NoError(t, err)
	close(filesCh)
	close(prefixes)

	var files []string
	var subPrefixes []string

	for f := range filesCh {
		files = append(files, f.Key)
	}
	for p := range prefixes {
		subPrefixes = append(subPrefixes, p)
	}

	require.Contains(t, files, "test1/test1.txt")
	require.Contains(t, subPrefixes, "test1/test1_sub1/")
	require.Contains(t, subPrefixes, "test1/test1_sub2/")
	require.Contains(t, subPrefixes, "test1/test1_sub3/")

	t.Logf("Files: %v", files)
	t.Logf("Prefixes: %v", subPrefixes)
}

func TestListObjectsRecursiveStream_Parallel(t *testing.T) {
	var active int32
	var maxActive int32

	hook := func() {
		current := atomic.AddInt32(&active, 1)
		if current > atomic.LoadInt32(&maxActive) {
			atomic.StoreInt32(&maxActive, current)
		}
		time.Sleep(200 * time.Millisecond) // increased the delay
		atomic.AddInt32(&active, -1)
	}

	mockClient := &mockS3Client{
		responses: map[string]*s3.ListObjectsV2Output{
			"": {
				CommonPrefixes: []types.CommonPrefix{
					{Prefix: aws.String("p1/")},
					{Prefix: aws.String("p2/")},
					{Prefix: aws.String("p3/")},
					{Prefix: aws.String("p4/")},
					{Prefix: aws.String("p5/")},
					{Prefix: aws.String("p6/")},
					{Prefix: aws.String("p7/")},
					{Prefix: aws.String("p8/")},
				},
			},
			"p1/": {Contents: []types.Object{{Key: aws.String("p1/file.txt"), LastModified: aws.Time(time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC))}}},
			"p2/": {Contents: []types.Object{{Key: aws.String("p2/file.txt"), LastModified: aws.Time(time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC))}}},
			"p3/": {Contents: []types.Object{{Key: aws.String("p3/file.txt"), LastModified: aws.Time(time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC))}}},
			"p4/": {Contents: []types.Object{{Key: aws.String("p4/file.txt"), LastModified: aws.Time(time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC))}}},
			"p5/": {Contents: []types.Object{{Key: aws.String("p5/file.txt"), LastModified: aws.Time(time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC))}}},
			"p6/": {Contents: []types.Object{{Key: aws.String("p6/file.txt"), LastModified: aws.Time(time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC))}}},
			"p7/": {Contents: []types.Object{{Key: aws.String("p7/file.txt"), LastModified: aws.Time(time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC))}}},
			"p8/": {Contents: []types.Object{{Key: aws.String("p8/file.txt"), LastModified: aws.Time(time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC))}}},
		},
		hook: hook,
	}

	src := &S3Source{
		client: mockClient,
		config: &config.S3Config{
			Bucket: "test-bucket",
		},
		common: &config.CommonSourceConfig{
			WorkerCount: 3,
		},
	}

	ctx := context.Background()
	start := time.Now()
	filesCh, errCh := src.ListObjectsRecursiveStream(ctx)

	var results []model.RemoteFile
	for f := range filesCh {
		results = append(results, f)
	}
	for range errCh {
	}

	duration := time.Since(start)

	require.Len(t, results, 8, "expected 8 files")
	require.Greater(t, maxActive, int32(1), "expected more than 1 concurrent worker")
	t.Logf("Max concurrent workers: %d", maxActive)
	t.Logf("Processing time: %v", duration)

	// Sequential: 8 × 200ms = 1600ms
	// Parallel (3 workers): ~700ms
	if duration > 1*time.Second {
		t.Errorf("expected faster execution with parallel workers, got %v", duration)
	}
}

// mockS3 simulates S3 API responses for testing retry logic.
type mockS3 struct {
	callCount int
	responses []mockResponse
}

type mockResponse struct {
	resp *s3.ListObjectsV2Output
	err  error
}

func (m *mockS3) ListObjectsV2(ctx context.Context, input *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if m.callCount >= len(m.responses) {
		return nil, errors.New("unexpected call")
	}
	r := m.responses[m.callCount]
	m.callCount++
	return r.resp, r.err
}

func (m *mockS3) GetObject(ctx context.Context, input *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return nil, errors.New("GetObject not implemented in mockS3")
}

func TestCallWithRetry(t *testing.T) {
	cfg := &config.S3Config{
		Bucket:          "test-bucket",
		Endpoint:        "http://localhost",
		AccessKeyID:     "AKIA...",
		SecretAccessKey: "SECRET",
	}
	common := &config.CommonSourceConfig{
		TimeoutSeconds: 1,
		MaxRetries:     3,
	}
	src := &S3Source{config: cfg, common: common}

	t.Run("success on first attempt", func(t *testing.T) {
		mock := &mockS3{
			responses: []mockResponse{
				{resp: &s3.ListObjectsV2Output{}, err: nil},
			},
		}
		src.client = mock

		ctx := context.Background()
		resp, err := src.callWithRetry(ctx, func(c context.Context) (*s3.ListObjectsV2Output, error) {
			return mock.ListObjectsV2(c, nil)
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, 1, mock.callCount)
	})

	t.Run("fail first, success second", func(t *testing.T) {
		mock := &mockS3{
			responses: []mockResponse{
				{resp: nil, err: errors.New("temporary error")},
				{resp: &s3.ListObjectsV2Output{}, err: nil},
			},
		}
		src.client = mock

		ctx := context.Background()
		resp, err := src.callWithRetry(ctx, func(c context.Context) (*s3.ListObjectsV2Output, error) {
			return mock.ListObjectsV2(c, nil)
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, 2, mock.callCount)
	})

	t.Run("all retries fail", func(t *testing.T) {
		mock := &mockS3{
			responses: []mockResponse{
				{resp: nil, err: errors.New("error 1")},
				{resp: nil, err: errors.New("error 2")},
				{resp: nil, err: errors.New("error 3")},
			},
		}
		src.client = mock

		ctx := context.Background()
		resp, err := src.callWithRetry(ctx, func(c context.Context) (*s3.ListObjectsV2Output, error) {
			return mock.ListObjectsV2(c, nil)
		})

		require.Error(t, err)
		require.Nil(t, resp)
		require.Equal(t, 3, mock.callCount)
		require.Contains(t, err.Error(), "all retries failed")
	})

	t.Run("context timeout", func(t *testing.T) {
		mock := &mockS3{
			responses: []mockResponse{
				{resp: nil, err: errors.New("timeout error")},
			},
		}
		src.client = mock

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		resp, err := src.callWithRetry(ctx, func(c context.Context) (*s3.ListObjectsV2Output, error) {
			time.Sleep(50 * time.Millisecond) // simulate delay > timeout
			return mock.ListObjectsV2(c, nil)
		})

		require.Error(t, err)
		require.Nil(t, resp)
		require.Contains(t, err.Error(), "context deadline exceeded")
	})
}

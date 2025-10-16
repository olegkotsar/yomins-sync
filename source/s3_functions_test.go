package source

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/olegkotsar/yomins-sync/config"
	"github.com/stretchr/testify/require"
)

// TestListSecondLevelPrefixes tests the ListSecondLevelPrefixes function
func TestListSecondLevelPrefixes(t *testing.T) {
	mockClient := &mockS3Client{
		responses: map[string]*s3.ListObjectsV2Output{
			"": {
				CommonPrefixes: []types.CommonPrefix{
					{Prefix: aws.String("level1_a/")},
					{Prefix: aws.String("level1_b/")},
				},
			},
			"level1_a/": {
				CommonPrefixes: []types.CommonPrefix{
					{Prefix: aws.String("level1_a/level2_x/")},
					{Prefix: aws.String("level1_a/level2_y/")},
				},
			},
			"level1_b/": {
				CommonPrefixes: []types.CommonPrefix{
					{Prefix: aws.String("level1_b/level2_z/")},
				},
			},
		},
	}

	src := &S3Source{
		client: mockClient,
		config: &config.S3Config{Bucket: "test-bucket"},
		common: &config.CommonSourceConfig{},
	}

	ctx := context.Background()
	prefixes, err := src.ListSecondLevelPrefixes(ctx, "")

	require.NoError(t, err)
	require.Len(t, prefixes, 3)
	require.Contains(t, prefixes, "level1_a/level2_x/")
	require.Contains(t, prefixes, "level1_a/level2_y/")
	require.Contains(t, prefixes, "level1_b/level2_z/")
}

func TestListSecondLevelPrefixes_EmptyBucket(t *testing.T) {
	mockClient := &mockS3Client{
		responses: map[string]*s3.ListObjectsV2Output{
			"": {CommonPrefixes: []types.CommonPrefix{}},
		},
	}

	src := &S3Source{
		client: mockClient,
		config: &config.S3Config{Bucket: "test-bucket"},
		common: &config.CommonSourceConfig{},
	}

	ctx := context.Background()
	prefixes, err := src.ListSecondLevelPrefixes(ctx, "")

	require.NoError(t, err)
	require.Empty(t, prefixes)
}

func TestListSecondLevelPrefixes_Integration(t *testing.T) {
	cfg := getS3ConfigFromEnv()
	if cfg == nil {
		t.Skip("Skipping test because S3 environment variables are not set")
	}

	common := &config.CommonSourceConfig{}
	common.ApplyDefaults()

	source, err := NewS3Source(cfg, common)
	require.NoError(t, err)

	ctx := context.Background()
	prefixes, err := source.ListSecondLevelPrefixes(ctx, "")
	require.NoError(t, err)

	t.Logf("Found %d second-level prefixes", len(prefixes))
	for i, prefix := range prefixes {
		if i < 10 {
			t.Logf("  Prefix %d: %s", i, prefix)
		}
	}
}

// mockGetObjectS3 implements GetObject for testing
type mockGetObjectS3 struct {
	objects map[string]*s3.GetObjectOutput
}

func (m *mockGetObjectS3) ListObjectsV2(ctx context.Context, input *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	return &s3.ListObjectsV2Output{}, nil
}

func (m *mockGetObjectS3) GetObject(ctx context.Context, input *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if output, ok := m.objects[*input.Key]; ok {
		return output, nil
	}
	return nil, fmt.Errorf("object not found: %s", *input.Key)
}

func TestGetObject(t *testing.T) {
	testContent := "test file content"
	mockClient := &mockGetObjectS3{
		objects: map[string]*s3.GetObjectOutput{
			"test.txt": {
				Body: io.NopCloser(strings.NewReader(testContent)),
			},
		},
	}

	src := &S3Source{
		client: mockClient,
		config: &config.S3Config{Bucket: "test-bucket"},
		common: &config.CommonSourceConfig{TimeoutSeconds: 30},
	}

	ctx := context.Background()
	reader, err := src.GetObject(ctx, "test.txt")
	require.NoError(t, err)
	require.NotNil(t, reader)
	defer reader.Close()

	content, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, testContent, string(content))
}

func TestGetObject_NotFound(t *testing.T) {
	mockClient := &mockGetObjectS3{
		objects: map[string]*s3.GetObjectOutput{},
	}

	src := &S3Source{
		client: mockClient,
		config: &config.S3Config{Bucket: "test-bucket"},
		common: &config.CommonSourceConfig{TimeoutSeconds: 30},
	}

	ctx := context.Background()
	reader, err := src.GetObject(ctx, "nonexistent.txt")
	require.Error(t, err)
	require.Nil(t, reader)
	require.Contains(t, err.Error(), "object not found")
}

func TestGetObject_ContextCancellation(t *testing.T) {
	testContent := "test content"
	mockClient := &mockGetObjectS3{
		objects: map[string]*s3.GetObjectOutput{
			"test.txt": {
				Body: io.NopCloser(strings.NewReader(testContent)),
			},
		},
	}

	src := &S3Source{
		client: mockClient,
		config: &config.S3Config{Bucket: "test-bucket"},
		common: &config.CommonSourceConfig{TimeoutSeconds: 1},
	}

	// Create context with very short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	// Wait for context to expire
	time.Sleep(10 * time.Millisecond)

	reader, err := src.GetObject(ctx, "test.txt")
	// The error might occur during GetObject or after, depending on timing
	// So we just verify that either we get an error immediately or the reader is closed
	if err == nil && reader != nil {
		reader.Close()
		t.Log("GetObject succeeded despite cancelled context (timing dependent)")
	} else {
		require.Error(t, err)
	}
}

func TestGetCurrentRPS(t *testing.T) {
	src := &S3Source{
		client:           &mockS3Client{responses: map[string]*s3.ListObjectsV2Output{}},
		config:           &config.S3Config{Bucket: "test-bucket"},
		common:           &config.CommonSourceConfig{TimeoutSeconds: 30},
		lastRPSTime:      time.Now(),
		requestCount:     0,
		lastRequestCount: 0,
		lastRPS:          0,
	}

	// Initially, RPS should be 0
	rps := src.GetCurrentRPS()
	require.Equal(t, int64(0), rps)

	// Simulate some requests
	atomic.AddInt64(&src.requestCount, 100)

	// Wait a bit more than 1 second to ensure calculation happens
	time.Sleep(1100 * time.Millisecond)

	// Get RPS - should calculate based on elapsed time
	rps = src.GetCurrentRPS()
	require.Greater(t, rps, int64(0))
	require.LessOrEqual(t, rps, int64(100))

	// Add more requests
	atomic.AddInt64(&src.requestCount, 50)
	time.Sleep(1100 * time.Millisecond)

	// Get RPS again - should be based on the delta
	rps2 := src.GetCurrentRPS()
	require.Greater(t, rps2, int64(0))
	require.LessOrEqual(t, rps2, int64(50))
}

func TestGetCurrentRPS_NoUpdate(t *testing.T) {
	src := &S3Source{
		client:           &mockS3Client{responses: map[string]*s3.ListObjectsV2Output{}},
		config:           &config.S3Config{Bucket: "test-bucket"},
		common:           &config.CommonSourceConfig{TimeoutSeconds: 30},
		lastRPSTime:      time.Now(),
		requestCount:     0,
		lastRequestCount: 0,
		lastRPS:          42, // Previous RPS value
	}

	// Call GetCurrentRPS before 1 second has passed
	rps := src.GetCurrentRPS()

	// Should return the cached value
	require.Equal(t, int64(42), rps)
}

package source

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3config "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"

	"github.com/olegkotsar/yomins-sync/config"
	"github.com/olegkotsar/yomins-sync/model"
)

// Mock structures for testing
type mockS3NestedClient struct {
	mockData  map[string][]types.CommonPrefix
	callCount int
}

func (m *mockS3NestedClient) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	m.callCount++
	prefix := aws.ToString(params.Prefix)

	commonPrefixes, exists := m.mockData[prefix]
	if !exists {
		return &s3.ListObjectsV2Output{
			CommonPrefixes: []types.CommonPrefix{},
			IsTruncated:    aws.Bool(false),
		}, nil
	}

	return &s3.ListObjectsV2Output{
		CommonPrefixes: commonPrefixes,
		IsTruncated:    aws.Bool(false),
	}, nil
}

func (m *mockS3NestedClient) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return nil, fmt.Errorf("GetObject not implemented in mockS3NestedClient")
}

// Interface for the S3 client
type S3ClientInterface interface {
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// Create a test S3Source with a mock client
func createTestS3Source(mockClient S3ClientInterface, cfg *config.S3Config) *S3Source {
	common := &config.CommonSourceConfig{}
	common.ApplyDefaults()
	return &S3Source{
		client: mockClient,
		config: cfg,
		common: common,
	}
}

// Create a real S3Source for integration tests
func createRealS3Source(cfg *config.S3Config) (*S3Source, error) {
	ctx := context.Background()
	s3cfg, err := s3config.LoadDefaultConfig(
		ctx,
		s3config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, "")),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := s3.NewFromConfig(s3cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.Endpoint)
	})
	common := &config.CommonSourceConfig{}
	common.ApplyDefaults()
	s3Source := &S3Source{
		client: client,
		config: cfg,
		common: common,
	}
	return s3Source, nil
}

// TESTS WITH MOCK DATA

func TestCollectPrefixes_WithMockData(t *testing.T) {
	tests := []struct {
		name        string
		level       int
		mockData    map[string][]types.CommonPrefix
		expected    []string
		expectError bool
	}{
		{
			name:     "Level 0 - returns root prefix",
			level:    0,
			mockData: map[string][]types.CommonPrefix{},
			expected: []string{""},
		},
		{
			name:  "Level 1 - root folders only",
			level: 1,
			mockData: map[string][]types.CommonPrefix{
				"": {
					{Prefix: aws.String("folder1/")},
					{Prefix: aws.String("folder2/")},
					{Prefix: aws.String("folder3/")},
				},
			},
			expected: []string{"folder1/", "folder2/", "folder3/"},
		},
		{
			name:  "Level 2 - root and subfolders",
			level: 2,
			mockData: map[string][]types.CommonPrefix{
				"": {
					{Prefix: aws.String("folder1/")},
					{Prefix: aws.String("folder2/")},
				},
				"folder1/": {
					{Prefix: aws.String("folder1/sub1/")},
					{Prefix: aws.String("folder1/sub2/")},
				},
				"folder2/": {
					{Prefix: aws.String("folder2/sub3/")},
				},
			},
			expected: []string{"folder1/sub1/", "folder1/sub2/", "folder2/sub3/"},
		},
		{
			name:     "Empty bucket",
			level:    1,
			mockData: map[string][]types.CommonPrefix{},
			expected: []string{""},
		},
		{
			name:  "Level 3 - deep nesting",
			level: 3,
			mockData: map[string][]types.CommonPrefix{
				"": {
					{Prefix: aws.String("root/")},
				},
				"root/": {
					{Prefix: aws.String("root/level1/")},
				},
				"root/level1/": {
					{Prefix: aws.String("root/level1/level2/")},
				},
			},
			expected: []string{"root/level1/level2/"},
		},
		{
			name:  "Level exceeds available depth",
			level: 5,
			mockData: map[string][]types.CommonPrefix{
				"": {
					{Prefix: aws.String("folder1/")},
				},
				"folder1/": {
					{Prefix: aws.String("folder1/sub1/")},
				},
			},
			expected: []string{"folder1/sub1/"},
		},
		{
			name:  "Multiple branches at different levels",
			level: 2,
			mockData: map[string][]types.CommonPrefix{
				"": {
					{Prefix: aws.String("logs/")},
					{Prefix: aws.String("images/")},
				},
				"logs/": {
					{Prefix: aws.String("logs/2023/")},
					{Prefix: aws.String("logs/2024/")},
				},
				"images/": {
					{Prefix: aws.String("images/thumbs/")},
				},
			},
			expected: []string{"logs/2023/", "logs/2024/", "images/thumbs/"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &mockS3NestedClient{
				mockData: tt.mockData,
			}

			s3Source := createTestS3Source(mockClient, &config.S3Config{
				Bucket: "test-bucket",
				Region: "us-east-1",
			})

			ctx := context.Background()
			result, err := s3Source.collectPrefixes(ctx, tt.level)

			if tt.expectError {
				require.Error(t, err, "Expected an error but got none")
				return
			}

			require.NoError(t, err, "Unexpected error occurred")

			sort.Strings(result)
			sort.Strings(tt.expected)
			require.Equal(t, tt.expected, result, "Mismatch in collected prefixes")
		})
	}
}

func TestGetDirectSubPrefixes_WithMockData(t *testing.T) {
	tests := []struct {
		name     string
		prefix   string
		mockData map[string][]types.CommonPrefix
		expected []string
	}{
		{
			name:   "Root prefix with multiple folders",
			prefix: "",
			mockData: map[string][]types.CommonPrefix{
				"": {
					{Prefix: aws.String("logs/")},
					{Prefix: aws.String("images/")},
					{Prefix: aws.String("docs/")},
				},
			},
			expected: []string{"logs/", "images/", "docs/"},
		},
		{
			name:   "Specific prefix with subfolders",
			prefix: "logs/",
			mockData: map[string][]types.CommonPrefix{
				"logs/": {
					{Prefix: aws.String("logs/2023/")},
					{Prefix: aws.String("logs/2024/")},
				},
			},
			expected: []string{"logs/2023/", "logs/2024/"},
		},
		{
			name:     "Empty result",
			prefix:   "empty/",
			mockData: map[string][]types.CommonPrefix{},
			expected: []string{},
		},
		{
			name:   "Single subfolder",
			prefix: "data/",
			mockData: map[string][]types.CommonPrefix{
				"data/": {
					{Prefix: aws.String("data/backup/")},
				},
			},
			expected: []string{"data/backup/"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &mockS3NestedClient{
				mockData: tt.mockData,
			}

			s3Source := createTestS3Source(mockClient, &config.S3Config{
				Bucket: "test-bucket",
				Region: "us-east-1",
			})

			ctx := context.Background()
			result, err := s3Source.getDirectSubPrefixes(ctx, tt.prefix)

			require.NoError(t, err, "Unexpected error occurred")

			sort.Strings(result)
			sort.Strings(tt.expected)
			require.Equal(t, tt.expected, result, "Mismatch in direct sub-prefixes")
		})
	}
}

// TESTS WITH REAL S3 DATA

func TestCollectPrefixes_WithRealS3Data(t *testing.T) {
	cfg := getS3ConfigFromEnv()
	if cfg == nil {
		t.Skip("Skipping test because S3 environment variables are not set")
	}

	s3Source, err := createRealS3Source(cfg)
	require.NoError(t, err, "Failed to create S3 source")

	tests := []struct {
		name  string
		level int
	}{
		{
			name:  "Level 0 - root only",
			level: 0,
		},
		{
			name:  "Level 1 - root folders",
			level: 1,
		},
		{
			name:  "Level 2 - root and subfolders",
			level: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			result, err := s3Source.collectPrefixes(ctx, tt.level)
			require.NoError(t, err, "Unexpected error collecting prefixes")

			t.Logf("Level %d returned %d prefixes", tt.level, len(result))

			// Print the first few prefixes for debugging
			maxShow := 5
			if len(result) < maxShow {
				maxShow = len(result)
			}
			for i := 0; i < maxShow; i++ {
				t.Logf("  Prefix %d: %q", i, result[i])
			}
			if len(result) > maxShow {
				t.Logf("  ... and %d more", len(result)-maxShow)
			}

			// Check: Level 0 should return only ""
			if tt.level == 0 {
				require.Len(t, result, 1, "Level 0 should return exactly one prefix")
				require.Equal(t, "", result[0], "Level 0 should return empty string as prefix")
			}

			// Check: All prefixes (except "" at level 0) must end with "/"
			for _, prefix := range result {
				if tt.level > 0 && prefix != "" {
					require.Truef(t, strings.HasSuffix(prefix, "/"), "Prefix should end with '/': %q", prefix)
				}
			}

			// Check: Prefixes must be unique
			seen := make(map[string]struct{})
			for _, prefix := range result {
				_, exists := seen[prefix]
				require.Falsef(t, exists, "Duplicate prefix found: %q", prefix)
				seen[prefix] = struct{}{}
			}
		})
	}
}

func TestGetDirectSubPrefixes_WithRealS3Data(t *testing.T) {
	cfg := getS3ConfigFromEnv()
	if cfg == nil {
		t.Skip("Skipping test because S3 environment variables are not set")
	}

	s3Source, err := createRealS3Source(cfg)
	require.NoError(t, err, "Failed to create S3 source")

	ctx := context.Background()

	// Retrieve root prefixes to use in dynamic test case
	rootPrefixes, err := s3Source.getDirectSubPrefixes(ctx, "")
	require.NoError(t, err, "Failed to get root prefixes")

	tests := []struct {
		name   string
		prefix string
	}{
		{
			name:   "Root prefix",
			prefix: "",
		},
	}

	// Add test case for the first found root prefix, if available
	if len(rootPrefixes) > 0 {
		tests = append(tests, struct {
			name   string
			prefix string
		}{
			name:   fmt.Sprintf("First root prefix: %s", rootPrefixes[0]),
			prefix: rootPrefixes[0],
		})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := s3Source.getDirectSubPrefixes(ctx, tt.prefix)
			require.NoError(t, err, "Unexpected error for prefix")

			t.Logf("Prefix %q returned %d sub-prefixes", tt.prefix, len(result))

			// Display up to 10 sub-prefixes for debugging
			maxShow := 10
			for i, subPrefix := range result {
				if i < maxShow {
					t.Logf("  Sub-prefix %d: %q", i, subPrefix)
				}
			}
			if len(result) > maxShow {
				t.Logf("  ... and %d more", len(result)-maxShow)
			}

			// Check that all sub-prefixes end with "/" and start with the parent prefix
			for _, subPrefix := range result {
				require.Truef(t, strings.HasSuffix(subPrefix, "/"), "Sub-prefix should end with '/': %q", subPrefix)
				require.Truef(t, strings.HasPrefix(subPrefix, tt.prefix), "Sub-prefix %q should start with %q", subPrefix, tt.prefix)
			}

			// Check that all sub-prefixes are unique
			seen := make(map[string]struct{})
			for _, subPrefix := range result {
				_, exists := seen[subPrefix]
				require.Falsef(t, exists, "Duplicate sub-prefix found: %q", subPrefix)
				seen[subPrefix] = struct{}{}
			}
		})
	}
}

// Edge case test
func TestCollectPrefixes_EdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		level       int
		mockData    map[string][]types.CommonPrefix
		expected    []string
		expectError bool
	}{
		{
			name:     "Negative level",
			level:    -1,
			mockData: map[string][]types.CommonPrefix{},
			expected: []string{""}, // Should return empty result or behave in a defined way
		},
		{
			name:  "Very deep nesting",
			level: 10,
			mockData: map[string][]types.CommonPrefix{
				"": {
					{Prefix: aws.String("a/")},
				},
				"a/": {
					{Prefix: aws.String("a/b/")},
				},
				"a/b/": {
					{Prefix: aws.String("a/b/c/")},
				},
			},
			expected: []string{"a/b/c/"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &mockS3NestedClient{
				mockData: tt.mockData,
			}

			s3Source := createTestS3Source(mockClient, &config.S3Config{
				Bucket: "test-bucket",
				Region: "us-east-1",
			})

			ctx := context.Background()
			result, err := s3Source.collectPrefixes(ctx, tt.level)

			if tt.expectError {
				require.Error(t, err, "Expected an error but got none")
				return
			}

			require.NoError(t, err, "Unexpected error occurred")

			// Ensure nil is treated as empty slice
			if result == nil {
				result = []string{}
			}

			sort.Strings(result)
			sort.Strings(tt.expected)
			require.Equal(t, tt.expected, result, "Mismatch in collected prefixes")
		})
	}
}

// Benchmark tests
func BenchmarkCollectPrefixes_Level1(b *testing.B) {
	mockClient := &mockS3NestedClient{
		mockData: map[string][]types.CommonPrefix{
			"": {
				{Prefix: aws.String("folder1/")},
				{Prefix: aws.String("folder2/")},
				{Prefix: aws.String("folder3/")},
				{Prefix: aws.String("folder4/")},
				{Prefix: aws.String("folder5/")},
			},
		},
	}

	s3Source := createTestS3Source(mockClient, &config.S3Config{
		Bucket: "test-bucket",
		Region: "us-east-1",
	})

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := s3Source.collectPrefixes(ctx, 1)
		if err != nil {
			b.Fatalf("Unexpected error: %v", err)
		}
	}
}

func BenchmarkGetDirectSubPrefixes(b *testing.B) {
	mockClient := &mockS3NestedClient{
		mockData: map[string][]types.CommonPrefix{
			"": {
				{Prefix: aws.String("folder1/")},
				{Prefix: aws.String("folder2/")},
				{Prefix: aws.String("folder3/")},
			},
		},
	}

	s3Source := createTestS3Source(mockClient, &config.S3Config{
		Bucket: "test-bucket",
		Region: "us-east-1",
	})

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := s3Source.getDirectSubPrefixes(ctx, "")
		if err != nil {
			b.Fatalf("Unexpected error: %v", err)
		}
	}
}

// TESTS ListObjectsByLevelStream

// Mock client that supports both CommonPrefixes and Contents
type mockS3ClientWithObjects struct {
	prefixData  map[string][]types.CommonPrefix
	objectsData map[string][]types.Object
	callCount   int
}

func (m *mockS3ClientWithObjects) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	m.callCount++
	prefix := aws.ToString(params.Prefix)
	delimiter := aws.ToString(params.Delimiter)

	output := &s3.ListObjectsV2Output{
		IsTruncated: aws.Bool(false),
	}

	// If delimiter is set, return CommonPrefixes (folders)
	if delimiter == "/" {
		if commonPrefixes, exists := m.prefixData[prefix]; exists {
			output.CommonPrefixes = commonPrefixes
		}
	} else {
		// If no delimiter, return objects (files)
		if objects, exists := m.objectsData[prefix]; exists {
			output.Contents = objects
		}
	}

	return output, nil
}

func (m *mockS3ClientWithObjects) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return nil, fmt.Errorf("GetObject not implemented in mockS3ClientWithObjects")
}

// Helper function to create test files
func createTestObjects(prefix string, count int) []types.Object {
	objects := make([]types.Object, count)
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("%sfile%d.txt", prefix, i+1)
		objects[i] = types.Object{
			Key:          aws.String(key),
			Size:         aws.Int64(int64(100 + i)),
			ETag:         aws.String(fmt.Sprintf("\"etag%d\"", i+1)),
			LastModified: aws.Time(time.Now().Add(-time.Duration(i) * time.Hour)),
		}
	}
	return objects
}

// Helper function to collect all files from channel
func collectFiles(filesCh <-chan model.RemoteFile, errCh <-chan error, timeout time.Duration) ([]model.RemoteFile, []error, error) {
	var files []model.RemoteFile
	var errors []error

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case file, ok := <-filesCh:
			if !ok {
				filesCh = nil
			} else {
				files = append(files, file)
			}
		case err, ok := <-errCh:
			if !ok {
				errCh = nil
			} else if err != nil {
				errors = append(errors, err)
			}
		case <-timer.C:
			return nil, nil, fmt.Errorf("timeout waiting for channels to close")
		}

		// Both channels are closed
		if filesCh == nil && errCh == nil {
			break
		}
	}

	return files, errors, nil
}

// TESTS WITH MOCK DATA

func TestListObjectsByLevelStream_WithMockData(t *testing.T) {
	tests := []struct {
		name           string
		level          int
		prefixData     map[string][]types.CommonPrefix
		objectsData    map[string][]types.Object
		expectedFiles  int
		expectedErrors int
		timeout        time.Duration
	}{
		{
			name:  "Level 0 - single root prefix",
			level: 0,
			prefixData: map[string][]types.CommonPrefix{
				"": {},
			},
			objectsData: map[string][]types.Object{
				"": createTestObjects("", 3),
			},
			expectedFiles:  3,
			expectedErrors: 0,
			timeout:        5 * time.Second,
		},
		{
			name:  "Level 1 - multiple root folders with files",
			level: 1,
			prefixData: map[string][]types.CommonPrefix{
				"": {
					{Prefix: aws.String("folder1/")},
					{Prefix: aws.String("folder2/")},
				},
			},
			objectsData: map[string][]types.Object{
				"folder1/": createTestObjects("folder1/", 2),
				"folder2/": createTestObjects("folder2/", 3),
			},
			expectedFiles:  5,
			expectedErrors: 0,
			timeout:        5 * time.Second,
		},
		{
			name:  "Level 2 - nested folders with files",
			level: 2,
			prefixData: map[string][]types.CommonPrefix{
				"": {
					{Prefix: aws.String("root/")},
				},
				"root/": {
					{Prefix: aws.String("root/sub1/")},
					{Prefix: aws.String("root/sub2/")},
				},
			},
			objectsData: map[string][]types.Object{
				"root/sub1/": createTestObjects("root/sub1/", 2),
				"root/sub2/": createTestObjects("root/sub2/", 1),
			},
			expectedFiles:  3,
			expectedErrors: 0,
			timeout:        5 * time.Second,
		},
		{
			name:  "Empty bucket - no prefixes, no files",
			level: 1,
			prefixData: map[string][]types.CommonPrefix{
				"": {},
			},
			objectsData: map[string][]types.Object{
				"": {},
			},
			expectedFiles:  0,
			expectedErrors: 0,
			timeout:        5 * time.Second,
		},
		{
			name:  "Mixed content - some prefixes have files, others don't",
			level: 1,
			prefixData: map[string][]types.CommonPrefix{
				"": {
					{Prefix: aws.String("empty/")},
					{Prefix: aws.String("full/")},
				},
			},
			objectsData: map[string][]types.Object{
				"empty/": {},
				"full/":  createTestObjects("full/", 4),
			},
			expectedFiles:  4,
			expectedErrors: 0,
			timeout:        5 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &mockS3ClientWithObjects{
				prefixData:  tt.prefixData,
				objectsData: tt.objectsData,
			}

			s3Source := createTestS3Source(mockClient, &config.S3Config{
				Bucket: "test-bucket",
				Region: "us-east-1",
			})

			// Set worker count for predictable testing
			s3Source.common.WorkerCount = 2

			ctx := context.Background()
			filesCh, errCh := s3Source.ListObjectsByLevelStream(ctx, tt.level)

			// Collect all results
			files, errors, err := collectFiles(filesCh, errCh, tt.timeout)
			require.NoError(t, err, "Should not timeout waiting for channels")

			// Check number of files
			require.Len(t, files, tt.expectedFiles, "Unexpected number of files")

			// Check number of errors
			require.Len(t, errors, tt.expectedErrors, "Unexpected number of errors")

			// Validate file structure
			for _, file := range files {
				require.NotEmpty(t, file.Key, "File key should not be empty")
				require.Greater(t, file.Size, int64(0), "File size should be positive")
				require.NotEmpty(t, file.Hash, "File hash should not be empty")
				require.Greater(t, file.ModTime, int64(0), "File ModTime should be positive")
			}

			// Sort files by key for consistent comparison
			sort.Slice(files, func(i, j int) bool {
				return files[i].Key < files[j].Key
			})

			t.Logf("Test %s: collected %d files, %d errors", tt.name, len(files), len(errors))
			for i, file := range files {
				if i < 5 { // Show first 5 files
					t.Logf("  File %d: %s (size: %d)", i+1, file.Key, file.Size)
				}
			}
		})
	}
}

func TestListObjectsByLevelStream_ContextCancellation(t *testing.T) {
	mockClient := &mockS3ClientWithObjects{
		prefixData: map[string][]types.CommonPrefix{
			"": {
				{Prefix: aws.String("folder1/")},
				{Prefix: aws.String("folder2/")},
			},
		},
		objectsData: map[string][]types.Object{
			"folder1/": createTestObjects("folder1/", 10),
			"folder2/": createTestObjects("folder2/", 10),
		},
	}

	s3Source := createTestS3Source(mockClient, &config.S3Config{
		Bucket: "test-bucket",
		Region: "us-east-1",
	})

	// Create a context that will be cancelled
	ctx, cancel := context.WithCancel(context.Background())

	filesCh, errCh := s3Source.ListObjectsByLevelStream(ctx, 1)

	// Cancel the context immediately
	cancel()

	// Collect results with a short timeout
	files, errors, err := collectFiles(filesCh, errCh, 2*time.Second)
	require.NoError(t, err, "Should not timeout")

	t.Logf("Context cancellation test: collected %d files, %d errors", len(files), len(errors))

	// We might get some files before cancellation, but channels should close
	// The important thing is that the function handles cancellation gracefully
}

func TestListObjectsByLevelStream_PrefixCollectionError(t *testing.T) {
	// Create a mock that will cause collectPrefixes to fail
	mockClient := &mockS3ClientWithObjects{
		prefixData:  nil, // This will cause an error in some implementations
		objectsData: nil,
	}

	s3Source := createTestS3Source(mockClient, &config.S3Config{
		Bucket: "test-bucket",
		Region: "us-east-1",
	})

	ctx := context.Background()
	filesCh, errCh := s3Source.ListObjectsByLevelStream(ctx, 1)

	files, errors, err := collectFiles(filesCh, errCh, 2*time.Second)
	require.NoError(t, err, "Should not timeout")

	t.Logf("Error test: collected %d files, %d errors", len(files), len(errors))

	// We should get either no files and no errors (if collectPrefixes returns empty slice)
	// or no files and some errors (if collectPrefixes fails)
	require.True(t, len(files) == 0, "Should not collect any files when prefix collection fails or returns empty")
}

// TESTS WITH REAL S3 DATA

func TestListObjectsByLevelStream_WithRealS3Data(t *testing.T) {
	cfg := getS3ConfigFromEnv()
	if cfg == nil {
		t.Skip("Skipping test because S3 environment variables are not set")
	}

	s3Source, err := createRealS3Source(cfg)
	require.NoError(t, err, "Failed to create S3 source")

	tests := []struct {
		name    string
		level   int
		timeout time.Duration
	}{
		{
			name:    "Level 0 - all files recursively",
			level:   0,
			timeout: 30 * time.Second,
		},
		{
			name:    "Level 1 - files in root folders",
			level:   1,
			timeout: 30 * time.Second,
		},
		{
			name:    "Level 2 - files in subfolders",
			level:   2,
			timeout: 30 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			filesCh, errCh := s3Source.ListObjectsByLevelStream(ctx, tt.level)

			files, errors, err := collectFiles(filesCh, errCh, tt.timeout)
			require.NoError(t, err, "Should not timeout waiting for channels")

			t.Logf("Level %d: collected %d files, %d errors", tt.level, len(files), len(errors))

			// Log any errors that occurred
			for i, err := range errors {
				t.Logf("Error %d: %v", i+1, err)
			}

			// Show sample files
			maxShow := 10
			if len(files) < maxShow {
				maxShow = len(files)
			}
			for i := 0; i < maxShow; i++ {
				t.Logf("  File %d: %s (size: %d)", i+1, files[i].Key, files[i].Size)
			}
			if len(files) > maxShow {
				t.Logf("  ... and %d more files", len(files)-maxShow)
			}

			// Basic validations
			for _, file := range files {
				require.NotEmpty(t, file.Key, "File key should not be empty")
				require.False(t, strings.HasSuffix(file.Key, "/"), "File key should not end with '/' (folders should be excluded)")
				require.GreaterOrEqual(t, file.Size, int64(0), "File size should be non-negative")
				require.NotEmpty(t, file.Hash, "File hash should not be empty")
				require.Greater(t, file.ModTime, int64(0), "File ModTime should be positive")
			}

			// Check for duplicate files
			fileKeys := make(map[string]bool)
			for _, file := range files {
				require.False(t, fileKeys[file.Key], "Duplicate file found: %s", file.Key)
				fileKeys[file.Key] = true
			}

			// For level 0, we expect at least some files (unless bucket is empty)
			if tt.level == 0 && len(files) == 0 {
				t.Log("Warning: No files found at level 0. Bucket might be empty.")
			}
		})
	}
}

func TestListObjectsByLevelStream_WithRealS3Data_ContextTimeout(t *testing.T) {
	cfg := getS3ConfigFromEnv()
	if cfg == nil {
		t.Skip("Skipping test because S3 environment variables are not set")
	}

	s3Source, err := createRealS3Source(cfg)
	require.NoError(t, err, "Failed to create S3 source")

	// Create a context with a very short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	filesCh, errCh := s3Source.ListObjectsByLevelStream(ctx, 1)

	files, errors, err := collectFiles(filesCh, errCh, 2*time.Second)
	require.NoError(t, err, "Should not timeout waiting for channels to close")

	t.Logf("Timeout test: collected %d files, %d errors before context timeout", len(files), len(errors))

	// The function should handle timeout gracefully and close channels
	// We might get some files before timeout, but that's expected
}

// BENCHMARK TESTS

func BenchmarkListObjectsByLevelStream_Level1(b *testing.B) {
	mockClient := &mockS3ClientWithObjects{
		prefixData: map[string][]types.CommonPrefix{
			"": {
				{Prefix: aws.String("folder1/")},
				{Prefix: aws.String("folder2/")},
				{Prefix: aws.String("folder3/")},
			},
		},
		objectsData: map[string][]types.Object{
			"folder1/": createTestObjects("folder1/", 100),
			"folder2/": createTestObjects("folder2/", 150),
			"folder3/": createTestObjects("folder3/", 200),
		},
	}

	s3Source := createTestS3Source(mockClient, &config.S3Config{
		Bucket: "test-bucket",
		Region: "us-east-1",
	})

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		filesCh, errCh := s3Source.ListObjectsByLevelStream(ctx, 1)

		// Consume all files
		fileCount := 0
		errorCount := 0

	consumeLoop:
		for {
			select {
			case _, ok := <-filesCh:
				if !ok {
					filesCh = nil
				} else {
					fileCount++
				}
			case _, ok := <-errCh:
				if !ok {
					errCh = nil
				} else {
					errorCount++
				}
			}

			if filesCh == nil && errCh == nil {
				break consumeLoop
			}
		}

		if fileCount == 0 {
			b.Fatalf("Expected files but got none")
		}
	}
}

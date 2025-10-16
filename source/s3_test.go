package source

import (
	"context"
	"os"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/olegkotsar/yomins-sync/config"
)

func getS3ConfigFromEnv() *config.S3Config {
	endpoint := os.Getenv("S3_ENDPOINT")
	bucket := os.Getenv("S3_BUCKET")
	accessKey := os.Getenv("S3_ACCESS_KEY")
	secretKey := os.Getenv("S3_SECRET_KEY")

	if endpoint == "" || bucket == "" || accessKey == "" || secretKey == "" {
		return nil
	}

	return &config.S3Config{
		Endpoint:        endpoint,
		Bucket:          bucket,
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
	}
}

// mockS3Client simulates the AWS S3 client
type mockS3Client struct {
	responses map[string]*s3.ListObjectsV2Output
	hook      func() // optional hook for testing concurrency
}

func (m *mockS3Client) ListObjectsV2(ctx context.Context, input *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if m.hook != nil {
		m.hook()
	}
	if resp, ok := m.responses[*input.Prefix]; ok {
		return resp, nil
	}
	return &s3.ListObjectsV2Output{}, nil
}

func (m *mockS3Client) GetObject(ctx context.Context, input *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return nil, nil
}

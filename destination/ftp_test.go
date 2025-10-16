package destination

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/olegkotsar/yomins-sync/config"
	"github.com/stretchr/testify/require"
)

// getFTPConfigFromEnv reads FTP configuration from environment variables for integration testing
func getFTPConfigFromEnv() *config.FTPConfig {
	host := os.Getenv("FTP_HOST")
	port := os.Getenv("FTP_PORT")
	username := os.Getenv("FTP_USERNAME")
	password := os.Getenv("FTP_PASSWORD")

	if host == "" || username == "" {
		return nil
	}

	cfg := &config.FTPConfig{
		Host:     host,
		Username: username,
		Password: password,
	}

	if port != "" {
		// Parse port if provided
		var p int
		_, err := fmt.Sscanf(port, "%d", &p)
		if err == nil {
			cfg.Port = p
		}
	}

	return cfg
}

func TestNewFTPDestination_InvalidConfig(t *testing.T) {
	tests := []struct {
		name         string
		ftpCfg       *config.FTPConfig
		commonCfg    *config.CommonDestinationConfig
		expectError  bool
		errorMessage string
	}{
		{
			name: "missing host",
			ftpCfg: &config.FTPConfig{
				Host:     "",
				Port:     21,
				Username: "user",
				Password: "pass",
			},
			commonCfg: &config.CommonDestinationConfig{
				WorkerCount:    5,
				TimeoutSeconds: 30,
				MaxRetries:     3,
			},
			expectError:  true,
			errorMessage: "host",
		},
		{
			name: "missing username",
			ftpCfg: &config.FTPConfig{
				Host:     "localhost",
				Port:     21,
				Username: "",
				Password: "pass",
			},
			commonCfg: &config.CommonDestinationConfig{
				WorkerCount:    5,
				TimeoutSeconds: 30,
				MaxRetries:     3,
			},
			expectError:  true,
			errorMessage: "username",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dest, err := NewFTPDestination(tt.ftpCfg, tt.commonCfg)

			if tt.expectError {
				require.Error(t, err)
				require.Nil(t, dest)
				require.Contains(t, err.Error(), tt.errorMessage)
			} else {
				require.NoError(t, err)
				require.NotNil(t, dest)
				if dest != nil {
					dest.Close()
				}
			}
		})
	}
}

func TestNewFTPDestination_Defaults(t *testing.T) {
	// Skip if no FTP server is available
	if os.Getenv("FTP_HOST") == "" {
		t.Skip("Skipping test because FTP_HOST environment variable is not set")
	}

	cfg := getFTPConfigFromEnv()
	if cfg == nil {
		t.Skip("Skipping test because FTP configuration is not available")
	}

	commonCfg := &config.CommonDestinationConfig{}

	dest, err := NewFTPDestination(cfg, commonCfg)
	require.NoError(t, err)
	require.NotNil(t, dest)
	defer dest.Close()

	// Check that defaults were applied
	require.Equal(t, 5, dest.common.WorkerCount)
	require.Equal(t, 30, dest.common.TimeoutSeconds)
	require.Equal(t, 3, dest.common.MaxRetries)
}

func TestFTPDestination_GetWorkerCount(t *testing.T) {
	dest := &FTPDestination{
		common: &config.CommonDestinationConfig{
			WorkerCount: 10,
		},
	}

	count := dest.GetWorkerCount()
	require.Equal(t, 10, count)
}

// Integration tests (require real FTP server)

func TestFTPDestination_Upload_Integration(t *testing.T) {
	cfg := getFTPConfigFromEnv()
	if cfg == nil {
		t.Skip("Skipping test because FTP configuration is not available")
	}

	commonCfg := &config.CommonDestinationConfig{}
	dest, err := NewFTPDestination(cfg, commonCfg)
	require.NoError(t, err)
	require.NotNil(t, dest)
	defer dest.Close()

	ctx := context.Background()

	// Upload a test file
	testContent := strings.NewReader("test file content for FTP upload")
	err = dest.Upload(ctx, "test_upload.txt", testContent)
	require.NoError(t, err)

	// Check if file exists
	exists, err := dest.FileExists(ctx, "test_upload.txt")
	require.NoError(t, err)
	require.True(t, exists)

	// Clean up - delete the file
	err = dest.Delete(ctx, "test_upload.txt")
	require.NoError(t, err)
}

func TestFTPDestination_Upload_WithDirectories_Integration(t *testing.T) {
	cfg := getFTPConfigFromEnv()
	if cfg == nil {
		t.Skip("Skipping test because FTP configuration is not available")
	}

	commonCfg := &config.CommonDestinationConfig{}
	dest, err := NewFTPDestination(cfg, commonCfg)
	require.NoError(t, err)
	require.NotNil(t, dest)
	defer dest.Close()

	ctx := context.Background()

	// Upload a file with nested directories
	testContent := strings.NewReader("test file content in nested directory")
	err = dest.Upload(ctx, "test_dir/subdir/test_nested.txt", testContent)
	require.NoError(t, err)

	// Check if file exists
	exists, err := dest.FileExists(ctx, "test_dir/subdir/test_nested.txt")
	require.NoError(t, err)
	require.True(t, exists)

	// Clean up - delete the file
	err = dest.Delete(ctx, "test_dir/subdir/test_nested.txt")
	require.NoError(t, err)
}

func TestFTPDestination_Delete_Integration(t *testing.T) {
	cfg := getFTPConfigFromEnv()
	if cfg == nil {
		t.Skip("Skipping test because FTP configuration is not available")
	}

	commonCfg := &config.CommonDestinationConfig{}
	dest, err := NewFTPDestination(cfg, commonCfg)
	require.NoError(t, err)
	require.NotNil(t, dest)
	defer dest.Close()

	ctx := context.Background()

	// Upload a test file first
	testContent := strings.NewReader("test file for deletion")
	err = dest.Upload(ctx, "test_delete.txt", testContent)
	require.NoError(t, err)

	// Verify it exists
	exists, err := dest.FileExists(ctx, "test_delete.txt")
	require.NoError(t, err)
	require.True(t, exists)

	// Delete the file
	err = dest.Delete(ctx, "test_delete.txt")
	require.NoError(t, err)

	// Verify it's deleted
	exists, err = dest.FileExists(ctx, "test_delete.txt")
	require.NoError(t, err)
	require.False(t, exists)

	// Delete non-existent file should not error
	err = dest.Delete(ctx, "test_delete.txt")
	require.NoError(t, err)
}

func TestFTPDestination_FileExists_Integration(t *testing.T) {
	cfg := getFTPConfigFromEnv()
	if cfg == nil {
		t.Skip("Skipping test because FTP configuration is not available")
	}

	commonCfg := &config.CommonDestinationConfig{}
	dest, err := NewFTPDestination(cfg, commonCfg)
	require.NoError(t, err)
	require.NotNil(t, dest)
	defer dest.Close()

	ctx := context.Background()

	// Check non-existent file
	exists, err := dest.FileExists(ctx, "nonexistent_file.txt")
	require.NoError(t, err)
	require.False(t, exists)

	// Upload a file
	testContent := strings.NewReader("test file for existence check")
	err = dest.Upload(ctx, "test_exists.txt", testContent)
	require.NoError(t, err)

	// Check existing file
	exists, err = dest.FileExists(ctx, "test_exists.txt")
	require.NoError(t, err)
	require.True(t, exists)

	// Clean up
	err = dest.Delete(ctx, "test_exists.txt")
	require.NoError(t, err)
}

func TestFTPDestination_Upload_Retry_Integration(t *testing.T) {
	cfg := getFTPConfigFromEnv()
	if cfg == nil {
		t.Skip("Skipping test because FTP configuration is not available")
	}

	// Set max retries to ensure retry logic works
	commonCfg := &config.CommonDestinationConfig{
		MaxRetries: 3,
	}
	dest, err := NewFTPDestination(cfg, commonCfg)
	require.NoError(t, err)
	require.NotNil(t, dest)
	defer dest.Close()

	ctx := context.Background()

	// Upload a test file (should succeed even with retries configured)
	testContent := strings.NewReader("test file content for retry test")
	err = dest.Upload(ctx, "test_retry.txt", testContent)
	require.NoError(t, err)

	// Clean up
	err = dest.Delete(ctx, "test_retry.txt")
	require.NoError(t, err)
}

func TestFTPDestination_Close_Integration(t *testing.T) {
	cfg := getFTPConfigFromEnv()
	if cfg == nil {
		t.Skip("Skipping test because FTP configuration is not available")
	}

	commonCfg := &config.CommonDestinationConfig{}
	dest, err := NewFTPDestination(cfg, commonCfg)
	require.NoError(t, err)
	require.NotNil(t, dest)

	// Close should not error
	err = dest.Close()
	require.NoError(t, err)

	// Multiple closes should be safe
	err = dest.Close()
	require.NoError(t, err)
}

func TestFTPDestination_ContextCancellation_Integration(t *testing.T) {
	cfg := getFTPConfigFromEnv()
	if cfg == nil {
		t.Skip("Skipping test because FTP configuration is not available")
	}

	commonCfg := &config.CommonDestinationConfig{}
	dest, err := NewFTPDestination(cfg, commonCfg)
	require.NoError(t, err)
	require.NotNil(t, dest)
	defer dest.Close()

	// Create a cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Try to upload with cancelled context (should fail gracefully)
	testContent := strings.NewReader("test content")
	err = dest.Upload(ctx, "test_cancelled.txt", testContent)
	require.Error(t, err)
}

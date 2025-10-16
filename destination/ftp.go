package destination

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"math"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/jlaffaye/ftp"
	"github.com/olegkotsar/yomins-sync/config"
)

var _ DestinationProvider = (*FTPDestination)(nil)

// FTPDestination implements DestinationProvider for FTP servers
type FTPDestination struct {
	config     *config.FTPConfig
	common     *config.CommonDestinationConfig
	connPool   chan *ftp.ServerConn
	poolSize   int
	mu         sync.Mutex
	dialConfig *ftp.DialOption
}

// NewFTPDestination creates a new FTP destination
func NewFTPDestination(cfg *config.FTPConfig, common *config.CommonDestinationConfig) (*FTPDestination, error) {
	// Apply defaults
	cfg.ApplyDefaults()
	common.ApplyDefaults()

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid ftp config: %w", err)
	}
	if err := common.Validate(); err != nil {
		return nil, fmt.Errorf("invalid common config: %w", err)
	}

	// Create connection pool
	poolSize := common.WorkerCount
	connPool := make(chan *ftp.ServerConn, poolSize)

	// Setup dial options
	var dialConfig *ftp.DialOption
	if cfg.UseTLS {
		opt := ftp.DialWithExplicitTLS(&tls.Config{
			InsecureSkipVerify: false, // TODO: Make this configurable
		})
		dialConfig = &opt
	}

	dest := &FTPDestination{
		config:     cfg,
		common:     common,
		connPool:   connPool,
		poolSize:   poolSize,
		dialConfig: dialConfig,
	}

	// Pre-populate connection pool with one connection to verify connectivity
	conn, err := dest.createConnection()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to FTP server: %w", err)
	}

	// Return connection to pool
	select {
	case connPool <- conn:
	default:
		conn.Quit()
	}

	return dest, nil
}

// createConnection creates a new FTP connection
func (f *FTPDestination) createConnection() (*ftp.ServerConn, error) {
	addr := fmt.Sprintf("%s:%d", f.config.Host, f.config.Port)

	var conn *ftp.ServerConn
	var err error

	if f.dialConfig != nil {
		conn, err = ftp.Dial(addr, *f.dialConfig, ftp.DialWithTimeout(time.Duration(f.common.TimeoutSeconds)*time.Second))
	} else {
		conn, err = ftp.Dial(addr, ftp.DialWithTimeout(time.Duration(f.common.TimeoutSeconds)*time.Second))
	}

	if err != nil {
		return nil, fmt.Errorf("failed to dial: %w", err)
	}

	// Login
	if err := conn.Login(f.config.Username, f.config.Password); err != nil {
		conn.Quit()
		return nil, fmt.Errorf("failed to login: %w", err)
	}

	return conn, nil
}

// getConnection retrieves a connection from the pool or creates a new one
func (f *FTPDestination) getConnection(ctx context.Context) (*ftp.ServerConn, error) {
	select {
	case conn := <-f.connPool:
		// Test if connection is still alive
		if err := conn.NoOp(); err != nil {
			// Connection is dead, create a new one
			conn.Quit()
			return f.createConnection()
		}
		return conn, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// No connection available, create a new one
		return f.createConnection()
	}
}

// returnConnection returns a connection to the pool
func (f *FTPDestination) returnConnection(conn *ftp.ServerConn) {
	if conn == nil {
		return
	}

	select {
	case f.connPool <- conn:
		// Connection returned to pool
	default:
		// Pool is full, close the connection
		conn.Quit()
	}
}

// Upload uploads a file to the FTP server
func (f *FTPDestination) Upload(ctx context.Context, filePath string, content io.Reader) error {
	// Construct full path
	fullPath := path.Join(f.config.BasePath, filePath)

	// Retry logic with exponential backoff
	var lastErr error
	for attempt := 0; attempt < f.common.MaxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff
			backoff := time.Duration(math.Pow(2, float64(attempt))) * 200 * time.Millisecond
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		// Get connection from pool
		conn, err := f.getConnection(ctx)
		if err != nil {
			lastErr = err
			continue
		}

		// Create directory structure if needed
		dir := path.Dir(fullPath)
		if dir != "/" && dir != "." {
			if err := f.ensureDirectory(conn, dir); err != nil {
				f.returnConnection(conn)
				lastErr = fmt.Errorf("failed to create directory %s: %w", dir, err)
				continue
			}
		}

		// Upload file
		err = conn.Stor(fullPath, content)
		f.returnConnection(conn)

		if err == nil {
			return nil
		}

		lastErr = fmt.Errorf("failed to upload %s: %w", fullPath, err)
	}

	return fmt.Errorf("upload failed after %d attempts: %w", f.common.MaxRetries, lastErr)
}

// ensureDirectory creates directory structure recursively
func (f *FTPDestination) ensureDirectory(conn *ftp.ServerConn, dirPath string) error {
	// Normalize path
	dirPath = path.Clean(dirPath)
	if dirPath == "/" || dirPath == "." {
		return nil
	}

	// Try to change to directory
	currentDir, err := conn.CurrentDir()
	if err != nil {
		return err
	}

	// Try to change to target directory
	if err := conn.ChangeDir(dirPath); err == nil {
		// Directory exists, return to original directory
		conn.ChangeDir(currentDir)
		return nil
	}

	// Directory doesn't exist, create it recursively
	parts := strings.Split(dirPath, "/")
	currentPath := ""

	for _, part := range parts {
		if part == "" {
			continue
		}

		if currentPath == "" {
			currentPath = part
		} else {
			currentPath = path.Join(currentPath, part)
		}

		// Try to create directory (ignore error if already exists)
		conn.MakeDir(currentPath)
	}

	// Return to original directory
	return conn.ChangeDir(currentDir)
}

// Delete deletes a file from the FTP server
func (f *FTPDestination) Delete(ctx context.Context, filePath string) error {
	// Construct full path
	fullPath := path.Join(f.config.BasePath, filePath)

	// Retry logic with exponential backoff
	var lastErr error
	for attempt := 0; attempt < f.common.MaxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff
			backoff := time.Duration(math.Pow(2, float64(attempt))) * 200 * time.Millisecond
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		// Get connection from pool
		conn, err := f.getConnection(ctx)
		if err != nil {
			lastErr = err
			continue
		}

		// Delete file
		err = conn.Delete(fullPath)
		f.returnConnection(conn)

		if err == nil {
			return nil
		}

		// Check if file doesn't exist (not an error in this case)
		if strings.Contains(err.Error(), "550") || strings.Contains(err.Error(), "not found") {
			return nil // File already doesn't exist
		}

		lastErr = fmt.Errorf("failed to delete %s: %w", fullPath, err)
	}

	return fmt.Errorf("delete failed after %d attempts: %w", f.common.MaxRetries, lastErr)
}

// FileExists checks if a file exists on the FTP server
func (f *FTPDestination) FileExists(ctx context.Context, filePath string) (bool, error) {
	// Construct full path
	fullPath := path.Join(f.config.BasePath, filePath)

	// Get connection from pool
	conn, err := f.getConnection(ctx)
	if err != nil {
		return false, err
	}
	defer f.returnConnection(conn)

	// Try to get file size (if file exists, this will succeed)
	_, err = conn.FileSize(fullPath)
	if err == nil {
		return true, nil
	}

	// Check if error indicates file doesn't exist
	if strings.Contains(err.Error(), "550") || strings.Contains(err.Error(), "not found") {
		return false, nil
	}

	return false, fmt.Errorf("failed to check file existence: %w", err)
}

// GetWorkerCount returns the configured number of parallel workers
func (f *FTPDestination) GetWorkerCount() int {
	return f.common.WorkerCount
}

// Close closes all connections in the pool
func (f *FTPDestination) Close() error {
	close(f.connPool)

	for conn := range f.connPool {
		if err := conn.Quit(); err != nil {
			// Log error but continue closing other connections
		}
	}

	return nil
}

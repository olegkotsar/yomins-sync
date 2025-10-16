# Destination Package

FTP destination implementation for syncing files from S3 to FTP servers.

## Features

- **FTP/FTPS support** - Connect via plain FTP or FTP over TLS
- **Connection pooling** - Reuses connections for better performance
- **Parallel uploads** - Multiple concurrent workers for faster syncing
- **Automatic retry** - Configurable retry logic with exponential backoff
- **Directory creation** - Automatically creates directory structure
- **Timeouts** - Configurable timeouts for all operations

## Configuration

```go
cfg := &config.DestinationConfig{
    DestinationType: config.DestinationTypeFTP,
    Common: config.CommonDestinationConfig{
        WorkerCount:    10,  // Number of parallel upload workers
        TimeoutSeconds: 30,  // Operation timeout
        MaxRetries:     3,   // Retry attempts
    },
    FTP: &config.FTPConfig{
        Host:     "ftp.example.com",
        Port:     21,
        Username: "user",
        Password: "pass",
        BasePath: "/uploads",  // Base path on FTP server
        UseTLS:   false,       // Enable FTPS
    },
}

dest, err := destination.CreateDestination(cfg)
```

## Usage

```go
// Upload file
err := dest.Upload(ctx, "path/to/file.txt", fileContent)

// Delete file
err := dest.Delete(ctx, "path/to/file.txt")

// Check if file exists
exists, err := dest.FileExists(ctx, "path/to/file.txt")

// Close connections when done
dest.Close()
```

## Connection Pooling

The FTP destination maintains a pool of connections equal to `WorkerCount`. Connections are:
- Reused across multiple operations
- Automatically recreated if they become stale
- Tested with `NoOp` before use
- Closed when the pool is full

## Directory Handling

Directories are created automatically during upload:
- Recursively creates parent directories
- Uses the configured `BasePath` as root
- Handles both Unix (`/`) and Windows (`\`) path separators

## Error Handling

Operations retry automatically on failure:
- Exponential backoff between retries (200ms × 2^attempt)
- Respects context cancellation
- Returns detailed error messages
- File not found errors are not treated as failures for Delete

## Performance

For optimal performance:
- Set `WorkerCount` to 5-20 depending on FTP server capacity
- Use connection pooling (enabled by default)
- Consider enabling FTPS only if required (adds TLS overhead)
- Monitor FTP server load and adjust workers accordingly

## Limitations

- Does not support SFTP (SSH File Transfer Protocol) - only FTP/FTPS
- Passive mode only
- No support for custom ports per file
- Directory deletion is not implemented (only file deletion)

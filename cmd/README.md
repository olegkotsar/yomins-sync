# S3-to-FTP Synchronization Tool

Command-line application for synchronizing files from S3 to FTP servers.

## Features

- **Flexible Configuration** - Supports both environment variables and command-line flags
- **Graceful Shutdown** - Handles CTRL+C (SIGINT) and SIGTERM for safe interruption
- **Parallel Processing** - Configurable worker pools for source and destination
- **Comprehensive Logging** - Multiple log levels (silent, error, info, debug, verbose)
- **State Management** - Tracks file synchronization state in persistent cache

## Building

```bash
go build -o yomins-sync ./cmd
```

## Configuration

Configuration can be provided via:
1. **Environment variables** - Loaded first as defaults
2. **Command-line flags** - Override environment variables

Command-line flags take precedence over environment variables.

### Quick Start

Minimum required configuration:

```bash
# Using environment variables
export S3_REGION="us-east-1"
export S3_BUCKET="my-bucket"
export S3_ACCESS_KEY_ID="AKIAIOSFODNN7EXAMPLE"
export S3_SECRET_ACCESS_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
export S3_ENDPOINT="https://s3.amazonaws.com"
export FTP_HOST="ftp.example.com"
export FTP_USERNAME="user"
export FTP_PASSWORD="pass"

./yomins-sync
```

```bash
# Using command-line flags
./yomins-sync \
  --s3-region=us-east-1 \
  --s3-bucket=my-bucket \
  --s3-access-key=AKIAIOSFODNN7EXAMPLE \
  --s3-secret-key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
  --s3-endpoint=https://s3.amazonaws.com \
  --ftp-host=ftp.example.com \
  --ftp-username=user \
  --ftp-password=pass
```

## Command-Line Flags

### General Options
- `--dry-run` - Run in dry-run mode (no files will be uploaded, only logs) (default: false)
- `--help` - Show help message

### Logger Options
- `--log-level` - Log level: silent, error, info, debug, verbose (default: info)

### Cache Options
- `--cache-type` - Cache type: bbolt (default: bbolt)
- `--cache-path` - Path to cache database (default: ./cache.db)
- `--cache-bucket` - Cache bucket name (default: files)
- `--cache-no-sync` - Disable fsync for cache (improves performance, reduces durability)

### Source Options
- `--source-type` - Source type: s3 (default: s3)
- `--source-workers` - Number of source workers (default: 5)
- `--source-max-queue-size` - Max queue size for source operations (default: 1000)
- `--source-max-retries` - Max retries for source operations (default: 3)
- `--source-timeout` - Source operation timeout in seconds (default: 30)
- `--source-max-rps` - Max requests per second to source, 0 = no limit (default: 0)
- `--listing-strategy` - Listing strategy: recursive, by_level (default: recursive)
- `--listing-level` - Listing level for by_level strategy (required if using by_level)

### S3 Options
- `--s3-region` - S3 region (required)
- `--s3-bucket` - S3 bucket name (required)
- `--s3-access-key` - S3 access key ID (required)
- `--s3-secret-key` - S3 secret access key (required)
- `--s3-endpoint` - S3 endpoint URL (required)

### Destination Options
- `--dest-type` - Destination type: ftp (default: ftp)
- `--dest-workers` - Number of destination workers (default: 5)
- `--dest-max-retries` - Max retries for destination operations (default: 3)
- `--dest-timeout` - Destination operation timeout in seconds (default: 30)

### FTP Options
- `--ftp-host` - FTP server host (required)
- `--ftp-port` - FTP server port (default: 21)
- `--ftp-username` - FTP username (required)
- `--ftp-password` - FTP password
- `--ftp-base-path` - FTP base path (default: /)
- `--ftp-use-tls` - Use FTPS (default: false)

## Environment Variables

All command-line flags have corresponding environment variables:

| Flag | Environment Variable | Default |
|------|---------------------|---------|
| --dry-run | DRY_RUN | false |
| --log-level | LOG_LEVEL | info |
| --cache-type | CACHE_TYPE | bbolt |
| --cache-path | CACHE_BBOLT_PATH | ./cache.db |
| --cache-bucket | CACHE_BBOLT_BUCKET | files |
| --cache-no-sync | CACHE_BBOLT_NO_SYNC | false |
| --source-type | SOURCE_TYPE | s3 |
| --source-workers | SOURCE_WORKER_COUNT | 5 |
| --source-max-queue-size | SOURCE_MAX_QUEUE_SIZE | 1000 |
| --source-max-retries | SOURCE_MAX_RETRIES | 3 |
| --source-timeout | SOURCE_TIMEOUT_SECONDS | 30 |
| --source-max-rps | SOURCE_MAX_RPS | 0 |
| --listing-strategy | SOURCE_LISTING_STRATEGY | recursive |
| --listing-level | SOURCE_LISTING_LEVEL | 0 |
| --s3-region | S3_REGION | (required) |
| --s3-bucket | S3_BUCKET | (required) |
| --s3-access-key | S3_ACCESS_KEY_ID | (required) |
| --s3-secret-key | S3_SECRET_ACCESS_KEY | (required) |
| --s3-endpoint | S3_ENDPOINT | (required) |
| --dest-type | DESTINATION_TYPE | ftp |
| --dest-workers | DESTINATION_WORKER_COUNT | 5 |
| --dest-max-retries | DESTINATION_MAX_RETRIES | 3 |
| --dest-timeout | DESTINATION_TIMEOUT_SECONDS | 30 |
| --ftp-host | FTP_HOST | (required) |
| --ftp-port | FTP_PORT | 21 |
| --ftp-username | FTP_USERNAME | (required) |
| --ftp-password | FTP_PASSWORD | |
| --ftp-base-path | FTP_BASE_PATH | / |
| --ftp-use-tls | FTP_USE_TLS | false |

## Graceful Shutdown

The application supports graceful shutdown via CTRL+C (SIGINT) or SIGTERM:

```bash
./yomins-sync
# Press CTRL+C to stop
```

When interrupted:
1. Current operations are allowed to complete
2. No new files are processed
3. Connections are properly closed
4. Cache is safely closed

## Examples

### Basic synchronization

```bash
./yomins-sync \
  --s3-bucket=my-bucket \
  --s3-region=us-east-1 \
  --ftp-host=ftp.example.com
```

### With FTPS and custom paths

```bash
./yomins-sync \
  --s3-bucket=my-bucket \
  --s3-region=us-east-1 \
  --ftp-host=ftp.example.com \
  --ftp-use-tls \
  --ftp-base-path=/uploads
```

### High-performance setup

```bash
./yomins-sync \
  --s3-bucket=my-bucket \
  --s3-region=us-east-1 \
  --source-workers=20 \
  --dest-workers=20 \
  --ftp-host=ftp.example.com \
  --cache-no-sync
```

### Verbose logging for debugging

```bash
./yomins-sync \
  --log-level=verbose \
  --s3-bucket=my-bucket \
  --s3-region=us-east-1 \
  --ftp-host=ftp.example.com
```

### Using by-level listing strategy

For buckets with deep directory structures:

```bash
./yomins-sync \
  --s3-bucket=my-bucket \
  --s3-region=us-east-1 \
  --listing-strategy=by_level \
  --listing-level=3 \
  --ftp-host=ftp.example.com
```

### Dry-run mode

Test the synchronization without actually uploading files:

```bash
./yomins-sync \
  --dry-run \
  --s3-bucket=my-bucket \
  --s3-region=us-east-1 \
  --ftp-host=ftp.example.com
```

In dry-run mode:
- Files are scanned and compared as normal
- Cache states are updated (NEW, SYNCED, etc.)
- Logs show what would be synced
- **No files are actually uploaded to FTP**

This is useful for:
- Testing configuration before running actual sync
- Estimating how many files will be transferred
- Verifying which files are marked as NEW

## Exit Codes

- `0` - Success
- `1` - Error (configuration, runtime, or shutdown error)

## Logs

Log levels (set via `--log-level` or `LOG_LEVEL`):

- **silent** - No output
- **error** - Errors only
- **info** - Important information (default)
- **debug** - Detailed debugging information
- **verbose** - Very detailed information (includes every file operation)

Example log output:

```
2025-10-16 12:00:00 [INFO] Starting S3-to-FTP synchronization service
2025-10-16 12:00:00 [INFO] Cache initialized: type=bbolt
2025-10-16 12:00:00 [INFO] Source initialized: type=s3, bucket=my-bucket
2025-10-16 12:00:00 [INFO] Destination initialized: type=ftp, workers=5
2025-10-16 12:00:00 [INFO] Starting synchronization process...
2025-10-16 12:00:01 [INFO] PrepareForScan: processed=1000, SYNCED→TEMP_DELETED=500, NEW=200, ...
2025-10-16 12:00:05 [INFO] ProcessSource: total=800, new=50, TEMP_DELETED→SYNCED=450, ...
2025-10-16 12:00:06 [INFO] FinalizeStates: processed=1000, TEMP_DELETED→DELETED_ON_S3=50, ...
2025-10-16 12:00:10 [INFO] Found 50 NEW files to sync (total size: 125.50 MB)
2025-10-16 12:00:20 [INFO] Upload completed: 48 succeeded, 2 failed
2025-10-16 12:00:20 [INFO] Synchronization completed successfully
```

## Security Notes

- Credentials are accepted via environment variables or command-line flags
- Command-line flags may be visible in process lists - prefer environment variables for credentials
- FTP passwords are transmitted in plaintext unless using `--ftp-use-tls`
- Cache database contains file metadata but not file contents

## Performance Tuning

- **Worker count**: Start with 5-10, increase based on network capacity
- **Cache sync**: Disable (`--cache-no-sync`) for better performance if you can tolerate potential data loss
- **Listing strategy**: Use `by_level` for buckets with very deep directory structures
- **Timeout**: Increase for slow or unreliable networks

## Troubleshooting

### "Configuration validation error"
- Check that all required fields are provided
- Verify S3 credentials and bucket access
- Test FTP connection manually

### "Failed to connect to FTP server"
- Verify FTP host and port are correct
- Check firewall rules
- Try with `--ftp-use-tls` if server requires FTPS

### Slow performance
- Increase `--source-workers` and `--dest-workers`
- Use `--cache-no-sync` (if data loss is acceptable)
- Check network bandwidth and latency

### High memory usage
- Reduce worker counts
- Check for large files being transferred

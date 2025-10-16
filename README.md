# yomins-sync

High-performance S3 to FTP synchronization tool with intelligent caching and state management.

## What is yomins-sync?

**yomins-sync** is a Go-based utility designed to efficiently synchronize files from S3 (or S3-compatible storage) to FTP destinations. It's optimized for handling **millions of files** with minimal overhead by using a cache-based state machine to track synchronization status.

### Why Cache-Based Synchronization?

When dealing with large-scale file synchronization (millions of files), repeatedly checking each file's status on both source and destination becomes prohibitively expensive:

- **S3 listing is fast** - Listing millions of files from S3 can be done in minutes with optimized strategies
- **FTP verification is slow** - Checking existence and comparing each file on FTP would take hours or days
- **Incremental updates are common** - Most sync operations only need to process a small subset of changed files

**The solution:** A persistent cache (bbolt database) stores file metadata and synchronization state. On subsequent runs, the tool:

1. Quickly scans S3 to detect changes (compares against cached hashes)
2. Only uploads **new or modified** files to FTP
3. Only deletes files from FTP that were removed from S3
4. Skips verification of already-synchronized files

This approach reduces sync time from hours to minutes for incremental updates, even when managing 10+ million files.

## Performance

yomins-sync offers multiple S3 listing strategies optimized for different data layouts. Performance can vary dramatically based on your configuration and data distribution.

### Real-World Benchmarks

Dataset: **~9.5 million files** concentrated in second-level directories

| Workers | RPS Limit | Strategy            | Duration       | Speedup |
|---------|-----------|---------------------|----------------|---------|
| 10      | 100       | Recursive           | 14m 50s        | 1.0×    |
| 20      | 100       | By-Level (depth 2)  | 14m 07s        | 1.05×   |
| 20      | 300       | Recursive           | 7m 35s         | 1.96×   |
| 20      | 300       | By-Level (depth 2)  | **2m 27s**     | **6.06×** |

### Key Performance Features

- **Two listing strategies:**
  - **Recursive** (default) - Universal, safe for any hierarchy
  - **By-Level** - Optimized for known directory depths (5-6× faster on suitable datasets)

- **Configurable rate limiting** - Avoid throttling while maximizing throughput
- **Worker pool parallelism** - Concurrent S3 operations with tunable worker count
- **Streaming architecture** - Memory-efficient processing via Go channels
- **Batch cache updates** - Minimizes database I/O during large scans

For datasets with files concentrated at a specific directory depth, **By-Level strategy can be 5-6× faster** than recursive traversal when paired with appropriate RPS limits and worker counts.

See [source/README.md](source/README.md) for detailed strategy comparison and tuning guidance.

## S3 Compatibility

yomins-sync has been **tested extensively on Hetzner Object Storage** but is designed to work with all S3-compatible storage providers, including:

- **AWS S3**
- **Hetzner Object Storage** (tested)
- **MinIO**
- **DigitalOcean Spaces**
- **Backblaze B2** (with S3-compatible API)
- **Wasabi**
- Any other S3-compatible service

The tool uses the official AWS SDK v2 for Go, ensuring broad compatibility with S3-compatible APIs.

## Installation

### From Source

Requires Go 1.24.3 or later.

```bash
git clone https://github.com/olegkotsar/yomins-sync.git
cd yomins-sync
go mod download
go build -o yomins-sync ./cmd/main.go
```

### Using Makefile

Build for multiple architectures:

```bash
# Build all architectures
make

# Build for specific target
make build-linux-amd64
make build-linux-arm64

# Output in build/ directory
```

### Pre-built Binaries

Download pre-built binaries from the [Releases](https://github.com/olegkotsar/yomins-sync/releases) page (when available).

## Usage

### Basic Command

```bash
./yomins-sync \
  --source-type s3 \
  --s3-endpoint https://your-s3-endpoint.com \
  --s3-bucket your-bucket \
  --s3-access-key YOUR_ACCESS_KEY \
  --s3-secret-key YOUR_SECRET_KEY \
  --cache-path ./sync-cache.db \
  --worker-count 20 \
  --max-rps 300 \
  --listing-strategy by-level \
  --listing-level 2
```

### Configuration Options

**Source (S3) Settings:**
- `--source-type` - Source provider type (currently: `s3`)
- `--s3-endpoint` - S3 endpoint URL
- `--s3-region` - AWS region
- `--s3-bucket` - Bucket name
- `--s3-prefix` - Optional prefix filter
- `--s3-access-key` - Access key ID
- `--s3-secret-key` - Secret access key

**Cache Settings:**
- `--cache-type` - Cache provider (currently: `bbolt`)
- `--cache-path` - Path to cache database file

**Performance Tuning:**
- `--worker-count` - Number of concurrent workers (default: 10)
- `--max-rps` - Maximum requests per second to S3 (default: 100)
- `--listing-strategy` - S3 listing method: `recursive`, `by-level`, or `flat`
- `--listing-level` - Directory depth for by-level strategy (default: 2)
- `--timeout` - Request timeout in seconds (default: 30)
- `--max-retries` - Maximum retry attempts (default: 3)

**Logging:**
- `--log-level` - Log verbosity: `silent`, `error`, `info`, `debug`, `verbose` (default: `info`)

### Example: First Sync

```bash
# Initial sync of entire bucket (may take time depending on size)
./yomins-sync \
  --s3-endpoint https://s3.region.example.com \
  --s3-bucket my-files \
  --s3-access-key AKIA... \
  --s3-secret-key ... \
  --cache-path ./sync.db \
  --worker-count 20 \
  --max-rps 200
```

### Example: Incremental Sync

```bash
# Subsequent sync (much faster - only processes changes)
./yomins-sync \
  --s3-endpoint https://s3.region.example.com \
  --s3-bucket my-files \
  --s3-access-key AKIA... \
  --s3-secret-key ... \
  --cache-path ./sync.db
```

The cache file (`sync.db`) persists state between runs, enabling fast incremental updates.

## Synchronization Algorithm

yomins-sync uses a **state machine-based algorithm** to efficiently track and synchronize files:

### File States

- **NEW** - File requires upload to FTP
- **SYNCED** - File is synchronized (S3 and FTP are identical)
- **TEMP_DELETED** - Temporary state during scanning
- **DELETED_IN_SOURCE** - File was deleted from S3, requires removal from FTP
- **ERROR** - Synchronization failed (retried on next run)

### Algorithm Flow

#### 1. Preparation Phase

- Mark all `SYNCED` files as `TEMP_DELETED` (assume deleted until proven otherwise)
- Leave `NEW` and `DELETED_IN_SOURCE` files unchanged (may be from interrupted session)

#### 2. S3 Scan Phase

For each file found in S3:

- **Not in cache** → Mark as `NEW`
- **Status is TEMP_DELETED:**
  - Hash matches → Mark as `SYNCED` (unchanged)
  - Hash differs → Mark as `NEW` (modified)
- **Status is NEW** → Keep as `NEW` (update hash)
- **Status is DELETED_IN_SOURCE** → Mark as `NEW` (file returned)

#### 3. Finalization Phase

- Any remaining `TEMP_DELETED` files → Mark as `DELETED_IN_SOURCE` (confirmed deleted from S3)

#### 4. Synchronization Phase

- **NEW files** → Upload to FTP → Mark as `SYNCED` on success
- **DELETED_IN_SOURCE files** → Delete from FTP → Remove from cache on success
- **ERROR files** → Retry with exponential backoff

### State Transition Table

| Previous State       | Found in S3 | Hash Same? | New State            |
|----------------------|-------------|------------|----------------------|
| SYNCED               | ❌           | —          | TEMP_DELETED         |
| TEMP_DELETED         | ❌           | —          | DELETED_IN_SOURCE    |
| TEMP_DELETED         | ✅           | ✅          | SYNCED               |
| TEMP_DELETED         | ✅           | ❌          | NEW                  |
| DELETED_IN_SOURCE    | ✅           | —          | NEW                  |
| NEW                  | ✅           | —          | NEW (update hash)    |

This approach ensures:
- **Efficiency:** Only changed files are processed
- **Correctness:** Deleted files are properly tracked and cleaned up
- **Resilience:** Interrupted sessions can resume without issues

See [ALG.ENG.md](ALG.ENG.md) for complete algorithm details.

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

MIT License

Copyright (c) 2025 Oleg Kotsar

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

## Acknowledgments

- Built with [AWS SDK for Go v2](https://github.com/aws/aws-sdk-go-v2)
- Cache powered by [bbolt](https://github.com/etcd-io/bbolt)
- Rate limiting via [golang.org/x/time/rate](https://pkg.go.dev/golang.org/x/time/rate)

---

**Status:** Active development. FTP destination implementation in progress.

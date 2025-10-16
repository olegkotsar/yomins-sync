# Configuration Mapping

Complete mapping of all configuration values to their corresponding CLI flags and environment variables.

## General Configuration

| Config Field | CLI Flag | Environment Variable | Default |
|--------------|----------|---------------------|---------|
| AppConfig.DryRun | --dry-run | DRY_RUN | false |

## Logger Configuration

| Config Field | CLI Flag | Environment Variable | Default |
|--------------|----------|---------------------|---------|
| LoggerConfig.Level | --log-level | LOG_LEVEL | info |

## Cache Configuration

| Config Field | CLI Flag | Environment Variable | Default |
|--------------|----------|---------------------|---------|
| CacheConfig.CacheType | --cache-type | CACHE_TYPE | bbolt |
| BboltConfig.Path | --cache-path | CACHE_BBOLT_PATH | ./cache.db |
| BboltConfig.Bucket | --cache-bucket | CACHE_BBOLT_BUCKET | files |
| BboltConfig.NoSync | --cache-no-sync | CACHE_BBOLT_NO_SYNC | false |
| BboltConfig.Mode | *(not settable via CLI)* | *(not settable)* | 0600 |

## Source Configuration

| Config Field | CLI Flag | Environment Variable | Default |
|--------------|----------|---------------------|---------|
| SourceConfig.SourceType | --source-type | SOURCE_TYPE | s3 |
| CommonSourceConfig.WorkerCount | --source-workers | SOURCE_WORKER_COUNT | 5 |
| CommonSourceConfig.MaxQueueSize | --source-max-queue-size | SOURCE_MAX_QUEUE_SIZE | 1000 |
| CommonSourceConfig.TimeoutSeconds | --source-timeout | SOURCE_TIMEOUT_SECONDS | 30 |
| CommonSourceConfig.MaxRetries | --source-max-retries | SOURCE_MAX_RETRIES | 3 |
| CommonSourceConfig.MaxRPS | --source-max-rps | SOURCE_MAX_RPS | 0 |
| CommonSourceConfig.ListingStrategy | --listing-strategy | SOURCE_LISTING_STRATEGY | recursive |
| CommonSourceConfig.ListingLevel | --listing-level | SOURCE_LISTING_LEVEL | 0 |

## S3 Configuration

| Config Field | CLI Flag | Environment Variable | Required |
|--------------|----------|---------------------|----------|
| S3Config.Region | --s3-region | S3_REGION | Yes |
| S3Config.Bucket | --s3-bucket | S3_BUCKET | Yes |
| S3Config.AccessKeyID | --s3-access-key | S3_ACCESS_KEY_ID | Yes |
| S3Config.SecretAccessKey | --s3-secret-key | S3_SECRET_ACCESS_KEY | Yes |
| S3Config.Endpoint | --s3-endpoint | S3_ENDPOINT | Yes |

## Destination Configuration

| Config Field | CLI Flag | Environment Variable | Default |
|--------------|----------|---------------------|---------|
| DestinationConfig.DestinationType | --dest-type | DESTINATION_TYPE | ftp |
| CommonDestinationConfig.WorkerCount | --dest-workers | DESTINATION_WORKER_COUNT | 5 |
| CommonDestinationConfig.TimeoutSeconds | --dest-timeout | DESTINATION_TIMEOUT_SECONDS | 30 |
| CommonDestinationConfig.MaxRetries | --dest-max-retries | DESTINATION_MAX_RETRIES | 3 |

## FTP Configuration

| Config Field | CLI Flag | Environment Variable | Required/Default |
|--------------|----------|---------------------|------------------|
| FTPConfig.Host | --ftp-host | FTP_HOST | Required |
| FTPConfig.Port | --ftp-port | FTP_PORT | 21 |
| FTPConfig.Username | --ftp-username | FTP_USERNAME | Required |
| FTPConfig.Password | --ftp-password | FTP_PASSWORD | Optional |
| FTPConfig.BasePath | --ftp-base-path | FTP_BASE_PATH | / |
| FTPConfig.UseTLS | --ftp-use-tls | FTP_USE_TLS | false |

## Notes

- **BboltConfig.Mode** is the only configuration value that cannot be set via CLI or environment variable. It uses a hardcoded default of `0600` (owner read/write only), which is appropriate for a cache database file.
- All other configuration values can be set via either CLI flags or environment variables.
- CLI flags take precedence over environment variables.
- Values with `0` as default can be explicitly set to 0 (e.g., `--source-max-rps=0` means no rate limit).

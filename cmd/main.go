package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/olegkotsar/yomins-sync/cache"
	"github.com/olegkotsar/yomins-sync/config"
	"github.com/olegkotsar/yomins-sync/destination"
	"github.com/olegkotsar/yomins-sync/logger"
	"github.com/olegkotsar/yomins-sync/processor"
	"github.com/olegkotsar/yomins-sync/source"
)

func main() {
	// Define CLI flags
	var (
		// General flags
		dryRun = flag.Bool("dry-run", false, "Run in dry-run mode (no files will be uploaded) (env: DRY_RUN)")

		// Logger flags
		logLevel = flag.String("log-level", "", "Log level: silent, error, info, debug, verbose (env: LOG_LEVEL)")

		// Cache flags
		cacheType   = flag.String("cache-type", "", "Cache type: bbolt (env: CACHE_TYPE)")
		cachePath   = flag.String("cache-path", "", "Path to cache database (env: CACHE_BBOLT_PATH)")
		cacheBucket = flag.String("cache-bucket", "", "Cache bucket name (env: CACHE_BBOLT_BUCKET)")
		cacheNoSync = flag.Bool("cache-no-sync", false, "Disable fsync for cache (env: CACHE_BBOLT_NO_SYNC)")

		// Source flags
		sourceType         = flag.String("source-type", "", "Source type: s3 (env: SOURCE_TYPE)")
		sourceWorkerCount  = flag.Int("source-workers", 0, "Number of source workers (env: SOURCE_WORKER_COUNT)")
		sourceMaxQueueSize = flag.Int("source-max-queue-size", 0, "Max queue size for source operations (env: SOURCE_MAX_QUEUE_SIZE)")
		sourceMaxRetries   = flag.Int("source-max-retries", 0, "Max retries for source operations (env: SOURCE_MAX_RETRIES)")
		sourceTimeout      = flag.Int("source-timeout", 0, "Source operation timeout in seconds (env: SOURCE_TIMEOUT_SECONDS)")
		sourceMaxRPS       = flag.Int("source-max-rps", 0, "Max requests per second to source (0 = no limit) (env: SOURCE_MAX_RPS)")
		listingStrategy    = flag.String("listing-strategy", "", "Listing strategy: recursive, by_level (env: SOURCE_LISTING_STRATEGY)")
		listingLevel       = flag.Int("listing-level", 0, "Listing level for by_level strategy (env: SOURCE_LISTING_LEVEL)")

		// S3 flags
		s3Region    = flag.String("s3-region", "", "S3 region (env: S3_REGION)")
		s3Bucket    = flag.String("s3-bucket", "", "S3 bucket name (env: S3_BUCKET)")
		s3AccessKey = flag.String("s3-access-key", "", "S3 access key ID (env: S3_ACCESS_KEY_ID)")
		s3SecretKey = flag.String("s3-secret-key", "", "S3 secret access key (env: S3_SECRET_ACCESS_KEY)")
		s3Endpoint  = flag.String("s3-endpoint", "", "S3 endpoint URL (env: S3_ENDPOINT)")

		// Destination flags
		destType        = flag.String("dest-type", "", "Destination type: ftp (env: DESTINATION_TYPE)")
		destWorkerCount = flag.Int("dest-workers", 0, "Number of destination workers (env: DESTINATION_WORKER_COUNT)")
		destMaxRetries  = flag.Int("dest-max-retries", 0, "Max retries for destination operations (env: DESTINATION_MAX_RETRIES)")
		destTimeout     = flag.Int("dest-timeout", 0, "Destination operation timeout in seconds (env: DESTINATION_TIMEOUT_SECONDS)")

		// FTP flags
		ftpHost     = flag.String("ftp-host", "", "FTP server host (env: FTP_HOST)")
		ftpPort     = flag.Int("ftp-port", 0, "FTP server port (env: FTP_PORT)")
		ftpUsername = flag.String("ftp-username", "", "FTP username (env: FTP_USERNAME)")
		ftpPassword = flag.String("ftp-password", "", "FTP password (env: FTP_PASSWORD)")
		ftpBasePath = flag.String("ftp-base-path", "", "FTP base path (env: FTP_BASE_PATH)")
		ftpUseTLS   = flag.Bool("ftp-use-tls", false, "Use FTPS (env: FTP_USE_TLS)")

		// General flags
		showHelp = flag.Bool("help", false, "Show help message")
	)

	flag.Parse()

	if *showHelp {
		printHelp()
		os.Exit(0)
	}

	// Load base configuration from environment variables
	cfg, err := config.LoadFromEnv()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading config from environment: %v\n", err)
		os.Exit(1)
	}

	// Override with CLI flags if provided
	applyFlags(cfg, flagValues{
		dryRun:             *dryRun,
		logLevel:           *logLevel,
		cacheType:          *cacheType,
		cachePath:          *cachePath,
		cacheBucket:        *cacheBucket,
		cacheNoSync:        *cacheNoSync,
		sourceType:         *sourceType,
		sourceWorkerCount:  *sourceWorkerCount,
		sourceMaxQueueSize: *sourceMaxQueueSize,
		sourceMaxRetries:   *sourceMaxRetries,
		sourceTimeout:      *sourceTimeout,
		sourceMaxRPS:       *sourceMaxRPS,
		listingStrategy:    *listingStrategy,
		listingLevel:       *listingLevel,
		s3Region:           *s3Region,
		s3Bucket:           *s3Bucket,
		s3AccessKey:        *s3AccessKey,
		s3SecretKey:        *s3SecretKey,
		s3Endpoint:         *s3Endpoint,
		destType:           *destType,
		destWorkerCount:    *destWorkerCount,
		destMaxRetries:     *destMaxRetries,
		destTimeout:        *destTimeout,
		ftpHost:            *ftpHost,
		ftpPort:            *ftpPort,
		ftpUsername:        *ftpUsername,
		ftpPassword:        *ftpPassword,
		ftpBasePath:        *ftpBasePath,
		ftpUseTLS:          *ftpUseTLS,
	})

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Configuration validation error: %v\n", err)
		os.Exit(1)
	}

	// Initialize logger
	log := logger.NewLogger(&cfg.Logger)
	log.Info("Starting S3-to-FTP synchronization service")
	log.Debug("Configuration loaded and validated")

	// Initialize cache
	log.Debug("Initializing cache...")
	cacheProvider, err := createCache(&cfg.Cache)
	if err != nil {
		log.Error("Failed to create cache: %v", err)
		os.Exit(1)
	}
	defer func() {
		log.Debug("Closing cache...")
		if err := cacheProvider.Close(); err != nil {
			log.Error("Error closing cache: %v", err)
		}
	}()
	log.Info("Cache initialized: type=%s", cfg.Cache.CacheType)

	// Initialize source
	log.Debug("Initializing source...")
	sourceProvider, err := createSource(&cfg.Source)
	if err != nil {
		log.Error("Failed to create source: %v", err)
		os.Exit(1)
	}
	log.Info("Source initialized: type=%s, bucket=%s", cfg.Source.SourceType, cfg.Source.S3.Bucket)

	// Initialize destination
	log.Debug("Initializing destination...")
	destProvider, err := createDestination(&cfg.Destination)
	if err != nil {
		log.Error("Failed to create destination: %v", err)
		os.Exit(1)
	}
	defer func() {
		log.Debug("Closing destination...")
		if err := destProvider.Close(); err != nil {
			log.Error("Error closing destination: %v", err)
		}
	}()
	log.Info("Destination initialized: type=%s, workers=%d", cfg.Destination.DestinationType, destProvider.GetWorkerCount())

	// Create processor
	log.Debug("Creating processor...")
	if cfg.DryRun {
		log.Info("Running in DRY-RUN mode - no files will be uploaded")
	}
	runner := processor.NewRunner(cacheProvider, sourceProvider, destProvider, log, cfg.DryRun)

	// Set up signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Run processor in a goroutine
	errChan := make(chan error, 1)
	go func() {
		log.Info("Starting synchronization process...")
		errChan <- runner.Run(ctx)
	}()

	// Wait for completion or interruption
	select {
	case err := <-errChan:
		if err != nil {
			log.Error("Synchronization failed: %v", err)
			os.Exit(1)
		}
		log.Info("Synchronization completed successfully")
	case sig := <-sigChan:
		log.Info("Received signal %v, initiating graceful shutdown...", sig)
		cancel()

		// Wait for the processor to finish
		err := <-errChan
		if err != nil && err != context.Canceled {
			log.Error("Error during shutdown: %v", err)
			os.Exit(1)
		}
		log.Info("Shutdown completed")
	}
}

type flagValues struct {
	dryRun             bool
	logLevel           string
	cacheType          string
	cachePath          string
	cacheBucket        string
	cacheNoSync        bool
	sourceType         string
	sourceWorkerCount  int
	sourceMaxQueueSize int
	sourceMaxRetries   int
	sourceTimeout      int
	sourceMaxRPS       int
	listingStrategy    string
	listingLevel       int
	s3Region           string
	s3Bucket           string
	s3AccessKey        string
	s3SecretKey        string
	s3Endpoint         string
	destType           string
	destWorkerCount    int
	destMaxRetries     int
	destTimeout        int
	ftpHost            string
	ftpPort            int
	ftpUsername        string
	ftpPassword        string
	ftpBasePath        string
	ftpUseTLS          bool
}

func applyFlags(cfg *config.AppConfig, flags flagValues) {
	// General
	if flag.Lookup("dry-run").Value.String() == "true" {
		cfg.DryRun = flags.dryRun
	}

	// Logger
	if flags.logLevel != "" {
		cfg.Logger.Level = config.LogLevel(flags.logLevel)
	}

	// Cache
	if flags.cacheType != "" {
		cfg.Cache.CacheType = config.CacheType(flags.cacheType)
	}
	if flags.cachePath != "" {
		cfg.Cache.Bbolt.Path = flags.cachePath
	}
	if flags.cacheBucket != "" {
		cfg.Cache.Bbolt.Bucket = flags.cacheBucket
	}
	if flag.Lookup("cache-no-sync").Value.String() == "true" {
		cfg.Cache.Bbolt.NoSync = flags.cacheNoSync
	}

	// Source
	if flags.sourceType != "" {
		cfg.Source.SourceType = config.SourceType(flags.sourceType)
	}
	if flags.sourceWorkerCount > 0 {
		cfg.Source.Common.WorkerCount = flags.sourceWorkerCount
	}
	if flags.sourceMaxQueueSize > 0 {
		cfg.Source.Common.MaxQueueSize = flags.sourceMaxQueueSize
	}
	if flags.sourceMaxRetries > 0 {
		cfg.Source.Common.MaxRetries = flags.sourceMaxRetries
	}
	if flags.sourceTimeout > 0 {
		cfg.Source.Common.TimeoutSeconds = flags.sourceTimeout
	}
	if flags.sourceMaxRPS >= 0 {
		// Allow 0 (no limit) to be explicitly set
		cfg.Source.Common.MaxRPS = flags.sourceMaxRPS
	}
	if flags.listingStrategy != "" {
		cfg.Source.Common.ListingStrategy = config.ListingStrategy(flags.listingStrategy)
	}
	if flags.listingLevel > 0 {
		cfg.Source.Common.ListingLevel = flags.listingLevel
	}

	// S3
	if flags.s3Region != "" {
		cfg.Source.S3.Region = flags.s3Region
	}
	if flags.s3Bucket != "" {
		cfg.Source.S3.Bucket = flags.s3Bucket
	}
	if flags.s3AccessKey != "" {
		cfg.Source.S3.AccessKeyID = flags.s3AccessKey
	}
	if flags.s3SecretKey != "" {
		cfg.Source.S3.SecretAccessKey = flags.s3SecretKey
	}
	if flags.s3Endpoint != "" {
		cfg.Source.S3.Endpoint = flags.s3Endpoint
	}

	// Destination
	if flags.destType != "" {
		cfg.Destination.DestinationType = config.DestinationType(flags.destType)
	}
	if flags.destWorkerCount > 0 {
		cfg.Destination.Common.WorkerCount = flags.destWorkerCount
	}
	if flags.destMaxRetries > 0 {
		cfg.Destination.Common.MaxRetries = flags.destMaxRetries
	}
	if flags.destTimeout > 0 {
		cfg.Destination.Common.TimeoutSeconds = flags.destTimeout
	}

	// FTP
	if flags.ftpHost != "" {
		cfg.Destination.FTP.Host = flags.ftpHost
	}
	if flags.ftpPort > 0 {
		cfg.Destination.FTP.Port = flags.ftpPort
	}
	if flags.ftpUsername != "" {
		cfg.Destination.FTP.Username = flags.ftpUsername
	}
	if flags.ftpPassword != "" {
		cfg.Destination.FTP.Password = flags.ftpPassword
	}
	if flags.ftpBasePath != "" {
		cfg.Destination.FTP.BasePath = flags.ftpBasePath
	}
	if flag.Lookup("ftp-use-tls").Value.String() == "true" {
		cfg.Destination.FTP.UseTLS = flags.ftpUseTLS
	}
}

func createCache(cfg *config.CacheConfig) (cache.CacheProvider, error) {
	switch cfg.CacheType {
	case config.CacheTypeBbolt:
		return cache.NewBboltCache(cfg.Bbolt)
	default:
		return nil, fmt.Errorf("unsupported cache type: %s", cfg.CacheType)
	}
}

func createSource(cfg *config.SourceConfig) (source.SourceProvider, error) {
	switch cfg.SourceType {
	case config.SourceTypeS3:
		return source.NewS3Source(cfg.S3, &cfg.Common)
	default:
		return nil, fmt.Errorf("unsupported source type: %s", cfg.SourceType)
	}
}

func createDestination(cfg *config.DestinationConfig) (destination.DestinationProvider, error) {
	return destination.CreateDestination(cfg)
}

func printHelp() {
	fmt.Println("S3-to-FTP Synchronization Tool")
	fmt.Println()
	fmt.Println("Usage: yomins-sync [options]")
	fmt.Println()
	fmt.Println("Configuration can be provided via environment variables or command-line flags.")
	fmt.Println("Command-line flags take precedence over environment variables.")
	fmt.Println()
	fmt.Println("Options:")
	flag.PrintDefaults()
	fmt.Println()
	fmt.Println("Example:")
	fmt.Println("  yomins-sync --s3-bucket=my-bucket --s3-region=us-east-1 --ftp-host=ftp.example.com")
	fmt.Println()
	fmt.Println("Environment Variables:")
	fmt.Println("  DRY_RUN                  - Run in dry-run mode (true/false)")
	fmt.Println("  LOG_LEVEL                - Log level (silent, error, info, debug, verbose)")
	fmt.Println("  CACHE_TYPE               - Cache type (bbolt)")
	fmt.Println("  CACHE_BBOLT_PATH         - Path to cache database")
	fmt.Println("  CACHE_BBOLT_BUCKET       - Cache bucket name")
	fmt.Println("  SOURCE_TYPE              - Source type (s3)")
	fmt.Println("  SOURCE_WORKER_COUNT      - Number of source workers")
	fmt.Println("  SOURCE_MAX_QUEUE_SIZE    - Max queue size for source operations")
	fmt.Println("  SOURCE_MAX_RETRIES       - Max retries for source operations")
	fmt.Println("  SOURCE_TIMEOUT_SECONDS   - Source operation timeout in seconds")
	fmt.Println("  SOURCE_MAX_RPS           - Max requests per second to source (0 = no limit)")
	fmt.Println("  SOURCE_LISTING_STRATEGY  - Listing strategy (recursive, by_level)")
	fmt.Println("  SOURCE_LISTING_LEVEL     - Listing level for by_level strategy")
	fmt.Println("  S3_REGION                - S3 region")
	fmt.Println("  S3_BUCKET                - S3 bucket name")
	fmt.Println("  S3_ACCESS_KEY_ID         - S3 access key ID")
	fmt.Println("  S3_SECRET_ACCESS_KEY     - S3 secret access key")
	fmt.Println("  S3_ENDPOINT              - S3 endpoint URL")
	fmt.Println("  DESTINATION_TYPE         - Destination type (ftp)")
	fmt.Println("  DESTINATION_WORKER_COUNT - Number of destination workers")
	fmt.Println("  FTP_HOST                 - FTP server host")
	fmt.Println("  FTP_PORT                 - FTP server port")
	fmt.Println("  FTP_USERNAME             - FTP username")
	fmt.Println("  FTP_PASSWORD             - FTP password")
	fmt.Println("  FTP_BASE_PATH            - FTP base path")
	fmt.Println("  FTP_USE_TLS              - Use FTPS (true/false)")
}

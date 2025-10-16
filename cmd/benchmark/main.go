package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/olegkotsar/yomins-sync/config"
	"github.com/olegkotsar/yomins-sync/model"
	"github.com/olegkotsar/yomins-sync/source"
)

func main() {
	ctx := context.Background()

	// Creating configuration from ENV
	s3cfg := &config.S3Config{
		Endpoint:        os.Getenv("S3_ENDPOINT"),
		Bucket:          os.Getenv("S3_BUCKET"),
		AccessKeyID:     os.Getenv("S3_ACCESS_KEY"),
		SecretAccessKey: os.Getenv("S3_SECRET_KEY"),
	}

	commonCfg := &config.CommonSourceConfig{
		WorkerCount:     mustGetEnvInt("S3_WORKERS", 20),
		MaxQueueSize:    mustGetEnvInt("S3_QUEUE_SIZE", 1000),
		TimeoutSeconds:  mustGetEnvInt("S3_TIMEOUT", 30),
		MaxRetries:      mustGetEnvInt("S3_RETRIES", 3),
		MaxRPS:          mustGetEnvInt("S3_MAX_RPS", 300),
		ListingStrategy: config.ListingStrategyByLevel,
		ListingLevel:    0,
	}

	s3source, err := source.NewS3Source(s3cfg, commonCfg)
	if err != nil {
		log.Fatalf("Failed to create S3 source: %v", err)
	}

	//start := time.Now()
	//filesCh, errCh := s3source.StreamObjectsRecursive(ctx)
	//count := consume(filesCh, errCh)
	//fmt.Printf("Recursive: %d files in %s\n", count, time.Since(start))

	start := time.Now()
	filesCh, errCh := s3source.ListObjectsStream(ctx)
	count := consume(filesCh, errCh)
	fmt.Printf("By Level: %d files in %s\n", count, time.Since(start))
}

// mustGetEnvInt tries to parse an environment variable as int, returns default if not set or invalid
func mustGetEnvInt(key string, def int) int {
	val := os.Getenv(key)
	if val == "" {
		return def
	}
	i, err := strconv.Atoi(val)
	if err != nil {
		log.Printf("Invalid int value for %s: %v. Using default: %d", key, err, def)
		return def
	}
	return i
}

func consume(filesCh <-chan model.RemoteFile, errCh <-chan error) int {
	count := 0
	for {
		select {
		case _, ok := <-filesCh:
			if !ok {
				filesCh = nil
			} else {
				count++
			}
		case err, ok := <-errCh:
			if ok && err != nil {
				log.Fatalf("stream error: %v", err)
			}
			errCh = nil
		}
		if filesCh == nil && errCh == nil {
			break
		}
	}
	return count
}

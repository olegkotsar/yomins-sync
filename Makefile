.PHONY: all build-linux-amd64 build-linux-arm64 clean test

# Binary name
BINARY_NAME=yomins-sync

# Build output directory
BUILD_DIR=build

# Version info (can be overridden: make VERSION=1.0.0)
VERSION?=$(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS=-ldflags "-X main.version=$(VERSION)"

# Default target
all: build-linux-amd64 build-linux-arm64

# Create build directory
$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

# Build for Linux AMD64
build-linux-amd64: $(BUILD_DIR)
	@echo "Building $(BINARY_NAME) for Linux AMD64..."
	GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-linux-amd64 ./cmd/main.go
	@echo "Built: $(BUILD_DIR)/$(BINARY_NAME)-linux-amd64"

# Build for Linux ARM64
build-linux-arm64: $(BUILD_DIR)
	@echo "Building $(BINARY_NAME) for Linux ARM64..."
	GOOS=linux GOARCH=arm64 go build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-linux-arm64 ./cmd/main.go
	@echo "Built: $(BUILD_DIR)/$(BINARY_NAME)-linux-arm64"

# Run tests
test:
	go test ./...

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	rm -rf $(BUILD_DIR)
	@echo "Clean complete"

# Show help
help:
	@echo "Available targets:"
	@echo "  all                - Build all architectures (default)"
	@echo "  build-linux-amd64  - Build for Linux AMD64"
	@echo "  build-linux-arm64  - Build for Linux ARM64"
	@echo "  test               - Run all tests"
	@echo "  clean              - Remove build artifacts"
	@echo "  help               - Show this help message"
	@echo ""
	@echo "Variables:"
	@echo "  VERSION            - Set version (default: git tag or 'dev')"
	@echo ""
	@echo "Examples:"
	@echo "  make                       # Build all architectures"
	@echo "  make build-linux-amd64     # Build only AMD64"
	@echo "  make VERSION=1.2.3         # Build with specific version"
	@echo "  make clean                 # Clean build directory"

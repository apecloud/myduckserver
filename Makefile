# Project name (name of the executable)
BINARY_NAME := myduckserver

# Go source directory
SRC_DIR := .

# Go source files
SRC_FILES := $(wildcard $(SRC_DIR)/*.go)

# Version information
VERSION := $(shell git describe --tags --always --dirty)
BUILD_TIME := $(shell date +%Y-%m-%dT%H:%M:%S%z)
GIT_COMMIT := $(shell git rev-parse HEAD)

# Docker image information
IMAGE_NAME ?= myduck
IMAGE_TAG ?= latest
REGISTRY ?= apecloud

# Compilation flags
LDFLAGS := -X 'main.Version=$(VERSION)' -X 'main.BuildTime=$(BUILD_TIME)' -X 'main.GitCommit=$(GIT_COMMIT)'

# Build target
$(BINARY_NAME): $(SRC_FILES)
	go build -ldflags "$(LDFLAGS)" -o $(BINARY_NAME) $(SRC_DIR)

# Default target
.PHONY: all build
all: build
build: $(BINARY_NAME)

# Clean target
.PHONY: clean
clean:
	rm -f $(BINARY_NAME)

# Run target
.PHONY: run
run: build
	./$(BINARY_NAME)

# Build and push for multiple architectures using Docker Buildx
.PHONY: push-image
	docker buildx build --platform linux/amd64,linux/arm64 -t $(REGISTRY)/$(IMAGE_NAME):$(IMAGE_TAG) --push .

# Build image locally using Docker Buildx (for testing)
.PHONY: build-image-local
build-image-local:
	docker buildx build -t $(IMAGE_NAME):$(IMAGE_TAG) --load .

.PHONY: run-image
run-image:
	docker run -d -p 3306:3306 -e MYSQL_CHARSET=utf8  $(IMAGE_NAME):$(IMAGE_TAG)

# Stop and remove the container
.PHONY: stop-image
stop-image:
	docker stop $$(docker ps -q --filter ancestor=$(IMAGE_NAME):$(IMAGE_TAG)) || true
	docker rm $$(docker ps -a -q --filter ancestor=$(IMAGE_NAME):$(IMAGE_TAG)) || true

# Remove the locally built image
.PHONY: clean-image
clean-image:
	docker rmi $(IMAGE_NAME):$(IMAGE_TAG) || true

# Test target
.PHONY: test-full test cover

TEST_CMD = go test -cover -coverprofile cover.out 
SHOW_TOTAL_COVERAGE = go tool cover -func=cover.out | grep total | awk '{print "Total Coverage: " $$3 " (run '\''make cover'\'' for details)"}'

test-full:
	$(TEST_CMD) ./...  | grep -v './transpiler'
	@$(SHOW_TOTAL_COVERAGE)

test:
	$(TEST_CMD) $(shell go list ./... | grep -v './binlogreplication')
	@$(SHOW_TOTAL_COVERAGE)

cover:
	go tool cover -html=cover.out

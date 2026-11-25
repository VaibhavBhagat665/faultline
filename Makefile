# Copyright 2026 Vaibhav Bhagat
# SPDX-License-Identifier: Apache-2.0

.PHONY: all lint test race fuzz-smoke bench clean tools help

# Tool versions
GOLANGCI_LINT_VERSION := v1.62.2

# Directories
BIN_DIR := bin

all: lint test

help:
	@echo "Faultline Build Targets:"
	@echo "  make lint        - Run golangci-lint"
	@echo "  make test        - Run all tests"
	@echo "  make race        - Run tests with race detector"
	@echo "  make fuzz-smoke  - Run fuzz tests for 30s each"
	@echo "  make bench       - Run benchmarks"
	@echo "  make clean       - Remove build artifacts"
	@echo "  make tools       - Install development tools"

tools:
	@echo "Installing golangci-lint $(GOLANGCI_LINT_VERSION)..."
	@curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b . $(GOLANGCI_LINT_VERSION)

lint:
	@echo "Running golangci-lint..."
	@golangci-lint run --config .golangci.yml ./...

test:
	@echo "Running tests (short mode)..."
	@go test -short -count=1 ./...

test-full:
	@echo "Running all tests except 100k soak..."
	@go test -count=1 -skip TestSoakClean -timeout 60m ./...

soak:
	@echo "Running 100k seed soak test (FAULTLINE_SOAK_SEEDS=$(FAULTLINE_SOAK_SEEDS:-100000))..."
	@FAULTLINE_SOAK_SEEDS=$${FAULTLINE_SOAK_SEEDS:-100000} go test -count=1 -run TestSoakClean -timeout 4h ./internal/integration/

race:
	@echo "Running tests with race detector..."
	@go test -short -race -count=1 ./...

fuzz-smoke:
	@echo "Running fuzz smoke tests (30s each)..."
	@for target in $$(go test -list=. ./... | grep '^Fuzz'); do \
		echo "Fuzzing $$target for 30s..."; \
		go test -fuzz=$$target -fuzztime=30s -run=^$$ ./...; \
	done

bench:
	@echo "Running benchmarks..."
	@go test -bench=. -benchmem ./...

clean:
	@echo "Cleaning..."
	@rm -rf $(BIN_DIR) failures/
	@go clean -testcache -fuzzcache

fmt:
	@echo "Formatting code..."
	@go fmt ./...
	@gofumpt -l -w . 2>/dev/null || true

GO ?= go
export CGO_ENABLED ?= 0
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

.PHONY: all build test lint fmt vet cover check run image clean

all: lint test

build:
	$(GO) build -trimpath -ldflags "-s -w -X main.version=$(VERSION)" -o bin/statuspage ./cmd/statuspage

## test: unit tests with the race detector
test:
	$(GO) test -race -count=1 ./...

cover:
	$(GO) test -count=1 -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out | tail -1

lint: fmt vet

fmt:
	@unformatted=$$(gofmt -l . | grep -v '^$$' || true); \
	if [ -n "$$unformatted" ]; then echo "gofmt needed:"; echo "$$unformatted"; exit 1; fi

vet:
	$(GO) vet ./...

## check: validate a config file without starting the service
check: build
	./bin/statuspage -config config.yaml -check-config

## run: run against config.yaml
run:
	$(GO) run ./cmd/statuspage -config config.yaml

image:
	docker build --build-arg VERSION=$(VERSION) -t status-page:$(VERSION) .

clean:
	rm -rf bin coverage.out statuspage.db*

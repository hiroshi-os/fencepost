.PHONY: all build test lint proto chaos chaos-small bench clean

SEED ?= 1
JOBS ?= 20000
WORKERS ?= 8
NOFENCE ?= 0

all: lint test build

proto:
	buf generate

lint:
	buf lint
	go vet ./...
	staticcheck ./...

test:
	go test -race -v -timeout 10m ./...

build:
	go build -o bin/fencepostd ./cmd/fencepostd
	go build -o bin/sink ./cmd/sink
	go build -o bin/demoworker ./cmd/demoworker
	go build -o bin/chaos ./cmd/chaos
	go build -o bin/bench ./cmd/bench

chaos:
ifeq ($(NOFENCE),1)
	go run ./cmd/chaos -seed $(SEED) -jobs $(JOBS) -workers $(WORKERS) -no-fencing
else
	go run ./cmd/chaos -seed $(SEED) -jobs $(JOBS) -workers $(WORKERS)
endif

chaos-small:
	go run ./cmd/chaos -smoke -seed 1 -jobs 1000 -workers 4

bench-enqueue:
	go run ./cmd/bench -mode enqueue -clients 1,8,32 -d 15s

bench-e2e:
	go run ./cmd/bench -mode e2e -workers 1,4,16,64 -lease-batch 1,10,50 -jobs 10000

clean:
	rm -rf bin/

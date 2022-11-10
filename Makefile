all: test lint

check-repository-unchanged:
	./_tools/check_repository_unchanged.sh

tools:
	 go install honnef.co/go/tools/cmd/staticcheck@latest
	 go install golang.org/x/tools/cmd/goimports@latest

fmt:
	goimports -w .

tidy:
	go mod tidy

dependencies:
	go get ./...

lint: 
	go vet ./...
	staticcheck ./...

test:
	go test -race ./...

test_short:
	go test -parallel 20 ./... -short

test_race:
	go test ./... -short -race

test_stress:
	go test -tags=stress -parallel 30 -timeout=15m ./...

up:

wait:

build:
	go build ./...

.PHONY: all ci check-repository-unchanged tools fmt tidy dependencies lint test test-short

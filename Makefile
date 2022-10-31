all: test lint

ci: tools dependencies test lint tidy fmt check-repository-unchanged 

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

test-short:
	go test -short -race ./...

up:

wait:

build:
	go build ./...

.PHONY: all ci check-repository-unchanged tools fmt tidy dependencies lint test test-short

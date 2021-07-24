all: test lint

ci: tools dependencies fmt check-repository-unchanged test lint

check-repository-unchanged:
	./_tools/check_repository_unchanged.sh

tools:
	 go install honnef.co/go/tools/cmd/staticcheck@latest
	 go install golang.org/x/tools/cmd/goimports@latest

fmt:
	goimports -w .

dependencies:
	go get ./...

lint: 
	go vet ./...
	staticcheck ./...

test:
	go test -race ./...

test-short:
	go test -short -race ./...

.PHONY: all ci check-repository-unchanged tools fmt dependencies lint test test-short

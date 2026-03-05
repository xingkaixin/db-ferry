set shell := ["bash", "-euo", "pipefail", "-c"]

go_cmd := "mise x go@1.25.2 -- go"
golangci_lint_cmd := "mise x golangci-lint@2.10.1 -- golangci-lint"

default:
    @just --list

fmt:
    @files=$(find . -type f -name '*.go' -not -path './.cache/*' -not -path './dist/*'); \
    if [ -n "$files" ]; then \
        gofmt -w $files; \
    fi

fmt-check:
    @files=$(find . -type f -name '*.go' -not -path './.cache/*' -not -path './dist/*'); \
    if [ -z "$files" ]; then \
        exit 0; \
    fi; \
    unformatted=$(gofmt -l $files); \
    if [ -n "$unformatted" ]; then \
        echo "以下 Go 文件未格式化:"; \
        echo "$unformatted"; \
        exit 1; \
    fi

lint:
    mkdir -p .cache/go-build
    mkdir -p .cache/golangci-lint
    GOCACHE=$(pwd)/.cache/go-build GOLANGCI_LINT_CACHE=$(pwd)/.cache/golangci-lint {{golangci_lint_cmd}} run ./...

test:
    mkdir -p .cache/go-build
    GOCACHE=$(pwd)/.cache/go-build {{go_cmd}} test ./...

build:
    mkdir -p .cache/go-build
    CGO_ENABLED=1 GOCACHE=$(pwd)/.cache/go-build {{go_cmd}} build .

check: fmt-check lint test

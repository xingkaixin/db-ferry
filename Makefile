# Project settings
PROJECT_NAME := db-ferry
DIST_DIR := dist

ZIG := $(shell command -v zig 2>/dev/null)
ifeq ($(ZIG),)
ZIG := zig
endif

ZIG_EXISTS := $(shell command -v $(ZIG) >/dev/null 2>&1 && echo yes)

MINGW_PREFIX ?= x86_64-w64-mingw32
WINDOWS_CC := $(MINGW_PREFIX)-gcc
WINDOWS_CXX := $(MINGW_PREFIX)-g++

.PHONY: all clean build mac-universal linux-amd64 windows-amd64

all: build

build: mac-universal linux-amd64 windows-amd64

$(DIST_DIR):
	mkdir -p $(DIST_DIR)

mac-universal: $(DIST_DIR)
	@echo "Building macOS universal binary..."
	CGO_ENABLED=1 GOOS=darwin GOARCH=arm64 go build -o $(DIST_DIR)/$(PROJECT_NAME)-darwin-arm64 .
	CGO_ENABLED=1 GOOS=darwin GOARCH=amd64 go build -o $(DIST_DIR)/$(PROJECT_NAME)-darwin-amd64 .
	lipo -create -output $(DIST_DIR)/$(PROJECT_NAME)-darwin-universal $(DIST_DIR)/$(PROJECT_NAME)-darwin-arm64 $(DIST_DIR)/$(PROJECT_NAME)-darwin-amd64
	rm -f $(DIST_DIR)/$(PROJECT_NAME)-darwin-arm64 $(DIST_DIR)/$(PROJECT_NAME)-darwin-amd64

linux-amd64: $(DIST_DIR)
	@echo "Building Linux amd64 binary..."
	@if [ "$(ZIG_EXISTS)" != "yes" ]; then \
		echo "Error: zig compiler is required for Linux cross compilation. Install zig and retry."; \
		exit 1; \
	fi
	CC="$(ZIG) cc -target x86_64-linux-gnu" \
	CXX="$(ZIG) c++ -target x86_64-linux-gnu" \
	CGO_ENABLED=1 GOOS=linux GOARCH=amd64 \
	go build -tags sqlite_omit_load_extension \
		-o $(DIST_DIR)/$(PROJECT_NAME)-linux-amd64 .

windows-amd64: $(DIST_DIR)
	@echo "Building Windows amd64 binary..."
	@if ! command -v $(WINDOWS_CC) >/dev/null 2>&1; then \
		echo "Error: $(WINDOWS_CC) not found. Install mingw-w64 (e.g. brew install mingw-w64) or override MINGW_PREFIX."; \
		exit 1; \
	fi
	@if ! command -v $(WINDOWS_CXX) >/dev/null 2>&1; then \
		echo "Error: $(WINDOWS_CXX) not found. Install mingw-w64 (e.g. brew install mingw-w64) or override MINGW_PREFIX."; \
		exit 1; \
	fi
	CC="$(WINDOWS_CC)" \
	CXX="$(WINDOWS_CXX)" \
	CGO_ENABLED=1 GOOS=windows GOARCH=amd64 \
	go build -tags sqlite_omit_load_extension \
		-o $(DIST_DIR)/$(PROJECT_NAME)-windows-amd64.exe .

clean:
	rm -rf $(DIST_DIR)

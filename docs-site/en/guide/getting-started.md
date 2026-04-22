# Getting Started

## Install from npm

`db-ferry` can be installed globally or executed directly with `npx`:

```bash
npm install -g db-ferry
db-ferry -version

npx db-ferry -version
```

Supported npm binary packages:

| Platform | Arch | npm package | Notes |
|----------|------|-------------|-------|
| Linux | x64 | `db-ferry-linux-x64` | Included via main `db-ferry` package |
| Linux | arm64 | `db-ferry-linux-arm64` | Included via main `db-ferry` package |
| macOS | x64 | `db-ferry-darwin-x64` | Included via main `db-ferry` package |
| macOS | arm64 | `db-ferry-darwin-arm64` | Included via main `db-ferry` package |
| Windows | x64 | `db-ferry-windows-x64` | Included via main `db-ferry` package |

> Windows arm64 npm binaries are not published yet. DuckDB remains unsupported on Windows builds.

## Build from Source

1. Clone the repository:

   ```bash
   git clone <repository-url>
   cd db-ferry
   ```

2. Install dependencies:

   ```bash
   go mod tidy
   ```

3. Build the application:

   ```bash
   go build -o db-ferry
   ```

   > DuckDB support relies on CGO. Ensure `CGO_ENABLED=1` and the default C toolchain (clang on macOS, gcc/clang on Linux) are available when building binaries that include DuckDB aliases.

## First Migration

1. Generate a configuration file:

   ```bash
   db-ferry config init
   ```

2. Edit `task.toml` with your database connections and tasks.

3. Run the migration:

   ```bash
   db-ferry
   ```

## Development Setup

If you plan to contribute or modify the code, the project provides a `justfile` with common quality checks:

```bash
# list all recipes
just

# format all go files
just fmt

# check formatting
just fmt-check

# run lint checks (golangci-lint)
just lint

# run tests
just test

# run coverage gate
just test-cover

# build all packages
just build

# run full local quality gate
just check
```

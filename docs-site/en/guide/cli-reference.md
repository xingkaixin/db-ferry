# CLI Reference

## Commands

### Run migration

```bash
# Run with default task.toml
db-ferry

# Specify an alternate configuration file
db-ferry -config ./configs/task.toml

# Enable verbose logging
db-ferry -v

# Show version information
db-ferry -version
```

### Config init

```bash
db-ferry config init
```

Interactive configuration wizard that creates `task.toml` in the current directory. Walks through engine selection, connection details, and table choices. Falls back to the built-in sample if non-interactive. Fails if the file already exists.

### Diff

```bash
db-ferry diff -task employees
```

Compare source and target data for a given task.

Flags:
- `-task` (required): Task table name
- `-keys`: Comparison keys
- `-where`: WHERE clause filter
- `-limit`: Row limit
- `-output`: Output file path
- `-format`: Output format — `json`, `csv`, or `html`

### MCP Server

```bash
db-ferry mcp serve
```

Start an MCP server with 5 agent-native tools for AI integration.

## Global Flags

| Flag | Description |
|------|-------------|
| `-config` | Path to the TOML configuration file (default: `task.toml`) |
| `-v` | Enable verbose logging with file/line prefixes |
| `-version` | Print build version and exit |
| `-sse-port` | Start an SSE server (e.g. `:8080`) that streams real-time task progress via `/events` and exposes current status via `/status`; supports CORS for local frontend development |

package processor

import (
	"strings"
	"testing"
	"time"

	"db-ferry/config"
	"db-ferry/database"
)

func TestNewPluginEngine(t *testing.T) {
	t.Run("empty engine returns nil", func(t *testing.T) {
		eng, err := newPluginEngine(config.PluginConfig{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if eng != nil {
			t.Fatalf("expected nil engine")
		}
	})

	t.Run("unsupported engine returns error", func(t *testing.T) {
		_, err := newPluginEngine(config.PluginConfig{Engine: "python", Script: "x"})
		if err == nil || !strings.Contains(err.Error(), "unsupported plugin engine") {
			t.Fatalf("expected unsupported engine error, got %v", err)
		}
	})
}

func TestLuaTransformer(t *testing.T) {
	columns := []database.ColumnMetadata{
		{Name: "id"},
		{Name: "name"},
		{Name: "value"},
	}

	t.Run("transform modifies row", func(t *testing.T) {
		script := `
			function transform(row)
				row.name = row.name .. "_suffix"
				row.value = row.value * 2
				return row
			end
		`
		tr, err := newLuaTransformer(config.PluginConfig{Engine: config.PluginEngineLua, Script: script, TimeoutMs: 1000})
		if err != nil {
			t.Fatalf("newLuaTransformer() error = %v", err)
		}
		defer tr.close()

		row := []any{1, "alice", 10.5}
		result, err := tr.transform(row, columns)
		if err != nil {
			t.Fatalf("transform() error = %v", err)
		}
		if result[0] != float64(1) {
			t.Fatalf("expected id unchanged, got %v", result[0])
		}
		if result[1] != "alice_suffix" {
			t.Fatalf("expected name modified, got %v", result[1])
		}
		if result[2] != 21.0 {
			t.Fatalf("expected value doubled, got %v", result[2])
		}
	})

	t.Run("transform with nil values", func(t *testing.T) {
		script := `
			function transform(row)
				if row.name == nil then
					row.name = "default"
				end
				return row
			end
		`
		tr, err := newLuaTransformer(config.PluginConfig{Engine: config.PluginEngineLua, Script: script, TimeoutMs: 1000})
		if err != nil {
			t.Fatalf("newLuaTransformer() error = %v", err)
		}
		defer tr.close()

		row := []any{1, nil, 10.5}
		result, err := tr.transform(row, columns)
		if err != nil {
			t.Fatalf("transform() error = %v", err)
		}
		if result[1] != "default" {
			t.Fatalf("expected nil replaced, got %v", result[1])
		}
	})

	t.Run("invalid script fails at compile", func(t *testing.T) {
		_, err := newLuaTransformer(config.PluginConfig{Engine: config.PluginEngineLua, Script: "invalid lua!!!", TimeoutMs: 1000})
		if err == nil || !strings.Contains(err.Error(), "failed to compile") {
			t.Fatalf("expected compile error, got %v", err)
		}
	})

	t.Run("transform missing returns error", func(t *testing.T) {
		script := `function other(row) return row end`
		tr, err := newLuaTransformer(config.PluginConfig{Engine: config.PluginEngineLua, Script: script, TimeoutMs: 1000})
		if err != nil {
			t.Fatalf("newLuaTransformer() error = %v", err)
		}
		defer tr.close()

		_, err = tr.transform([]any{1, "a", 1.0}, columns)
		if err == nil || !strings.Contains(err.Error(), "lua transform failed") {
			t.Fatalf("expected transform error, got %v", err)
		}
	})

	t.Run("non-table return errors", func(t *testing.T) {
		script := `function transform(row) return 42 end`
		tr, err := newLuaTransformer(config.PluginConfig{Engine: config.PluginEngineLua, Script: script, TimeoutMs: 1000})
		if err != nil {
			t.Fatalf("newLuaTransformer() error = %v", err)
		}
		defer tr.close()

		_, err = tr.transform([]any{1, "a", 1.0}, columns)
		if err == nil || !strings.Contains(err.Error(), "must return a table") {
			t.Fatalf("expected non-table error, got %v", err)
		}
	})

	t.Run("timeout aborts long-running script", func(t *testing.T) {
		script := `
			function transform(row)
				local x = 0
				while true do
					x = x + 1
				end
				return row
			end
		`
		tr, err := newLuaTransformer(config.PluginConfig{Engine: config.PluginEngineLua, Script: script, TimeoutMs: 50})
		if err != nil {
			t.Fatalf("newLuaTransformer() error = %v", err)
		}
		defer tr.close()

		start := time.Now()
		_, err = tr.transform([]any{1, "a", 1.0}, columns)
		elapsed := time.Since(start)

		if err == nil || !strings.Contains(err.Error(), "context deadline exceeded") {
			t.Fatalf("expected timeout error, got %v", err)
		}
		if elapsed > 200*time.Millisecond {
			t.Fatalf("timeout took too long: %v", elapsed)
		}
	})

	t.Run("conditional branch", func(t *testing.T) {
		script := `
			function transform(row)
				if row.value > 50 then
					row.name = "high"
				else
					row.name = "low"
				end
				return row
			end
		`
		tr, err := newLuaTransformer(config.PluginConfig{Engine: config.PluginEngineLua, Script: script, TimeoutMs: 1000})
		if err != nil {
			t.Fatalf("newLuaTransformer() error = %v", err)
		}
		defer tr.close()

		row := []any{1, "alice", 75.0}
		result, err := tr.transform(row, columns)
		if err != nil {
			t.Fatalf("transform() error = %v", err)
		}
		if result[1] != "high" {
			t.Fatalf("expected high, got %v", result[1])
		}
	})
}

func TestJSTransformer(t *testing.T) {
	columns := []database.ColumnMetadata{
		{Name: "id"},
		{Name: "name"},
		{Name: "value"},
	}

	t.Run("transform modifies row", func(t *testing.T) {
		script := `
			function transform(row) {
				row.name = row.name + "_suffix";
				row.value = row.value * 2;
				return row;
			}
		`
		tr, err := newJSTransformer(config.PluginConfig{Engine: config.PluginEngineJavaScript, Script: script, TimeoutMs: 1000})
		if err != nil {
			t.Fatalf("newJSTransformer() error = %v", err)
		}
		defer tr.close()

		row := []any{1, "alice", 10.5}
		result, err := tr.transform(row, columns)
		if err != nil {
			t.Fatalf("transform() error = %v", err)
		}
		if result[0] != float64(1) {
			t.Fatalf("expected id unchanged, got %v", result[0])
		}
		if result[1] != "alice_suffix" {
			t.Fatalf("expected name modified, got %v", result[1])
		}
		if result[2] != 21.0 {
			t.Fatalf("expected value doubled, got %v", result[2])
		}
	})

	t.Run("transform with nil values", func(t *testing.T) {
		script := `
			function transform(row) {
				if (row.name === null || row.name === undefined) {
					row.name = "default";
				}
				return row;
			}
		`
		tr, err := newJSTransformer(config.PluginConfig{Engine: config.PluginEngineJavaScript, Script: script, TimeoutMs: 1000})
		if err != nil {
			t.Fatalf("newJSTransformer() error = %v", err)
		}
		defer tr.close()

		row := []any{1, nil, 10.5}
		result, err := tr.transform(row, columns)
		if err != nil {
			t.Fatalf("transform() error = %v", err)
		}
		if result[1] != "default" {
			t.Fatalf("expected nil replaced, got %v", result[1])
		}
	})

	t.Run("invalid script fails at compile", func(t *testing.T) {
		_, err := newJSTransformer(config.PluginConfig{Engine: config.PluginEngineJavaScript, Script: "function {", TimeoutMs: 1000})
		if err == nil || !strings.Contains(err.Error(), "failed to compile") {
			t.Fatalf("expected compile error, got %v", err)
		}
	})

	t.Run("transform missing returns error", func(t *testing.T) {
		script := `function other(row) { return row; }`
		tr, err := newJSTransformer(config.PluginConfig{Engine: config.PluginEngineJavaScript, Script: script, TimeoutMs: 1000})
		if err != nil {
			t.Fatalf("newJSTransformer() error = %v", err)
		}
		defer tr.close()

		_, err = tr.transform([]any{1, "a", 1.0}, columns)
		if err == nil || !strings.Contains(err.Error(), "javascript transform failed") {
			t.Fatalf("expected transform error, got %v", err)
		}
	})

	t.Run("non-object return errors", func(t *testing.T) {
		script := `function transform(row) { return 42; }`
		tr, err := newJSTransformer(config.PluginConfig{Engine: config.PluginEngineJavaScript, Script: script, TimeoutMs: 1000})
		if err != nil {
			t.Fatalf("newJSTransformer() error = %v", err)
		}
		defer tr.close()

		_, err = tr.transform([]any{1, "a", 1.0}, columns)
		if err == nil || !strings.Contains(err.Error(), "must return an object") {
			t.Fatalf("expected non-object error, got %v", err)
		}
	})

	t.Run("timeout aborts long-running script", func(t *testing.T) {
		script := `
			function transform(row) {
				var x = 0;
				while (true) { x++; }
				return row;
			}
		`
		tr, err := newJSTransformer(config.PluginConfig{Engine: config.PluginEngineJavaScript, Script: script, TimeoutMs: 50})
		if err != nil {
			t.Fatalf("newJSTransformer() error = %v", err)
		}
		defer tr.close()

		start := time.Now()
		_, err = tr.transform([]any{1, "a", 1.0}, columns)
		elapsed := time.Since(start)

		if err == nil || !strings.Contains(err.Error(), "timed out") {
			t.Fatalf("expected timeout error, got %v", err)
		}
		if elapsed > 200*time.Millisecond {
			t.Fatalf("timeout took too long: %v", elapsed)
		}
	})

	t.Run("conditional branch", func(t *testing.T) {
		script := `
			function transform(row) {
				if (row.value > 50) {
					row.name = "high";
				} else {
					row.name = "low";
				}
				return row;
			}
		`
		tr, err := newJSTransformer(config.PluginConfig{Engine: config.PluginEngineJavaScript, Script: script, TimeoutMs: 1000})
		if err != nil {
			t.Fatalf("newJSTransformer() error = %v", err)
		}
		defer tr.close()

		row := []any{1, "alice", 75.0}
		result, err := tr.transform(row, columns)
		if err != nil {
			t.Fatalf("transform() error = %v", err)
		}
		if result[1] != "high" {
			t.Fatalf("expected high, got %v", result[1])
		}
	})
}

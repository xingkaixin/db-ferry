package processor

import (
	"context"
	"fmt"
	"time"

	"db-ferry/config"
	"db-ferry/database"

	"github.com/robertkrimen/otto"
	lua "github.com/yuin/gopher-lua"
)

// rowTransformer applies per-row transformations via a scripting engine.
type rowTransformer interface {
	transform(row []any, columns []database.ColumnMetadata) ([]any, error)
	close()
}

// newPluginEngine creates a rowTransformer based on the task's plugin config.
// Returns nil when no plugin is configured.
func newPluginEngine(cfg config.PluginConfig) (rowTransformer, error) {
	if cfg.Engine == "" {
		return nil, nil
	}

	switch cfg.Engine {
	case config.PluginEngineLua:
		return newLuaTransformer(cfg)
	case config.PluginEngineJavaScript:
		return newJSTransformer(cfg)
	default:
		return nil, fmt.Errorf("unsupported plugin engine: %s", cfg.Engine)
	}
}

// luaTransformer implements rowTransformer using gopher-lua.
type luaTransformer struct {
	lstate  *lua.LState
	script  string
	timeout time.Duration
}

func newLuaTransformer(cfg config.PluginConfig) (*luaTransformer, error) {
	l := lua.NewState()
	if err := l.DoString(cfg.Script); err != nil {
		l.Close()
		return nil, fmt.Errorf("failed to compile lua script: %w", err)
	}

	return &luaTransformer{
		lstate:  l,
		script:  cfg.Script,
		timeout: time.Duration(cfg.TimeoutMs) * time.Millisecond,
	}, nil
}

func (t *luaTransformer) transform(row []any, columns []database.ColumnMetadata) ([]any, error) {
	ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
	defer cancel()

	t.lstate.SetContext(ctx)
	defer t.lstate.RemoveContext()

	table := t.rowToLuaTable(row, columns)
	t.lstate.Push(t.lstate.GetGlobal("transform"))
	t.lstate.Push(table)

	if err := t.lstate.PCall(1, 1, nil); err != nil {
		return nil, fmt.Errorf("lua transform failed: %w", err)
	}

	result := t.lstate.Get(-1)
	t.lstate.Pop(1)

	retTable, ok := result.(*lua.LTable)
	if !ok {
		return nil, fmt.Errorf("lua transform must return a table, got %T", result)
	}

	return t.luaTableToRow(retTable, columns), nil
}

func (t *luaTransformer) close() {
	if t.lstate != nil {
		t.lstate.Close()
	}
}

func (t *luaTransformer) rowToLuaTable(row []any, columns []database.ColumnMetadata) *lua.LTable {
	table := t.lstate.CreateTable(len(row), 0)
	for i, v := range row {
		table.RawSetString(columns[i].Name, goValueToLua(t.lstate, v))
	}
	return table
}

func (t *luaTransformer) luaTableToRow(table *lua.LTable, columns []database.ColumnMetadata) []any {
	row := make([]any, len(columns))
	for i, col := range columns {
		v := table.RawGetString(col.Name)
		row[i] = luaValueToGo(v)
	}
	return row
}

func goValueToLua(l *lua.LState, v any) lua.LValue {
	if v == nil {
		return lua.LNil
	}
	switch val := v.(type) {
	case int:
		return lua.LNumber(val)
	case int8:
		return lua.LNumber(val)
	case int16:
		return lua.LNumber(val)
	case int32:
		return lua.LNumber(val)
	case int64:
		return lua.LNumber(val)
	case uint:
		return lua.LNumber(val)
	case uint8:
		return lua.LNumber(val)
	case uint16:
		return lua.LNumber(val)
	case uint32:
		return lua.LNumber(val)
	case uint64:
		return lua.LNumber(val)
	case float32:
		return lua.LNumber(val)
	case float64:
		return lua.LNumber(val)
	case string:
		return lua.LString(val)
	case bool:
		return lua.LBool(val)
	case []byte:
		return lua.LString(string(val))
	default:
		return lua.LString(fmt.Sprint(v))
	}
}

func luaValueToGo(v lua.LValue) any {
	if v == lua.LNil {
		return nil
	}
	switch val := v.(type) {
	case lua.LNumber:
		return float64(val)
	case lua.LString:
		return string(val)
	case lua.LBool:
		return bool(val)
	default:
		return fmt.Sprint(v)
	}
}

// jsTransformer implements rowTransformer using otto.
type jsTransformer struct {
	vm      *otto.Otto
	script  string
	timeout time.Duration
}

func newJSTransformer(cfg config.PluginConfig) (*jsTransformer, error) {
	vm := otto.New()
	if _, err := vm.Run(cfg.Script); err != nil {
		return nil, fmt.Errorf("failed to compile javascript script: %w", err)
	}

	return &jsTransformer{
		vm:      vm,
		script:  cfg.Script,
		timeout: time.Duration(cfg.TimeoutMs) * time.Millisecond,
	}, nil
}

func (t *jsTransformer) transform(row []any, columns []database.ColumnMetadata) ([]any, error) {
	obj := t.rowToJSObject(row, columns)

	// otto does not support context cancellation; use a goroutine with timeout.
	// Note: the goroutine may leak on timeout because otto cannot be interrupted.
	type result struct {
		value otto.Value
		err   error
	}
	done := make(chan result, 1)

	go func() {
		val, err := t.vm.Call("transform", nil, obj)
		done <- result{value: val, err: err}
	}()

	select {
	case <-time.After(t.timeout):
		return nil, fmt.Errorf("javascript transform timed out after %v", t.timeout)
	case res := <-done:
		if res.err != nil {
			return nil, fmt.Errorf("javascript transform failed: %w", res.err)
		}
		return t.jsObjectToRow(res.value, columns)
	}
}

func (t *jsTransformer) close() {}

func (t *jsTransformer) rowToJSObject(row []any, columns []database.ColumnMetadata) map[string]any {
	obj := make(map[string]any, len(row))
	for i, v := range row {
		obj[columns[i].Name] = v
	}
	return obj
}

func (t *jsTransformer) jsObjectToRow(val otto.Value, columns []database.ColumnMetadata) ([]any, error) {
	if !val.IsObject() {
		return nil, fmt.Errorf("javascript transform must return an object, got %s", val.Class())
	}

	row := make([]any, len(columns))
	for i, col := range columns {
		v, err := val.Object().Get(col.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to get property %q: %w", col.Name, err)
		}
		row[i] = jsValueToGo(v)
	}
	return row, nil
}

func jsValueToGo(v otto.Value) any {
	if v.IsNull() || v.IsUndefined() {
		return nil
	}
	if v.IsNumber() {
		f, _ := v.ToFloat()
		return f
	}
	if v.IsString() {
		s, _ := v.ToString()
		return s
	}
	if v.IsBoolean() {
		b, _ := v.ToBoolean()
		return b
	}
	return v.String()
}

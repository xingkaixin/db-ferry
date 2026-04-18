package processor

import (
	"strings"
	"testing"

	"db-ferry/config"
	"db-ferry/database"
)

func TestMaskPhoneCN(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"13800138000", "138****8000"},
		{"138123", "1****3"},
		{"123", "***"},
		{"1234", "1**4"},
		{"12345", "1***5"},
	}
	for _, tc := range cases {
		got := maskPhoneCN(tc.input)
		if got != tc.want {
			t.Fatalf("maskPhoneCN(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestMaskPhoneUS(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"(415) 555-2671", "(415) ***-2671"},
		{"415-555-2671", "(415) ***-2671"},
		{"4155552671", "(415) ***-2671"},
		{"123-4567", "123-****"},
		{"1234", "****"},
	}
	for _, tc := range cases {
		got := maskPhoneUS(tc.input)
		if got != tc.want {
			t.Fatalf("maskPhoneUS(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestMaskEmail(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"alice@example.com", "a***@example.com"},
		{"a@example.com", "a@example.com"},
		{"not-an-email", "n***********"},
	}
	for _, tc := range cases {
		got := maskEmail(tc.input)
		if got != tc.want {
			t.Fatalf("maskEmail(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestMaskIDCardCN(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"110101199001011234", "110***********1234"},
		{"1234567", "1234567"},
		{"12345678", "123*5678"},
	}
	for _, tc := range cases {
		got := maskIDCardCN(tc.input)
		if got != tc.want {
			t.Fatalf("maskIDCardCN(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestMaskEngineApply(t *testing.T) {
	columns := []database.ColumnMetadata{
		{Name: "id"},
		{Name: "phone"},
		{Name: "email"},
	}
	masks := []config.MaskingConfig{
		{Column: "phone", Rule: config.MaskRulePhoneCN},
		{Column: "email", Rule: config.MaskRuleEmail},
	}
	engine := newMaskEngine(masks, columns)

	row := []any{1, "13800138000", "alice@example.com"}
	result := engine.apply(row, columns)

	if result[0] != 1 {
		t.Fatalf("unexpected id: %v", result[0])
	}
	if result[1] != "138****8000" {
		t.Fatalf("unexpected phone: %v", result[1])
	}
	if result[2] != "a***@example.com" {
		t.Fatalf("unexpected email: %v", result[2])
	}
}

func TestMaskEngineNilPassthrough(t *testing.T) {
	columns := []database.ColumnMetadata{{Name: "phone"}}
	masks := []config.MaskingConfig{{Column: "phone", Rule: config.MaskRulePhoneCN}}
	engine := newMaskEngine(masks, columns)

	row := []any{nil}
	result := engine.apply(row, columns)
	if result[0] != nil {
		t.Fatalf("expected nil passthrough, got %v", result[0])
	}
}

func TestMaskEngineMissingColumn(t *testing.T) {
	columns := []database.ColumnMetadata{{Name: "id"}}
	masks := []config.MaskingConfig{{Column: "missing", Rule: config.MaskRulePhoneCN}}
	engine := newMaskEngine(masks, columns)

	row := []any{1}
	result := engine.apply(row, columns)
	if result[0] != 1 {
		t.Fatalf("expected unchanged row, got %v", result[0])
	}
}

func TestMaskEngineRandomNumeric(t *testing.T) {
	columns := []database.ColumnMetadata{{Name: "score"}}
	masks := []config.MaskingConfig{{Column: "score", Rule: config.MaskRuleRandomNumeric, Range: []float64{0, 100}}}
	engine := newMaskEngine(masks, columns)

	row := []any{999}
	result := engine.apply(row, columns)
	v, ok := result[0].(int64)
	if !ok {
		t.Fatalf("expected int64, got %T", result[0])
	}
	if v < 0 || v > 100 {
		t.Fatalf("expected value in [0,100], got %d", v)
	}
}

func TestMaskEngineRandomNumericFloat(t *testing.T) {
	columns := []database.ColumnMetadata{{Name: "rate"}}
	masks := []config.MaskingConfig{{Column: "rate", Rule: config.MaskRuleRandomNumeric, Range: []float64{0.5, 1.5}}}
	engine := newMaskEngine(masks, columns)

	row := []any{9.99}
	result := engine.apply(row, columns)
	v, ok := result[0].(float64)
	if !ok {
		t.Fatalf("expected float64, got %T", result[0])
	}
	if v < 0.5 || v > 1.5 {
		t.Fatalf("expected value in [0.5,1.5], got %f", v)
	}
}

func TestMaskEngineFixedValue(t *testing.T) {
	columns := []database.ColumnMetadata{{Name: "status", DatabaseType: "VARCHAR", GoType: "string"}}
	masks := []config.MaskingConfig{{Column: "status", Rule: config.MaskRuleFixedValue, Value: "MASKED"}}
	engine := newMaskEngine(masks, columns)

	row := []any{"active"}
	result := engine.apply(row, columns)
	if result[0] != "MASKED" {
		t.Fatalf("expected MASKED, got %v", result[0])
	}
}

func TestMaskEngineHashDeterministic(t *testing.T) {
	columns := []database.ColumnMetadata{{Name: "token"}}
	masks := []config.MaskingConfig{{Column: "token", Rule: config.MaskRuleHash}}
	engine := newMaskEngine(masks, columns)

	row := []any{"secret123"}
	result1 := engine.apply(append([]any(nil), row...), columns)
	result2 := engine.apply(append([]any(nil), row...), columns)
	if result1[0] != result2[0] {
		t.Fatalf("expected deterministic hash, got %v vs %v", result1[0], result2[0])
	}
	if result1[0] == "secret123" {
		t.Fatalf("expected hashed value, got original")
	}
}

func TestMaskEngineNameCN(t *testing.T) {
	columns := []database.ColumnMetadata{{Name: "name"}}
	masks := []config.MaskingConfig{{Column: "name", Rule: config.MaskRuleNameCN}}
	engine := newMaskEngine(masks, columns)

	row := []any{"张三"}
	result := engine.apply(row, columns)
	name, ok := result[0].(string)
	if !ok || len(name) == 0 {
		t.Fatalf("expected non-empty string, got %v", result[0])
	}
}

func TestMaskEngineRandomDate(t *testing.T) {
	columns := []database.ColumnMetadata{{Name: "dob"}}
	masks := []config.MaskingConfig{{Column: "dob", Rule: config.MaskRuleRandomDate}}
	engine := newMaskEngine(masks, columns)

	row := []any{"1990-01-01"}
	result := engine.apply(row, columns)
	dateStr, ok := result[0].(string)
	if !ok || len(dateStr) != 10 {
		t.Fatalf("expected date string YYYY-MM-DD, got %v", result[0])
	}
}

func TestParseFixedValueTypes(t *testing.T) {
	cases := []struct {
		value  string
		col    database.ColumnMetadata
		expect any
	}{
		{"42", database.ColumnMetadata{GoType: "int64"}, int64(42)},
		{"3.14", database.ColumnMetadata{GoType: "float64"}, 3.14},
		{"true", database.ColumnMetadata{GoType: "bool"}, true},
		{"hello", database.ColumnMetadata{GoType: "string"}, "hello"},
		{"NULL", database.ColumnMetadata{GoType: "string"}, nil},
		{"2024-01-01", database.ColumnMetadata{DatabaseType: "DATE"}, "2024-01-01"},
	}
	for _, tc := range cases {
		got := parseFixedValue(tc.value, tc.col)
		if tc.expect == nil {
			if got != nil {
				t.Fatalf("parseFixedValue(%q, %+v) = %v, want nil", tc.value, tc.col, got)
			}
			continue
		}
		if got != tc.expect {
			t.Fatalf("parseFixedValue(%q, %+v) = %v (%T), want %v (%T)", tc.value, tc.col, got, got, tc.expect, tc.expect)
		}
	}
}

func TestMaskEmailComplexLocal(t *testing.T) {
	got := maskEmail("alice+bob@example.co.uk")
	if !strings.HasPrefix(got, "a***@") {
		t.Fatalf("expected masked local part, got %q", got)
	}
}

func TestToString(t *testing.T) {
	cases := []struct {
		input any
		want  string
	}{
		{"hello", "hello"},
		{[]byte("world"), "world"},
		{42, "42"},
		{3.14, "3.14"},
		{true, "true"},
	}
	for _, tc := range cases {
		got := toString(tc.input)
		if got != tc.want {
			t.Fatalf("toString(%v) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestMaskValueUnsupportedRule(t *testing.T) {
	columns := []database.ColumnMetadata{{Name: "token"}}
	masks := []config.MaskingConfig{{Column: "token", Rule: "unsupported_rule"}}
	engine := newMaskEngine(masks, columns)

	row := []any{"secret"}
	result := engine.apply(row, columns)
	if result[0] != "secret" {
		t.Fatalf("expected unsupported rule to passthrough, got %v", result[0])
	}
}

func TestMaskValueLength(t *testing.T) {
	cases := []struct {
		input      string
		keepPrefix int
		keepSuffix int
		expected   string
	}{
		{"hello", 1, 1, "h***o"},
		{"hi", 1, 1, "**"},
		{"a", 1, 1, "*"},
		{"abcdef", 2, 2, "ab**ef"},
	}
	for _, tc := range cases {
		got := maskValueLength(tc.input, tc.keepPrefix, tc.keepSuffix)
		if got != tc.expected {
			t.Fatalf("maskValueLength(%q, %d, %d) = %q, want %q", tc.input, tc.keepPrefix, tc.keepSuffix, got, tc.expected)
		}
	}
}

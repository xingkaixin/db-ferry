package processor

import (
	"fmt"
	"hash/fnv"
	"log"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"time"

	"db-ferry/config"
	"db-ferry/database"
)

var (
	commonSurnames = []string{
		"王", "李", "张", "刘", "陈", "杨", "黄", "赵", "周", "吴",
		"徐", "孙", "马", "朱", "胡", "郭", "何", "林", "罗", "高",
	}
	commonNames = []string{
		"伟", "芳", "娜", "敏", "静", "丽", "强", "磊", "军", "洋",
		"勇", "艳", "杰", "娟", "涛", "明", "超", "秀", "霞", "平",
		"刚", "桂", "英", "华", "建", "文", "辉", "玲", "婷", "宇",
	}
)

// maskEngine applies configured masking rules to row values.
type maskEngine struct {
	masks   []config.MaskingConfig
	indices []int
	rng     *rand.Rand
}

func newMaskEngine(masks []config.MaskingConfig, columns []database.ColumnMetadata) *maskEngine {
	if len(masks) == 0 {
		return nil
	}

	indices := make([]int, len(masks))
	for i, m := range masks {
		idx := findColumnIndex(columns, m.Column)
		if idx < 0 {
			log.Printf("Warning: masking column '%s' not found in query result", m.Column)
		}
		indices[i] = idx
	}

	return &maskEngine{
		masks:   masks,
		indices: indices,
		rng:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (e *maskEngine) apply(row []any, columns []database.ColumnMetadata) []any {
	if e == nil {
		return row
	}

	for i, idx := range e.indices {
		if idx < 0 || idx >= len(row) {
			continue
		}
		if row[idx] == nil {
			continue
		}
		masked, ok := e.maskValue(row[idx], e.masks[i], columns[idx])
		if ok {
			row[idx] = masked
		}
	}
	return row
}

func (e *maskEngine) maskValue(value any, rule config.MaskingConfig, column database.ColumnMetadata) (any, bool) {
	switch rule.Rule {
	case config.MaskRulePhoneCN:
		return maskPhoneCN(toString(value)), true
	case config.MaskRulePhoneUS:
		return maskPhoneUS(toString(value)), true
	case config.MaskRuleEmail:
		return maskEmail(toString(value)), true
	case config.MaskRuleIDCardCN:
		return maskIDCardCN(toString(value)), true
	case config.MaskRuleNameCN:
		return e.randomNameCN(), true
	case config.MaskRuleRandomNumeric:
		return e.randomNumeric(rule.Range), true
	case config.MaskRuleRandomDate:
		return e.randomDate(), true
	case config.MaskRuleFixedValue:
		return parseFixedValue(rule.Value, column), true
	case config.MaskRuleHash:
		return hashValue(value), true
	default:
		log.Printf("Warning: unsupported masking rule '%s' for column '%s'", rule.Rule, column.Name)
		return value, false
	}
}

func toString(v any) string {
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	default:
		return fmt.Sprint(val)
	}
}

func maskPhoneCN(s string) string {
	runes := []rune(s)
	n := len(runes)
	if n <= 3 {
		return strings.Repeat("*", n)
	}
	if n <= 7 {
		out := make([]rune, n)
		out[0] = runes[0]
		for i := 1; i < n-1; i++ {
			out[i] = '*'
		}
		out[n-1] = runes[n-1]
		return string(out)
	}
	return string(runes[:3]) + "****" + string(runes[n-4:])
}

var phoneUSRegex = regexp.MustCompile(`^\D*(\d{3})\D*(\d{3})\D*(\d{4})\D*$`)

func maskPhoneUS(s string) string {
	matches := phoneUSRegex.FindStringSubmatch(s)
	if len(matches) == 4 {
		return fmt.Sprintf("(%s) ***-%s", matches[1], matches[3])
	}
	runes := []rune(s)
	n := len(runes)
	if n <= 4 {
		return strings.Repeat("*", n)
	}
	return string(runes[:n-4]) + strings.Repeat("*", 4)
}

func maskEmail(s string) string {
	parts := strings.Split(s, "@")
	if len(parts) != 2 {
		return maskAllButFirst(s)
	}
	local := parts[0]
	domain := parts[1]
	if len(local) == 0 {
		return "***@" + domain
	}
	if len(local) == 1 {
		return local + "@" + domain
	}
	return string(local[0]) + "***@" + domain
}

func maskIDCardCN(s string) string {
	runes := []rune(s)
	n := len(runes)
	if n <= 6 {
		return strings.Repeat("*", n)
	}
	return string(runes[:3]) + strings.Repeat("*", n-7) + string(runes[n-4:])
}

func maskAllButFirst(s string) string {
	runes := []rune(s)
	n := len(runes)
	if n <= 1 {
		return strings.Repeat("*", n)
	}
	return string(runes[0]) + strings.Repeat("*", n-1)
}

func (e *maskEngine) randomNameCN() string {
	surname := commonSurnames[e.rng.Intn(len(commonSurnames))]
	name := commonNames[e.rng.Intn(len(commonNames))]
	if e.rng.Float32() < 0.3 {
		name += commonNames[e.rng.Intn(len(commonNames))]
	}
	return surname + name
}

func (e *maskEngine) randomNumeric(rng []float64) any {
	minVal, maxVal := rng[0], rng[1]
	if minVal > maxVal {
		minVal, maxVal = maxVal, minVal
	}
	if isIntegerRange(minVal, maxVal) {
		return int64(minVal) + int64(e.rng.Int63n(int64(maxVal-minVal)+1))
	}
	val := minVal + e.rng.Float64()*(maxVal-minVal)
	return val
}

func isIntegerRange(a, b float64) bool {
	return a == float64(int64(a)) && b == float64(int64(b))
}

func (e *maskEngine) randomDate() any {
	start := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Now()
	duration := end.Sub(start)
	randomDuration := time.Duration(e.rng.Int63n(int64(duration)))
	return start.Add(randomDuration).Format("2006-01-02")
}

func parseFixedValue(value string, column database.ColumnMetadata) any {
	goType := strings.ToUpper(column.GoType)
	dbType := strings.ToUpper(column.DatabaseType)

	switch {
	case strings.Contains(goType, "INT"), strings.Contains(goType, "UINT"):
		if v, err := strconv.ParseInt(value, 10, 64); err == nil {
			return v
		}
	case strings.Contains(goType, "FLOAT"), strings.Contains(goType, "DOUBLE"):
		if v, err := strconv.ParseFloat(value, 64); err == nil {
			return v
		}
	case strings.Contains(goType, "BOOL"):
		if v, err := strconv.ParseBool(value); err == nil {
			return v
		}
	case strings.Contains(dbType, "DATE"), strings.Contains(dbType, "TIME"):
		return value
	}

	if value == "NULL" || value == "null" {
		return nil
	}
	return value
}

func hashValue(value any) string {
	h := fnv.New64a()
	_, _ = fmt.Fprintf(h, "%v", value)
	return fmt.Sprintf("%016x", h.Sum64())
}

// maskValueLength masks a string while preserving its UTF-8 rune length.
func maskValueLength(s string, keepPrefix, keepSuffix int) string {
	runes := []rune(s)
	n := len(runes)
	if n <= keepPrefix+keepSuffix {
		return strings.Repeat("*", n)
	}
	return string(runes[:keepPrefix]) + strings.Repeat("*", n-keepPrefix-keepSuffix) + string(runes[n-keepSuffix:])
}

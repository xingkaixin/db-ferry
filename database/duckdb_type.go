package database

import (
	"fmt"
	"strings"
)

// MapToDuckDBType maps column metadata to a DuckDB column type.
func MapToDuckDBType(column ColumnMetadata) string {
	typeName := strings.ToUpper(column.DatabaseType)
	if typeName == "" {
		typeName = strings.ToUpper(column.GoType)
	}

	length := int64(0)
	if column.LengthValid {
		length = column.Length
	}

	precision := int64(0)
	scale := int64(0)
	if column.PrecisionScaleValid {
		precision = column.Precision
		scale = column.Scale
	}

	switch {
	case strings.Contains(typeName, "INT"), strings.Contains(typeName, "NUMBER") && !column.PrecisionScaleValid:
		return "BIGINT"
	case strings.Contains(typeName, "DEC"), strings.Contains(typeName, "NUMERIC"), strings.Contains(typeName, "NUMBER"):
		if precision > 0 {
			if scale < 0 {
				scale = 0
			}
			return fmt.Sprintf("DECIMAL(%d,%d)", precision, scale)
		}
		return "DECIMAL(38,0)"
	case strings.Contains(typeName, "DOUBLE"), strings.Contains(typeName, "FLOAT"), strings.Contains(typeName, "REAL"):
		return "DOUBLE"
	case strings.Contains(typeName, "CHAR"), strings.Contains(typeName, "TEXT"), strings.Contains(typeName, "CLOB"), strings.Contains(typeName, "STRING"):
		if length > 0 && length <= 1048576 {
			return fmt.Sprintf("VARCHAR(%d)", length)
		}
		return "VARCHAR"
	case strings.Contains(typeName, "DATE"), strings.Contains(typeName, "TIME"):
		return "TIMESTAMP"
	case strings.Contains(typeName, "BLOB"), strings.Contains(typeName, "BINARY"), strings.Contains(typeName, "RAW"):
		return "BLOB"
	case strings.Contains(typeName, "BOOL"):
		return "BOOLEAN"
	default:
		if column.PrecisionScaleValid && column.Scale > 0 {
			return "DOUBLE"
		}
		return "VARCHAR"
	}
}

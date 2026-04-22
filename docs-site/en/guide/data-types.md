# Data Type Mapping

The processor inspects driver metadata to determine precision, scale, length, and nullability before creating the target table.

| Source Type (Oracle/MySQL/PostgreSQL/SQL Server) | Target Mapping |
|---------------------------|----------------|
| NUMBER / DECIMAL | INTEGER or REAL (precision-aware) |
| VARCHAR / CHAR / TEXT | TEXT |
| DATE / DATETIME / TIMESTAMP | TEXT (ISO 8601) |
| BLOB / RAW / BINARY | BLOB |

## Mapping Details

- **Integer types** are mapped to `INTEGER` in the target database
- **Decimal/floating-point types** are mapped to `REAL` with awareness of precision and scale
- **String types** (VARCHAR, CHAR, TEXT, etc.) are mapped to `TEXT`
- **Date/time types** are stored as `TEXT` in ISO 8601 format
- **Binary types** (BLOB, RAW, BINARY, etc.) are mapped to `BLOB`

The exact target type varies by destination database engine. db-ferry uses each adapter's type mapping function to translate the generic type into the engine-specific DDL.

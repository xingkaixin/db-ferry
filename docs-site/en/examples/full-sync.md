# Full Sync: Production to Local

This example demonstrates syncing data from a production MySQL database to a local SQLite file for analysis.

## Configuration

```toml
# ============= Database Definitions =============

[[databases]]
name = "production"
type = "mysql"
host = "prod-db.company.com"
port = "3306"
database = "business_db"
user = "readonly"
password = "readonly_password"

[[databases]]
name = "local"
type = "sqlite"
path = "./business_analysis.db"

# ============= Migration Tasks =============

[[tasks]]
table_name = "customers"
sql = """
SELECT
    customer_id,
    customer_name,
    email,
    phone,
    registration_date,
    customer_level
FROM customers
WHERE status = 'active'
"""
source_db = "production"
target_db = "local"
ignore = false

[[tasks.indexes]]
name = "idx_customer_id"
columns = ["customer_id"]
unique = true

[[tasks.indexes]]
name = "idx_customer_level"
columns = ["customer_level"]
unique = false

[[tasks]]
table_name = "orders_2024"
sql = """
SELECT
    order_id,
    customer_id,
    order_date,
    total_amount,
    payment_method,
    order_status
FROM orders
WHERE order_date >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR)
"""
source_db = "production"
target_db = "local"
ignore = false

[[tasks.indexes]]
name = "idx_orders_customer"
columns = ["customer_id"]
unique = false

[[tasks.indexes]]
name = "idx_orders_date"
columns = ["order_date:DESC"]
unique = false

[[tasks]]
table_name = "products"
sql = "SELECT * FROM products"
source_db = "production"
target_db = "local"
ignore = false

[[tasks.indexes]]
name = "idx_product_id"
columns = ["product_id"]
unique = true
```

## Running

```bash
db-ferry -config task.toml
```

This will:
1. Connect to the production MySQL database
2. Create `customers`, `orders_2024`, and `products` tables in the local SQLite file
3. Stream data in batches of 1000 rows with a progress bar
4. Create the specified indexes after data load completes

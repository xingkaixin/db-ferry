# 场景：从生产库同步数据到本地分析库

## 完整配置示例

```toml
# ============= 数据库定义 =============

# 生产环境 MySQL
[[databases]]
name = "生产数据库"
type = "mysql"
host = "prod-db.company.com"
port = "3306"
database = "business_db"
user = "readonly"
password = "readonly_password"

# 本地 SQLite 分析库
[[databases]]
name = "本地分析库"
type = "sqlite"
path = "./business_analysis.db"

# ============= 迁移任务 =============

# 迁移客户信息
[[tasks]]
table_name = "客户资料"
sql = """
SELECT
    customer_id,
    customer_name,
    email,
    phone,
    registration_date,
    customer_level
FROM customers
WHERE status = '活跃'
"""
source_db = "生产数据库"
target_db = "本地分析库"
ignore = false

[[tasks.indexes]]
name = "idx_客户ID"
columns = ["customer_id"]
unique = true

[[tasks.indexes]]
name = "idx_客户等级"
columns = ["customer_level"]
unique = false

# 迁移订单信息（最近一年）
[[tasks]]
table_name = "订单数据_2024"
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
source_db = "生产数据库"
target_db = "本地分析库"
ignore = false

[[tasks.indexes]]
name = "idx_订单客户"
columns = ["customer_id"]
unique = false

[[tasks.indexes]]
name = "idx_订单日期"
columns = ["order_date:DESC"]
unique = false

# 迁移产品信息
[[tasks]]
table_name = "产品目录"
sql = "SELECT * FROM products"
source_db = "生产数据库"
target_db = "本地分析库"
ignore = false

[[tasks.indexes]]
name = "idx_产品编号"
columns = ["product_id"]
unique = true
```

## 执行迁移

```bash
db-ferry -config task.toml
```

执行过程：
1. 连接生产环境 MySQL 数据库
2. 在本地 SQLite 文件中创建 `客户资料`、`订单数据_2024`、`产品目录` 三张表
3. 分批（每批 1000 行）流式迁移数据，显示进度条
4. 数据迁移完成后，自动创建配置的索引

## 多数据库汇总到数据仓库

另一个常见场景是将多个源数据库的数据汇总到一个数据仓库：

```toml
[[databases]]
name = "销售系统"
type = "mysql"
host = "sales.company.com"
database = "sales_db"
user = "dw_reader"
password = "dw_pass"

[[databases]]
name = "库存系统"
type = "oracle"
host = "inventory.company.com"
service = "INVDB"
user = "dw_reader"
password = "dw_pass"

[[databases]]
name = "数据仓库"
type = "mysql"
host = "dw.company.com"
database = "data_warehouse"
user = "dw_writer"
password = "dw_write_pass"

[[tasks]]
table_name = "fact_orders"
sql = """
SELECT
    order_id,
    customer_id,
    product_id,
    order_date,
    quantity,
    unit_price,
    (quantity * unit_price) as revenue
FROM sales_orders
WHERE order_status = '已完成'
"""
source_db = "销售系统"
target_db = "数据仓库"
ignore = false

[[tasks]]
table_name = "fact_inventory"
sql = """
SELECT
    product_id,
    warehouse_id,
    inventory_date,
    quantity_on_hand,
    quantity_reserved,
    (quantity_on_hand - quantity_reserved) as available_quantity
FROM inventory_snapshot
"""
source_db = "库存系统"
target_db = "数据仓库"
ignore = false

[[tasks.indexes]]
name = "idx_库存产品日期"
columns = ["product_id", "inventory_date:DESC"]
unique = false
```

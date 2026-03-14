# EMR Pipeline – Data Extraction from RDS via Spark

## 项目概述

本项目实现了一个完整的数据抽取流程：

```
PostgreSQL (RDS, 私有子网)
        ↓  JDBC over SSL (SG-to-SG)
EMR Cluster (Spark / SparkSQL)
        ↓  写入 Parquet
S3 Bucket (加密，VPC Endpoint 路由)
```

---

## 目录结构

```
emr-pipeline/
├── module1_database/
│   ├── 01_create_schema.sql      # 建表 DDL（12字段，10+类型）
│   ├── 02_generate_data.py       # 生成1000行真实测试数据
│   └── 03_validate_data.sql      # 数据质量验证查询
├── module2_emr/
│   ├── pyspark_extract.py        # PySpark 抽取作业（主要提交版本）
│   ├── sparksql_extract.py       # SparkSQL 版本（可交互运行）
│   └── bootstrap_emr.sh          # EMR Bootstrap：安装 JDBC 驱动
├── infrastructure/
│   ├── main.tf                   # 全部 AWS 资源定义
│   ├── variables.tf              # 可配置参数
│   └── outputs.tf                # 输出值（端点、桶名等）
└── README.md                     # 本文件
```

---

## 模块一：数据库设计

### 表结构（ecommerce.orders，12个字段）

| 字段                | 类型             | 说明                          |
|---------------------|------------------|-------------------------------|
| `order_id`          | INTEGER (PK)     | 订单主键                      |
| `customer_id`       | VARCHAR(36)      | 客户UUID字符串                |
| `order_status`      | VARCHAR(20)      | 枚举：6种状态，CHECK约束      |
| `order_total`       | NUMERIC(12,2)    | 订单金额（精确小数）           |
| `item_count`        | INTEGER          | 商品数量                      |
| `product_category`  | VARCHAR(50)      | 商品类目（10种）              |
| `created_at`        | TIMESTAMPTZ      | 创建时间（带时区）             |
| `delivery_date`     | DATE             | 预计送达日期（仅日期）         |
| `is_loyalty_member` | BOOLEAN          | 是否会员                      |
| `rating`            | NUMERIC(3,1)     | 评分 1.0–5.0，可为NULL        |
| `metadata`          | JSONB            | 设备、优惠码等扩展字段         |
| `shipping_address`  | TEXT             | 配送地址（可为NULL）           |

覆盖类型：INTEGER, VARCHAR, NUMERIC, TIMESTAMPTZ, DATE, BOOLEAN, JSONB, TEXT

### 数据真实性设计

- 使用 `Faker('en_AU')` 生成符合澳洲地区格式的地址
- 订单状态按真实电商比例加权分布（delivered占50%，pending占5%）
- rating 仅对 delivered 订单随机生成，且评分分布向高分偏斜（符合现实）
- JSONB metadata 字段含 device / coupon_code / referral，None值自动剔除
- 使用 `random.seed(42)` 保证每次生成结果可复现

---

## 模块二：EMR 数据抽取

### 运行方式（最小验证路径）

**方式A：交互式（在 EMR primary node SSH 后）**

```bash
# 进入 primary node
ssh -i your-key.pem hadoop@<EMR_PRIMARY_DNS>

# 启动 PySpark shell（带 JDBC driver）
pyspark --jars /usr/lib/spark/jars/postgresql-42.7.3.jar

# 在 shell 中粘贴以下代码验证连通性
import boto3, json
from pyspark.sql import SparkSession

secret = json.loads(
    boto3.client("secretsmanager", region_name="ap-southeast-2")
        .get_secret_value(SecretId="emr-pipeline/rds/credentials")["SecretString"]
)
jdbc_url = f"jdbc:postgresql://{secret['host']}:5432/{secret['dbname']}"
props = {"user": secret["username"], "password": secret["password"],
         "driver": "org.postgresql.Driver", "ssl": "true", "sslmode": "require"}

df = spark.read.jdbc(url=jdbc_url, table="ecommerce.orders", properties=props)
df.show(5)
df.count()   # 期望输出: 1000
```

**方式B：提交作业**

```bash
spark-submit \
    --jars /usr/lib/spark/jars/postgresql-42.7.3.jar \
    s3://YOUR-BUCKET/jobs/pyspark_extract.py \
        --secret-name emr-pipeline/rds/credentials \
        --output-path  s3://YOUR-BUCKET/emr-pipeline/output/ \
        --region       ap-southeast-2
```

### JDBC 并行读取设计

```
order_id 1         250        500        750       1001
    |────────────|────────────|────────────|────────────|
    [  partition 1 ][ partition 2 ][ partition 3 ][ partition 4 ]
         ↓               ↓               ↓               ↓
    Spark Worker    Spark Worker    Spark Worker    Spark Worker
         └───────────────┴───────────────┴──────────────→ RDS
                    (最多 N 个并发连接)
```

- `column="order_id"` 作为分区列（整型PK，分布均匀）
- `numPartitions` 动态计算：`max(4, min(20, row_count // 100))`
- 每个分区独立发起 JDBC 连接，范围查询互不重叠

### 输出结构（S3 Parquet）

```
s3://BUCKET/output/
├── order_status=cancelled/   part-*.parquet
├── order_status=delivered/   part-*.parquet
├── order_status=pending/     part-*.parquet
├── order_status=processing/  part-*.parquet
├── order_status=refunded/    part-*.parquet
└── order_status=shipped/     part-*.parquet
```

按 `order_status` 分区，下游 Athena 查询特定状态时跳过无关分区（分区裁剪）。

---

## 工程设计说明

### 安全与权限

| 设计点 | 实现方式 |
|--------|----------|
| DB 凭证不硬编码 | 存入 Secrets Manager，运行时动态获取 |
| 最小权限 | `emr_reader` 用户仅有 `SELECT` on `ecommerce.orders` |
| IAM 权限范围 | EC2 Role 仅能访问指定 S3 桶 + 指定 Secret ARN |
| 数据库网络隔离 | RDS 在私有子网，`publicly_accessible = false` |
| 连接加密 | JDBC `sslmode=require`，传输层全程 TLS |
| S3 加密 | Server-side encryption (AES256) |
| S3 公共访问 | `block_public_acls = true` 全部封锁 |

### 网络设计

```
Internet
   │
   ▼
[Internet Gateway]
   │
[Public Subnet]  ←─ NAT Gateway（供私有子网出站使用）
   │
[Private Subnet] ←─ RDS + EMR nodes
   │                 ↑
   └─ Security Group 规则：
        EMR SG → RDS SG : port 5432 only
        （不开放 0.0.0.0/0）
   │
[VPC Endpoints]
   ├─ S3 Gateway Endpoint   （S3流量不过NAT）
   └─ Secrets Manager Interface Endpoint（SM调用不过NAT）
```

### 可重复部署

```bash
# 首次部署（约15分钟）
cd infrastructure/
terraform init
terraform apply -var="environment=dev"

# 数据初始化（RDS 启动后）
export DB_HOST=$(terraform output -raw rds_endpoint)
export DB_USER=dbadmin
export DB_PASSWORD=<from secrets manager>
export DB_NAME=ordersdb

# 1. 建表
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -f ../module1_database/01_create_schema.sql

# 2. 生成数据
pip install psycopg2-binary faker boto3
python ../module1_database/02_generate_data.py --rows 1000

# 3. 提交 Spark 作业
BUCKET=$(terraform output -raw s3_bucket_name)
spark-submit \
    --jars /usr/lib/spark/jars/postgresql-42.7.3.jar \
    s3://$BUCKET/jobs/pyspark_extract.py \
    --secret-name emr-pipeline/rds/credentials \
    --output-path s3://$BUCKET/emr-pipeline/output/ \
    --region ap-southeast-2

# 拆除（避免持续费用）
terraform destroy
```

### 设计取舍说明

**选择 PostgreSQL 而非 MySQL**
PostgreSQL 的 JSONB 类型（字段11）原生支持 GIN 索引，比 MySQL 的 JSON 类型更适合展示多类型字段设计；同时 Spark JDBC 对 JSONB 的读取处理也更直接。

**RDS 单 AZ vs Multi-AZ**
本 demo 使用单 AZ（`multi_az = false`）以节省成本。生产环境应改为 `true` 以保证 RDS 故障切换。

**EMR 按需实例 vs Spot**
当前使用按需实例确保稳定性。生产/批处理场景可将 core 节点改为 Spot 以节约 60–70% 费用，但需要处理 Spot 中断的重试逻辑。

**Parquet 按 order_status 分区**
对下游查询（"查所有已取消订单"）有利，但对全表扫描效率无影响。若下游主要按 `created_at` 过滤，应改用日期分区。

**SparkSQL vs PySpark DataFrame API**
两种实现均已提供：SparkSQL 版本可在 pyspark shell 中交互式粘贴运行，适合快速验证；PySpark 版本有更完整的错误处理和质量检查，适合作为正式 EMR Step 提交。

---

## 质量检查项

Spark 作业运行后自动执行以下检查，任一失败则 job exit code = 1：

1. **行数检查**：抽取行数 ≥ 900（容许少量 seed 差异）
2. **非负金额**：`order_total` 无 NULL 且全部 ≥ 0
3. **状态非空**：`order_status` 无 NULL 行

---

## 依赖版本

| 组件 | 版本 |
|------|------|
| EMR Release | 6.15.0 |
| Spark | 3.4.x（含于 EMR 6.15.0）|
| PostgreSQL JDBC | 42.7.3 |
| Python | 3.9+（EMR 6.x 自带）|
| Terraform | ≥ 1.5 |
| Faker（数据生成）| ≥ 24.0 |
| psycopg2-binary | ≥ 2.9 |

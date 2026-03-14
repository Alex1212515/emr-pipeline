# EMR Pipeline – RDS to S3 Data Extraction

A small data extraction pipeline that reads from an AWS RDS PostgreSQL database using PySpark on AWS EMR and writes the output to S3 as partitioned Parquet files.

## Architecture

```
RDS PostgreSQL (ecommerce.orders)
        ↓  JDBC over SSL
EMR Cluster (PySpark 3.5.6)
        ↓  Parquet write
S3 Bucket (partitioned by order_status)
```

## Tech Stack

- **Database**: AWS RDS PostgreSQL 15
- **Compute**: AWS EMR 7.12.0 (Spark 3.5.6)
- **Storage**: AWS S3 (Parquet format)
- **IaC**: Terraform
- **Language**: Python / SQL

## Project Structure

```
├── module1_database/
│   ├── 01_create_schema.sql      # Table DDL (12 fields, 8 types)
│   ├── 02_generate_data.py       # Generate 1000 rows of test data
│   ├── 03_validate_data.sql      # Data quality checks
│   └── 04_create_app_user.sql    # Read-only app user setup
├── module2_emr/
│   ├── pyspark_extract.py        # Main PySpark extraction job
│   ├── sparksql_extract.py       # SparkSQL alternative (interactive)
│   └── bootstrap_emr.sh          # EMR bootstrap: install JDBC driver
├── infrastructure/
│   ├── main.tf                   # AWS resources (VPC, RDS, EMR, S3, IAM)
│   ├── variables.tf
│   └── outputs.tf
└── README.md
```

## Quick Start

### Module 1 – Database Setup

```bash
# Create table
psql -h <RDS_HOST> -U dbadmin -d ordersdb -f module1_database/01_create_schema.sql

# Generate 1000 rows
pip install psycopg2-binary faker
python module1_database/02_generate_data.py --rows 1000
```

### Module 2 – EMR Extraction

```bash
# SSH into EMR primary node
ssh -i emr-pipeline-key.pem hadoop@<EMR_DNS>

# Install JDBC driver
sudo wget https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
    -O /usr/lib/spark/jars/postgresql-42.7.3.jar

# Start PySpark
pyspark --jars /usr/lib/spark/jars/postgresql-42.7.3.jar

# In PySpark shell
jdbc_url = "jdbc:postgresql://<RDS_HOST>:5432/ordersdb"
props = {"user": "dbadmin", "password": "***", "driver": "org.postgresql.Driver"}
df = spark.read.jdbc(url=jdbc_url, table="ecommerce.orders", properties=props)
df.show(5)
df.count()  # Expected: 1000
```

### Infrastructure (Terraform)

```bash
cd infrastructure/
terraform init
terraform apply -var="environment=dev"
```

## Security

- RDS deployed in private subnet (`publicly_accessible = false`)
- JDBC connection encrypted with `sslmode=require`
- Read-only database user `emr_reader` with SELECT-only permissions
- EMR EC2 IAM Role scoped to specific S3 bucket and Secret ARN
- Security group rules: EMR SG → RDS SG on port 5432 only

#!/usr/bin/env python3
"""
Module 2 – EMR PySpark Extraction Job
======================================
Connects to the PostgreSQL RDS instance from inside EMR,
reads ecommerce.orders via JDBC, applies lightweight transformations,
runs quality checks, and writes the result to S3 as Parquet.

Submission command (from EMR primary node):
    spark-submit \
        --jars /usr/lib/spark/jars/postgresql-42.7.3.jar \
        --conf spark.hadoop.fs.s3.impl=com.amazon.ws.emr.hadoop.fs.EmrFileSystem \
        pyspark_extract.py \
            --secret-name emr-pipeline/rds/credentials \
            --output-path  s3://YOUR-BUCKET/emr-pipeline/output/ \
            --region       ap-southeast-2

Design decisions:
    - JDBC parallelism: partitioned on order_id (integer PK) so Spark
      launches multiple parallel readers against RDS, avoiding a single
      serial fetch. numPartitions=10 keeps RDS connection count low.
    - Credentials: never passed as CLI args or embedded in code.
      Retrieved from Secrets Manager at driver startup; worker nodes
      receive only the connection string (password in JDBC URL, in memory).
    - Output: Parquet on S3 with snappy compression. Downstream Athena
      or Glue jobs can query it directly without re-processing.
    - Quality checks: row count, null rate, total-amount sanity.
      Job exits with code 1 on failure so EMR Step status reflects it.
"""

import argparse
import json
import logging
import sys

import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("emr-extract")


# ── Credential helpers ────────────────────────────────────────────────────────

def get_jdbc_url_and_props(secret_name: str, region: str) -> tuple[str, dict]:
    """
    Fetch RDS credentials from Secrets Manager.
    Returns (jdbc_url, connection_properties_dict).

    Keeping the password inside connection_properties (not the URL) avoids
    it appearing in Spark UI query plans or logs.
    """
    client = boto3.client("secretsmanager", region_name=region)
    secret  = json.loads(client.get_secret_value(SecretId=secret_name)["SecretString"])

    host   = secret["host"]
    port   = secret.get("port", 5432)
    dbname = secret["dbname"]
    user   = secret["username"]
    passwd = secret["password"]

    jdbc_url = f"jdbc:postgresql://{host}:{port}/{dbname}"
    props = {
        "user":     user,
        "password": passwd,
        "driver":   "org.postgresql.Driver",
        # SSL: require encrypted connections to RDS
        "ssl":              "true",
        "sslmode":          "require",
        # Connection pool settings for JDBC partition readers
        "fetchsize":        "5000",    # rows fetched per round-trip
        "socketTimeout":    "120",
        "connectTimeout":   "30",
    }
    return jdbc_url, props


# ── Spark job ─────────────────────────────────────────────────────────────────

def run(args):
    log.info("Initialising SparkSession …")
    spark = (
        SparkSession.builder
        .appName("emr-pipeline-extract")
        # Pushdown predicates to RDS where possible
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ── 1. Read credentials ──────────────────────────────────────────────────
    log.info(f"Fetching credentials from Secrets Manager: {args.secret_name}")
    jdbc_url, jdbc_props = get_jdbc_url_and_props(args.secret_name, args.region)

    # ── 2. Get row count for partition planning ──────────────────────────────
    log.info("Querying row count for partition planning …")
    count_df = (
        spark.read.jdbc(
            url=jdbc_url,
            table="(SELECT COUNT(*) AS n FROM ecommerce.orders) AS cnt",
            properties=jdbc_props,
        )
    )
    total_rows = count_df.collect()[0]["n"]
    log.info(f"  Total rows in source: {total_rows}")

    num_partitions = max(4, min(20, total_rows // 100))   # 4–20 partitions
    log.info(f"  Using {num_partitions} JDBC partitions")

    # ── 3. Parallel JDBC read with partition column ──────────────────────────
    log.info("Reading ecommerce.orders via JDBC …")
    raw_df = (
        spark.read.jdbc(
            url=jdbc_url,
            table="ecommerce.orders",
            column="order_id",          # numeric PK → evenly distributable
            lowerBound=1,
            upperBound=total_rows + 1,
            numPartitions=num_partitions,
            properties=jdbc_props,
        )
    )

    raw_df.cache()
    log.info(f"  Schema:")
    raw_df.printSchema()

    # ── 4. Transformations ───────────────────────────────────────────────────
    log.info("Applying transformations …")

    transformed_df = (
        raw_df
        # Derive revenue tier based on order_total
        .withColumn(
            "revenue_tier",
            F.when(F.col("order_total") < 50,   F.lit("low"))
             .when(F.col("order_total") < 200,  F.lit("mid"))
             .otherwise(F.lit("high"))
        )
        # Extract JSONB device field into its own column
        .withColumn("device", F.get_json_object(F.col("metadata"), "$.device"))
        # Truncate created_at to date for partitioning
        .withColumn("order_date", F.to_date(F.col("created_at")))
        # Boolean to integer for downstream BI tools that don't support bool
        .withColumn("loyalty_flag", F.col("is_loyalty_member").cast("integer"))
        # Normalise rating: null → 0.0 for aggregation safety
        .withColumn(
            "rating_safe",
            F.coalesce(F.col("rating"), F.lit(0.0))
        )
    )

    # ── 5. Quality checks ────────────────────────────────────────────────────
    log.info("Running quality checks …")

    actual_count = transformed_df.count()
    log.info(f"  Row count: {actual_count}")

    # Check 1: row count must be > 900 (allow for seed variance)
    if actual_count < 900:
        log.error(f"QC FAIL: row count {actual_count} < 900")
        sys.exit(1)

    # Check 2: order_total must have no nulls and be >= 0
    null_total = transformed_df.filter(
        F.col("order_total").isNull() | (F.col("order_total") < 0)
    ).count()
    if null_total > 0:
        log.error(f"QC FAIL: {null_total} rows with null/negative order_total")
        sys.exit(1)

    # Check 3: null rate on order_status must be 0
    null_status = transformed_df.filter(F.col("order_status").isNull()).count()
    if null_status > 0:
        log.error(f"QC FAIL: {null_status} rows with null order_status")
        sys.exit(1)

    log.info("  All quality checks passed ✓")

    # ── 6. Summary statistics (logged for verification) ──────────────────────
    log.info("Summary statistics:")
    transformed_df.groupBy("order_status").count().orderBy("count", ascending=False).show()
    transformed_df.groupBy("revenue_tier").count().show()
    transformed_df.select(
        F.round(F.avg("order_total"), 2).alias("avg_total"),
        F.round(F.sum("order_total"), 2).alias("sum_total"),
        F.avg("rating_safe").alias("avg_rating"),
    ).show()

    # ── 7. Write to S3 as Parquet ────────────────────────────────────────────
    output_path = args.output_path.rstrip("/")
    log.info(f"Writing Parquet output to {output_path} …")

    (
        transformed_df
        .drop("metadata")            # raw JSON kept as-is only if needed downstream
        .repartition(4)              # consolidate small files
        .write
        .mode("overwrite")
        .partitionBy("order_status") # partition for selective downstream reads
        .parquet(output_path)
    )

    log.info("Job complete. Output written successfully.")
    spark.stop()


# ── Entry point ───────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="EMR PySpark extraction job")
    p.add_argument("--secret-name",  required=True, help="Secrets Manager secret name")
    p.add_argument("--output-path",  required=True, help="S3 output path (s3://bucket/prefix/)")
    p.add_argument("--region",       default="ap-southeast-2", help="AWS region")
    return p.parse_args()


if __name__ == "__main__":
    run(parse_args())

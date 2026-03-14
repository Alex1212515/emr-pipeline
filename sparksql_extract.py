#!/usr/bin/env python3
"""
Module 2 – SparkSQL Extraction (Interactive / spark-shell compatible)
=====================================================================
Alternative to pyspark_extract.py using SparkSQL syntax.
Can be run interactively in pyspark shell or as a script.

Run in interactive mode (on EMR primary node):
    pyspark --jars /usr/lib/spark/jars/postgresql-42.7.3.jar

Then paste or %run this file.

Or as a submitted job:
    spark-submit \
        --jars /usr/lib/spark/jars/postgresql-42.7.3.jar \
        sparksql_extract.py \
            --secret-name emr-pipeline/rds/credentials \
            --output-path  s3://YOUR-BUCKET/emr-pipeline/output-sql/ \
            --region       ap-southeast-2
"""

import argparse
import json
import logging
import sys

import boto3
from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("sparksql-extract")


def get_jdbc_url_and_props(secret_name: str, region: str) -> tuple[str, dict]:
    client = boto3.client("secretsmanager", region_name=region)
    secret  = json.loads(client.get_secret_value(SecretId=secret_name)["SecretString"])
    jdbc_url = f"jdbc:postgresql://{secret['host']}:{secret.get('port', 5432)}/{secret['dbname']}"
    props = {
        "user":           secret["username"],
        "password":       secret["password"],
        "driver":         "org.postgresql.Driver",
        "ssl":            "true",
        "sslmode":        "require",
        "fetchsize":      "5000",
    }
    return jdbc_url, props


def run(args):
    spark = (
        SparkSession.builder
        .appName("emr-pipeline-sparksql-extract")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    jdbc_url, props = get_jdbc_url_and_props(args.secret_name, args.region)

    # ── Load raw table into a Spark temporary view ────────────────────────────
    log.info("Loading ecommerce.orders into Spark temp view …")
    (
        spark.read.jdbc(
            url=jdbc_url,
            table="ecommerce.orders",
            column="order_id",
            lowerBound=1,
            upperBound=1001,
            numPartitions=10,
            properties=props,
        )
        .createOrReplaceTempView("orders_raw")
    )

    log.info("Temp view 'orders_raw' registered.")

    # ── SparkSQL transformations ──────────────────────────────────────────────
    transformed = spark.sql("""
        SELECT
            order_id,
            customer_id,
            order_status,
            CAST(order_total AS DOUBLE)                             AS order_total,
            item_count,
            product_category,
            created_at,
            delivery_date,
            is_loyalty_member,
            rating,
            -- Revenue tier derived column
            CASE
                WHEN order_total < 50   THEN 'low'
                WHEN order_total < 200  THEN 'mid'
                ELSE                         'high'
            END                                                     AS revenue_tier,
            -- Extract device from JSONB metadata
            get_json_object(metadata, '$.device')                   AS device,
            -- Extract coupon from metadata
            get_json_object(metadata, '$.coupon_code')              AS coupon_code,
            -- Date-only column for partitioning
            CAST(created_at AS DATE)                                AS order_date,
            -- Null-safe rating
            COALESCE(rating, 0.0)                                   AS rating_safe,
            -- Boolean to int
            CAST(is_loyalty_member AS INT)                          AS loyalty_flag,
            shipping_address
        FROM orders_raw
    """)

    transformed.createOrReplaceTempView("orders_transformed")

    # ── Quality check queries via SparkSQL ────────────────────────────────────
    log.info("Running quality checks via SparkSQL …")

    qc1 = spark.sql("SELECT COUNT(*) AS n FROM orders_transformed").collect()[0]["n"]
    log.info(f"  Row count: {qc1}")
    if qc1 < 900:
        log.error(f"QC FAIL: only {qc1} rows")
        sys.exit(1)

    qc2 = spark.sql(
        "SELECT COUNT(*) AS n FROM orders_transformed WHERE order_total IS NULL OR order_total < 0"
    ).collect()[0]["n"]
    if qc2 > 0:
        log.error(f"QC FAIL: {qc2} bad order_total rows")
        sys.exit(1)

    log.info("  Quality checks passed ✓")

    # ── Summary via SparkSQL ──────────────────────────────────────────────────
    log.info("Order status breakdown:")
    spark.sql("""
        SELECT order_status, COUNT(*) AS cnt,
               ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct
        FROM orders_transformed
        GROUP BY order_status
        ORDER BY cnt DESC
    """).show()

    log.info("Revenue tier breakdown:")
    spark.sql("""
        SELECT revenue_tier,
               COUNT(*)                    AS order_count,
               ROUND(AVG(order_total), 2)  AS avg_total,
               ROUND(SUM(order_total), 2)  AS total_revenue
        FROM orders_transformed
        GROUP BY revenue_tier
        ORDER BY total_revenue DESC
    """).show()

    log.info("Category performance:")
    spark.sql("""
        SELECT product_category,
               COUNT(*)                    AS orders,
               ROUND(AVG(order_total), 2)  AS avg_order_value,
               ROUND(AVG(rating_safe), 2)  AS avg_rating
        FROM orders_transformed
        GROUP BY product_category
        ORDER BY orders DESC
        LIMIT 10
    """).show()

    # ── Write to S3 ───────────────────────────────────────────────────────────
    output_path = args.output_path.rstrip("/")
    log.info(f"Writing to {output_path} …")

    spark.sql("""
        SELECT * FROM orders_transformed
    """).repartition(4) \
      .write \
      .mode("overwrite") \
      .partitionBy("order_status") \
      .parquet(output_path)

    log.info("Done.")
    spark.stop()


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--secret-name", required=True)
    p.add_argument("--output-path", required=True)
    p.add_argument("--region",      default="ap-southeast-2")
    return p.parse_args()


if __name__ == "__main__":
    run(parse_args())

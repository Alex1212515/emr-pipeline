#!/usr/bin/env python3
"""
Module 1 – Data Generator
=========================
Generates ~1000 realistic e-commerce order rows and inserts them
into the PostgreSQL RDS instance.

Usage:
    # Install deps (once)
    pip install psycopg2-binary faker boto3

    # Run (credentials pulled from environment or Secrets Manager)
    python generate_data.py --rows 1000 [--use-secrets-manager]

Design notes:
    - Uses Faker for realistic names/addresses so data is human-readable
      during manual inspection and not just random noise.
    - Batch-inserts in chunks of 100 to reduce round-trips.
    - Supports both local .env credentials and AWS Secrets Manager so
      the same script works in dev (laptop) and prod (EMR bootstrap).
"""

import argparse
import json
import os
import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import psycopg2
from psycopg2.extras import execute_values
from faker import Faker

fake = Faker('en_AU')   # Australian locale for realistic AUD context
random.seed(42)         # Reproducibility


# ── Configuration ────────────────────────────────────────────────────────────

PRODUCT_CATEGORIES = [
    "Electronics", "Clothing", "Home & Garden", "Sports & Outdoors",
    "Books", "Toys & Games", "Beauty & Health", "Automotive",
    "Food & Grocery", "Pet Supplies",
]

ORDER_STATUSES = [
    ("pending",     0.05),
    ("processing",  0.10),
    ("shipped",     0.20),
    ("delivered",   0.50),
    ("cancelled",   0.10),
    ("refunded",    0.05),
]
STATUS_WEIGHTS  = [w for _, w in ORDER_STATUSES]
STATUS_VALUES   = [s for s, _ in ORDER_STATUSES]

DEVICES = ["mobile", "desktop", "tablet", "app-ios", "app-android"]
COUPONS = [None, None, None, "SAVE10", "WELCOME20", "FLASH15", "MEMBER5"]


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_db_credentials(use_secrets_manager: bool) -> dict:
    """
    Retrieve DB credentials.
    Priority: Secrets Manager (prod) → environment variables (dev/CI).
    """
    if use_secrets_manager:
        import boto3
        client = boto3.client("secretsmanager", region_name=os.environ.get("AWS_REGION", "ap-southeast-2"))
        secret_name = os.environ.get("DB_SECRET_NAME", "emr-pipeline/rds/credentials")
        resp = client.get_secret_value(SecretId=secret_name)
        creds = json.loads(resp["SecretString"])
        return {
            "host":     creds["host"],
            "port":     int(creds.get("port", 5432)),
            "dbname":   creds["dbname"],
            "user":     creds["username"],
            "password": creds["password"],
        }
    else:
        # Fall back to environment variables (set in .env or shell)
        return {
            "host":     os.environ["DB_HOST"],
            "port":     int(os.environ.get("DB_PORT", 5432)),
            "dbname":   os.environ.get("DB_NAME", "ordersdb"),
            "user":     os.environ["DB_USER"],
            "password": os.environ["DB_PASSWORD"],
        }


def random_order(order_id: int) -> tuple:
    """Build one realistic order row as a tuple matching INSERT column order."""
    now = datetime.now(timezone.utc)

    # created_at: random moment in the past 2 years
    created_at = now - timedelta(
        days=random.randint(0, 730),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )

    status = random.choices(STATUS_VALUES, STATUS_WEIGHTS)[0]

    # delivery_date only makes sense for shipped/delivered orders
    delivery_date = None
    if status in ("shipped", "delivered"):
        delivery_date = (created_at + timedelta(days=random.randint(1, 14))).date()

    item_count    = random.randint(1, 20)
    unit_price    = round(random.uniform(5.0, 500.0), 2)
    order_total   = round(unit_price * item_count * random.uniform(0.85, 1.0), 2)

    is_loyalty    = random.random() < 0.35

    # Ratings only present for delivered orders (~80% leave a rating)
    rating = None
    if status == "delivered" and random.random() < 0.80:
        rating = round(random.choices(
            [1.0, 2.0, 3.0, 3.5, 4.0, 4.5, 5.0],
            weights=[2, 3, 8, 10, 20, 25, 32]
        )[0], 1)

    metadata = {
        "device":       random.choice(DEVICES),
        "coupon_code":  random.choice(COUPONS),
        "ip_country":   fake.country_code(),
        "referral":     random.choice([None, "google", "email", "social", "direct"]),
    }
    # Remove None values from JSON to keep it clean
    metadata = {k: v for k, v in metadata.items() if v is not None}

    shipping_address = fake.address().replace("\n", ", ") if status != "cancelled" else None

    return (
        order_id,
        str(fake.uuid4()),          # customer_id
        status,
        Decimal(str(order_total)),
        item_count,
        random.choice(PRODUCT_CATEGORIES),
        created_at,
        delivery_date,
        is_loyalty,
        Decimal(str(rating)) if rating else None,
        json.dumps(metadata),
        shipping_address,
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Seed ecommerce.orders with fake data")
    parser.add_argument("--rows",                type=int,  default=1000, help="Number of rows to insert")
    parser.add_argument("--batch-size",          type=int,  default=100,  help="Insert batch size")
    parser.add_argument("--use-secrets-manager", action="store_true",     help="Pull creds from AWS Secrets Manager")
    args = parser.parse_args()

    print(f"[INFO] Connecting to database …")
    creds = get_db_credentials(args.use_secrets_manager)

    conn = psycopg2.connect(**creds, connect_timeout=10)
    conn.autocommit = False
    cur = conn.cursor()

    print(f"[INFO] Truncating existing data …")
    cur.execute("TRUNCATE TABLE ecommerce.orders RESTART IDENTITY;")

    INSERT_SQL = """
        INSERT INTO ecommerce.orders (
            order_id, customer_id, order_status, order_total, item_count,
            product_category, created_at, delivery_date, is_loyalty_member,
            rating, metadata, shipping_address
        ) VALUES %s
    """

    rows_generated = 0
    batch = []

    print(f"[INFO] Generating {args.rows} rows (batch size {args.batch_size}) …")
    for i in range(1, args.rows + 1):
        batch.append(random_order(i))
        if len(batch) >= args.batch_size:
            execute_values(cur, INSERT_SQL, batch)
            rows_generated += len(batch)
            print(f"  … inserted {rows_generated}/{args.rows}")
            batch = []

    if batch:                          # flush remainder
        execute_values(cur, INSERT_SQL, batch)
        rows_generated += len(batch)

    conn.commit()
    cur.close()
    conn.close()

    print(f"[INFO] Done. {rows_generated} rows inserted into ecommerce.orders.")


if __name__ == "__main__":
    main()

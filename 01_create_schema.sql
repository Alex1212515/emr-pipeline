-- ============================================================
-- Module 1: Database Schema Design
-- Database: PostgreSQL (RDS)
-- Table: ecommerce.orders
-- Fields: 12 columns covering diverse data types
-- ============================================================

-- Create schema
CREATE SCHEMA IF NOT EXISTS ecommerce;

-- Drop table if re-running
DROP TABLE IF EXISTS ecommerce.orders;

-- ============================================================
-- Table design rationale:
--   Simulates an e-commerce order dataset with realistic
--   field diversity: identifiers, categories, numerics,
--   timestamps, booleans, JSON metadata, and text blobs.
--   This ensures Spark type mapping coverage is non-trivial.
-- ============================================================
CREATE TABLE ecommerce.orders (
    -- (1) INTEGER: surrogate primary key
    order_id          INTEGER         PRIMARY KEY,

    -- (2) VARCHAR: customer identifier (UUID-style string)
    customer_id       VARCHAR(36)     NOT NULL,

    -- (3) VARCHAR with enum-like constraint: order status
    order_status      VARCHAR(20)     NOT NULL
                          CHECK (order_status IN (
                              'pending','processing','shipped',
                              'delivered','cancelled','refunded'
                          )),

    -- (4) NUMERIC(12,2): order total in AUD
    order_total       NUMERIC(12,2)   NOT NULL CHECK (order_total >= 0),

    -- (5) INTEGER: number of items in the order
    item_count        INTEGER         NOT NULL CHECK (item_count > 0),

    -- (6) VARCHAR: product category
    product_category  VARCHAR(50)     NOT NULL,

    -- (7) TIMESTAMP WITH TIME ZONE: order creation time
    created_at        TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    -- (8) DATE: expected delivery date (date only, no time)
    delivery_date     DATE,

    -- (9) BOOLEAN: whether the customer is a loyalty member
    is_loyalty_member BOOLEAN         NOT NULL DEFAULT FALSE,

    -- (10) NUMERIC(3,1): customer satisfaction score 1.0–5.0
    rating            NUMERIC(3,1)    CHECK (rating BETWEEN 1.0 AND 5.0),

    -- (11) JSONB: flexible metadata (device, coupon, notes)
    metadata          JSONB,

    -- (12) TEXT: free-form shipping address (nullable)
    shipping_address  TEXT
);

-- Indexes to support realistic query patterns
CREATE INDEX idx_orders_customer    ON ecommerce.orders (customer_id);
CREATE INDEX idx_orders_status      ON ecommerce.orders (order_status);
CREATE INDEX idx_orders_created_at  ON ecommerce.orders (created_at);
CREATE INDEX idx_orders_category    ON ecommerce.orders (product_category);
CREATE INDEX idx_orders_metadata    ON ecommerce.orders USING GIN (metadata);

-- Grant read access to the EMR application role
-- Replace 'emr_reader' with your actual IAM-authenticated DB user
GRANT USAGE  ON SCHEMA ecommerce    TO emr_reader;
GRANT SELECT ON ecommerce.orders    TO emr_reader;

COMMENT ON TABLE ecommerce.orders IS
    'E-commerce orders table for EMR pipeline demo. ~1000 rows, 12 typed columns.';

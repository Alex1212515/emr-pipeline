-- ============================================================
-- Module 1: Data Validation Queries
-- Run these after data generation to verify correctness.
-- ============================================================

-- 1. Row count (expect ~1000)
SELECT COUNT(*) AS total_rows FROM ecommerce.orders;

-- 2. Column type summary (shows actual pg types)
SELECT
    column_name,
    data_type,
    character_maximum_length,
    numeric_precision,
    numeric_scale,
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'ecommerce'
  AND table_name   = 'orders'
ORDER BY ordinal_position;

-- 3. Status distribution (check weights are roughly correct)
SELECT
    order_status,
    COUNT(*)                                              AS cnt,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1)  AS pct
FROM ecommerce.orders
GROUP BY order_status
ORDER BY cnt DESC;

-- 4. Nullability sanity check
SELECT
    COUNT(*)                                    AS total,
    COUNT(*) FILTER (WHERE rating         IS NULL) AS null_rating,
    COUNT(*) FILTER (WHERE delivery_date  IS NULL) AS null_delivery,
    COUNT(*) FILTER (WHERE shipping_address IS NULL) AS null_address,
    COUNT(*) FILTER (WHERE metadata       IS NULL) AS null_metadata
FROM ecommerce.orders;

-- 5. Numeric range checks
SELECT
    MIN(order_total)  AS min_total,
    MAX(order_total)  AS max_total,
    AVG(order_total)  AS avg_total,
    MIN(item_count)   AS min_items,
    MAX(item_count)   AS max_items,
    MIN(rating)       AS min_rating,
    MAX(rating)       AS max_rating
FROM ecommerce.orders;

-- 6. JSONB field check – device distribution
SELECT
    metadata->>'device'  AS device,
    COUNT(*)             AS cnt
FROM ecommerce.orders
WHERE metadata IS NOT NULL
GROUP BY 1
ORDER BY cnt DESC;

-- 7. Date range check
SELECT
    MIN(created_at)   AS earliest_order,
    MAX(created_at)   AS latest_order,
    MIN(delivery_date) AS earliest_delivery,
    MAX(delivery_date) AS latest_delivery
FROM ecommerce.orders;

-- 8. Sample 5 rows for manual eyeball test
SELECT * FROM ecommerce.orders ORDER BY random() LIMIT 5;

-- ──────────────────────────────────────────────────────────────
--  Mini Data Platform — PostgreSQL Initialisation
--  Runs once on first container start.
--  All credentials are injected via Docker env vars from .env
-- ──────────────────────────────────────────────────────────────

-- ── Airflow DB ────────────────────────────────────────────────
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;

-- ── Metabase DB ───────────────────────────────────────────────
CREATE DATABASE metabase OWNER admin;

-- ── Sales DB ──────────────────────────────────────────────────
CREATE USER datauser WITH PASSWORD 'datapass';
CREATE DATABASE salesdb OWNER datauser;

\connect salesdb

GRANT ALL PRIVILEGES ON DATABASE salesdb TO datauser;

-- ── Schema ────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dim_products (
    product_id   SERIAL PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category     VARCHAR(100) NOT NULL,
    unit_price   NUMERIC(10, 2) NOT NULL,
    created_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id   SERIAL PRIMARY KEY,
    customer_name VARCHAR(255) NOT NULL,
    email         VARCHAR(255) UNIQUE,
    region        VARCHAR(100),
    created_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id      SERIAL PRIMARY KEY,
    order_id     VARCHAR(50) UNIQUE NOT NULL,
    sale_date    DATE NOT NULL,
    customer_id  INT REFERENCES dim_customers(customer_id),
    product_id   INT REFERENCES dim_products(product_id),
    quantity     INT NOT NULL,
    unit_price   NUMERIC(10, 2) NOT NULL,
    discount     NUMERIC(5, 2) DEFAULT 0,
    total_amount NUMERIC(12, 2) GENERATED ALWAYS AS
                   (quantity * unit_price * (1 - discount / 100)) STORED,
    region       VARCHAR(100),
    channel      VARCHAR(50),
    processed_at TIMESTAMP DEFAULT NOW(),
    source_file  VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id        SERIAL PRIMARY KEY,
    dag_id        VARCHAR(255),
    run_date      TIMESTAMP DEFAULT NOW(),
    source_file   VARCHAR(255),
    rows_loaded   INT,
    status        VARCHAR(50),
    error_message TEXT
);

-- ── Materialised view ─────────────────────────────────────────
-- Must be created by datauser so they can REFRESH it later
SET ROLE datauser;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_monthly_sales AS
SELECT
    DATE_TRUNC('month', sale_date) AS month,
    region,
    channel,
    p.category,
    COUNT(*)          AS total_orders,
    SUM(quantity)     AS total_units,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_order_value
FROM fact_sales fs
JOIN dim_products p USING (product_id)
GROUP BY 1, 2, 3, 4;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_monthly_sales
    ON mv_monthly_sales (month, region, channel, category);

RESET ROLE;

-- ── Analytical views ──────────────────────────────────────────

CREATE OR REPLACE VIEW v_product_performance AS
SELECT
    p.product_name,
    p.category,
    COUNT(fs.sale_id)    AS total_orders,
    SUM(fs.quantity)     AS total_units_sold,
    SUM(fs.total_amount) AS total_revenue,
    AVG(fs.unit_price)   AS avg_selling_price,
    AVG(fs.discount)     AS avg_discount_pct
FROM dim_products p
LEFT JOIN fact_sales fs USING (product_id)
GROUP BY p.product_id, p.product_name, p.category;

CREATE OR REPLACE VIEW v_regional_performance AS
SELECT
    region,
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT(sale_id)               AS total_orders,
    SUM(total_amount)            AS total_revenue,
    AVG(total_amount)            AS avg_order_value
FROM fact_sales
GROUP BY region;

-- ── Permissions ───────────────────────────────────────────────
GRANT ALL ON ALL TABLES    IN SCHEMA public TO datauser;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO datauser;

-- ── Seed product catalogue ────────────────────────────────────
INSERT INTO dim_products (product_name, category, unit_price) VALUES
    ('Laptop Pro 15',       'Electronics', 1299.99),
    ('Wireless Mouse',      'Electronics',   29.99),
    ('USB-C Hub',           'Electronics',   49.99),
    ('Mechanical Keyboard', 'Electronics',  129.99),
    ('4K Monitor',          'Electronics',  399.99),
    ('Office Chair',        'Furniture',    299.99),
    ('Standing Desk',       'Furniture',    599.99),
    ('Bookshelf',           'Furniture',    149.99),
    ('Notebook Set',        'Stationery',    12.99),
    ('Pen Pack (10)',       'Stationery',     8.99),
    ('Whiteboard',          'Stationery',    79.99),
    ('Coffee Maker',        'Appliances',   129.99),
    ('Water Purifier',      'Appliances',   199.99),
    ('Air Purifier',        'Appliances',   249.99)
ON CONFLICT DO NOTHING;
CREATE SCHEMA raw;
CREATE SCHEMA staging;
CREATE SCHEMA dwh;

--buat tabel raw 
CREATE TABLE raw.raw_orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT,
    order_status TEXT,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE raw.raw_order_items (
    order_id TEXT,
    order_item_id INT,
    product_id TEXT,
    seller_id TEXT,
    price NUMERIC,
    freight_value NUMERIC,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE raw.raw_payments (
    order_id TEXT,
    payment_sequential INT,
    payment_type TEXT,
    payment_installments INT,
    payment_value NUMERIC,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


--buat staging tabde
CREATE TABLE staging.stg_orders AS
SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp
FROM raw.raw_orders
WHERE order_status IS NOT NULL;

-- buat data tabel warehouse

CREATE TABLE dwh.dim_customer AS
SELECT DISTINCT
    customer_id
FROM staging.stg_orders;

CREATE TABLE dwh.dim_product AS
SELECT DISTINCT
    product_id
FROM raw.raw_order_items;

CREATE TABLE dwh.dim_seller AS
SELECT DISTINCT
    seller_id
FROM raw.raw_order_items;

CREATE TABLE dwh.fact_sales AS
SELECT
    o.order_id,
    o.customer_id,
    i.product_id,
    i.seller_id,
    p.payment_type,
    i.price,
    i.freight_value,
    p.payment_value,
    o.order_purchase_timestamp
FROM raw.raw_orders o
JOIN raw.raw_order_items i
    ON o.order_id = i.order_id
JOIN raw.raw_payments p
    ON o.order_id = p.order_id;

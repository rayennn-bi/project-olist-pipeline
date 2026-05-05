import os
import psycopg2
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from scripts.common.spark_session import create_spark
from scripts.common.db import get_jdbc_config


def prepare_target_table() -> None:
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "olist_dw"),
        user=os.getenv("POSTGRES_USER", "airflow"),
        password=os.getenv("POSTGRES_PASSWORD", "airflow"),
    )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("CREATE SCHEMA IF NOT EXISTS warehouse;")
    cur.execute("DROP TABLE IF EXISTS warehouse.fact_sales CASCADE;")
    cur.execute("""
    CREATE TABLE warehouse.fact_sales (
        fact_sales_key INT PRIMARY KEY,
        order_id TEXT,
        order_item_id INT,
        customer_key INT,
        product_key INT,
        seller_key INT,
        customer_id TEXT,
        product_id TEXT,
        seller_id TEXT,
        order_status TEXT,
        order_purchase_timestamp TIMESTAMP,
        order_approved_at TIMESTAMP,
        order_delivered_carrier_date TIMESTAMP,
        order_delivered_customer_date TIMESTAMP,
        order_estimated_delivery_date TIMESTAMP,
        shipping_limit_date TIMESTAMP,
        price DOUBLE PRECISION,
        freight_value DOUBLE PRECISION,
        gross_sales_value DOUBLE PRECISION,
        delivery_days INT,
        estimated_delivery_days INT,
        is_delayed INT
    );
    """)

    cur.close()
    conn.close()


def main() -> None:
    spark = create_spark("olist-fact-sales")
    jdbc_url, props = get_jdbc_config()

    orders_df = spark.read.jdbc(
        url=jdbc_url,
        table="staging.orders_stream_kafka",
        properties=props,
    )

    order_items_df = spark.read.jdbc(
        url=jdbc_url,
        table="staging.order_items_stream_kafka",
        properties=props,
    )

    dim_customer_df = spark.read.jdbc(
        url=jdbc_url,
        table="warehouse.dim_customer",
        properties=props,
    )

    dim_product_df = spark.read.jdbc(
        url=jdbc_url,
        table="warehouse.dim_product",
        properties=props,
    )

    dim_seller_df = spark.read.jdbc(
        url=jdbc_url,
        table="warehouse.dim_seller",
        properties=props,
    )

    orders_base_df = (
        orders_df
        .select(
            F.col("order_id").cast("string").alias("order_id"),
            F.col("customer_id").cast("string").alias("customer_id"),
            F.col("order_status").cast("string").alias("order_status"),
            F.col("order_purchase_timestamp").alias("order_purchase_timestamp"),
            F.col("order_approved_at").alias("order_approved_at"),
            F.col("order_delivered_carrier_date").alias("order_delivered_carrier_date"),
            F.col("order_delivered_customer_date").alias("order_delivered_customer_date"),
            F.col("order_estimated_delivery_date").alias("order_estimated_delivery_date"),
        )
        .dropDuplicates(["order_id"])
    )

    order_items_base_df = (
        order_items_df
        .select(
            F.col("order_id").cast("string").alias("order_id"),
            F.col("order_item_id").cast("int").alias("order_item_id"),
            F.col("product_id").cast("string").alias("product_id"),
            F.col("seller_id").cast("string").alias("seller_id"),
            F.col("shipping_limit_date").alias("shipping_limit_date"),
            F.col("price").cast("double").alias("price"),
            F.col("freight_value").cast("double").alias("freight_value"),
        )
        .dropDuplicates(["order_id", "order_item_id"])
    )

    sales_base_df = (
        order_items_base_df.alias("oi")
        .join(orders_base_df.alias("o"), on="order_id", how="inner")
    )

    fact_sales_stage_df = (
        sales_base_df.alias("f")
        .join(
            dim_customer_df.select("customer_key", "customer_id").alias("dc"),
            on="customer_id",
            how="left",
        )
        .join(
            dim_product_df.select("product_key", "product_id").alias("dp"),
            on="product_id",
            how="left",
        )
        .join(
            dim_seller_df.select("seller_key", "seller_id").alias("ds"),
            on="seller_id",
            how="left",
        )
        .withColumn(
            "delivery_days",
            F.datediff(F.col("order_delivered_customer_date"), F.col("order_purchase_timestamp"))
        )
        .withColumn(
            "estimated_delivery_days",
            F.datediff(F.col("order_estimated_delivery_date"), F.col("order_purchase_timestamp"))
        )
        .withColumn(
            "is_delayed",
            F.when(
                F.col("order_delivered_customer_date") > F.col("order_estimated_delivery_date"),
                F.lit(1),
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "gross_sales_value",
            F.col("price") + F.col("freight_value")
        )
    )

    window_spec = Window.orderBy("order_id", "order_item_id")

    fact_sales_df = (
        fact_sales_stage_df
        .withColumn("fact_sales_key", F.row_number().over(window_spec))
        .select(
            "fact_sales_key",
            "order_id",
            "order_item_id",
            "customer_key",
            "product_key",
            "seller_key",
            "customer_id",
            "product_id",
            "seller_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
            "shipping_limit_date",
            "price",
            "freight_value",
            "gross_sales_value",
            "delivery_days",
            "estimated_delivery_days",
            "is_delayed",
        )
    )

    prepare_target_table()

    (
        fact_sales_df.write
        .mode("append")
        .jdbc(url=jdbc_url, table="warehouse.fact_sales", properties=props)
    )

    print("fact_sales loaded successfully")
    print("rows:", fact_sales_df.count())

    spark.stop()


if __name__ == "__main__":
    main()
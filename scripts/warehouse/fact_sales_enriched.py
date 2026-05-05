import os
import psycopg2
from pyspark.sql import functions as F

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
    cur.execute("DROP TABLE IF EXISTS warehouse.fact_sales_enriched CASCADE;")
    cur.execute("""
    CREATE TABLE warehouse.fact_sales_enriched (
        fact_sales_key INT,
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
        is_delayed INT,
        total_payment_value DOUBLE PRECISION,
        payment_count BIGINT,
        max_installments INT,
        avg_payment_value DOUBLE PRECISION
    );
    """)

    cur.close()
    conn.close()


def main() -> None:
    spark = create_spark("olist-fact-sales-enriched")
    jdbc_url, props = get_jdbc_config()

    fact_df = spark.read.jdbc(
        url=jdbc_url,
        table="warehouse.fact_sales",
        properties=props,
    )

    payments_df = spark.read.jdbc(
        url=jdbc_url,
        table="staging.payments_stream_kafka",
        properties=props,
    )

    payments_agg = (
        payments_df
        .groupBy("order_id")
        .agg(
            F.sum("payment_value").alias("total_payment_value"),
            F.count("*").alias("payment_count"),
            F.max("payment_installments").alias("max_installments"),
        )
    )

    fact_enriched_df = (
        fact_df
        .join(payments_agg, "order_id", "left")
        .withColumn(
            "total_payment_value",
            F.coalesce(F.col("total_payment_value"), F.lit(0.0))
        )
        .withColumn(
            "payment_count",
            F.coalesce(F.col("payment_count"), F.lit(0))
        )
        .withColumn(
            "max_installments",
            F.coalesce(F.col("max_installments"), F.lit(0))
        )
        .withColumn(
            "avg_payment_value",
            F.when(
                F.col("payment_count") > 0,
                F.col("total_payment_value") / F.col("payment_count")
            ).otherwise(F.lit(0.0))
        )
    )

    prepare_target_table()

    (
        fact_enriched_df.write
        .mode("append")
        .jdbc(
            url=jdbc_url,
            table="warehouse.fact_sales_enriched",
            properties=props,
        )
    )

    print("fact_sales_enriched loaded successfully")
    print("rows:", fact_enriched_df.count())

    spark.stop()


if __name__ == "__main__":
    main()
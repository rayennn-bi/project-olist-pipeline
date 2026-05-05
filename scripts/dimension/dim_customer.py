import os
import psycopg2
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from scripts.common.spark_session import create_spark
from scripts.common.paths import get_data_paths
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
    cur.execute("DROP TABLE IF EXISTS warehouse.dim_customer;")
    cur.execute("""
    CREATE TABLE warehouse.dim_customer (
        customer_key INT PRIMARY KEY,
        customer_id TEXT UNIQUE,
        customer_unique_id TEXT,
        customer_zip_code_prefix INT,
        customer_city TEXT,
        customer_state TEXT
    );
    """)

    cur.close()
    conn.close()


def main() -> None:
    spark = create_spark("olist-dim-customer")
    raw_path, _ = get_data_paths()
    jdbc_url, props = get_jdbc_config()

    customers_df = spark.read.csv(
        os.path.join(raw_path, "customers"),
        header=True,
        inferSchema=True,
    )

    customers_clean_df = (
        customers_df
        .select(
            F.trim(F.col("customer_id")).alias("customer_id"),
            F.trim(F.col("customer_unique_id")).alias("customer_unique_id"),
            F.col("customer_zip_code_prefix").cast("int").alias("customer_zip_code_prefix"),
            F.initcap(F.trim(F.col("customer_city"))).alias("customer_city"),
            F.upper(F.trim(F.col("customer_state"))).alias("customer_state"),
        )
    )

    customers_valid_df = (
        customers_clean_df
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("customer_unique_id").isNotNull())
        .dropDuplicates(["customer_id"])
    )

    window_spec = Window.orderBy("customer_id")

    dim_customer_df = (
        customers_valid_df
        .withColumn("customer_key", F.row_number().over(window_spec))
        .select(
            "customer_key",
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        )
    )

    prepare_target_table()

    (
        dim_customer_df.write
        .mode("append")
        .jdbc(url=jdbc_url, table="warehouse.dim_customer", properties=props)
    )

    print("dim_customer loaded successfully")
    print("rows:", dim_customer_df.count())

    spark.stop()


if __name__ == "__main__":
    main()
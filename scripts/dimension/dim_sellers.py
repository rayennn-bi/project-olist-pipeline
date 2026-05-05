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
    cur.execute("DROP TABLE IF EXISTS warehouse.dim_seller CASCADE;")
    cur.execute("""
    CREATE TABLE warehouse.dim_seller (
        seller_key INT PRIMARY KEY,
        seller_id TEXT UNIQUE,
        seller_zip_code_prefix INT,
        seller_city TEXT,
        seller_state TEXT
    );
    """)

    cur.close()
    conn.close()


def main() -> None:
    spark = create_spark("olist-dim-seller")
    raw_path, _ = get_data_paths()
    jdbc_url, props = get_jdbc_config()

    sellers_df = spark.read.csv(
        os.path.join(raw_path, "sellers"),
        header=True,
        inferSchema=True,
    )

    sellers_clean_df = (
        sellers_df
        .select(
            F.trim(F.col("seller_id")).alias("seller_id"),
            F.col("seller_zip_code_prefix").cast("int").alias("seller_zip_code_prefix"),
            F.initcap(F.trim(F.col("seller_city"))).alias("seller_city"),
            F.upper(F.trim(F.col("seller_state"))).alias("seller_state"),
        )
    )

    sellers_valid_df = (
        sellers_clean_df
        .filter(F.col("seller_id").isNotNull())
        .dropDuplicates(["seller_id"])
    )

    window_spec = Window.orderBy("seller_id")

    dim_seller_df = (
        sellers_valid_df
        .withColumn("seller_key", F.row_number().over(window_spec))
        .select(
            "seller_key",
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state",
        )
    )

    prepare_target_table()

    (
        dim_seller_df.write
        .mode("append")
        .jdbc(url=jdbc_url, table="warehouse.dim_seller", properties=props)
    )

    print("dim_seller loaded successfully")
    print("rows:", dim_seller_df.count())

    spark.stop()


if __name__ == "__main__":
    main()
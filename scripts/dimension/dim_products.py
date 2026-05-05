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
    cur.execute("DROP TABLE IF EXISTS warehouse.dim_product CASCADE;")
    cur.execute("""
    CREATE TABLE warehouse.dim_product (
        product_key INT PRIMARY KEY,
        product_id TEXT UNIQUE,
        product_category_name_pt TEXT,
        product_category_name TEXT,
        product_name_length INT,
        product_description_length INT,
        product_photos_qty INT,
        product_weight_g INT,
        product_length_cm INT,
        product_height_cm INT,
        product_width_cm INT
    );
    """)

    cur.close()
    conn.close()


def main() -> None:
    spark = create_spark("olist-dim-product")
    raw_path, _ = get_data_paths()
    jdbc_url, props = get_jdbc_config()

    products_df = spark.read.csv(
        os.path.join(raw_path, "products"),
        header=True,
        inferSchema=True,
    )

    translation_df = spark.read.csv(
        os.path.join(raw_path, "product_category"),
        header=True,
        inferSchema=True,
    )

    products_clean_df = (
        products_df
        .select(
            F.trim(F.col("product_id")).alias("product_id"),
            F.trim(F.col("product_category_name")).alias("product_category_name_pt"),
            F.col("product_name_lenght").cast("int").alias("product_name_length"),
            F.col("product_description_lenght").cast("int").alias("product_description_length"),
            F.col("product_photos_qty").cast("int").alias("product_photos_qty"),
            F.col("product_weight_g").cast("int").alias("product_weight_g"),
            F.col("product_length_cm").cast("int").alias("product_length_cm"),
            F.col("product_height_cm").cast("int").alias("product_height_cm"),
            F.col("product_width_cm").cast("int").alias("product_width_cm"),
        )
    )

    translation_clean_df = (
        translation_df
        .select(
            F.trim(F.col("product_category_name")).alias("product_category_name_pt"),
            F.trim(F.col("product_category_name_english")).alias("product_category_name_en"),
        )
    )

    products_enriched_df = (
        products_clean_df
        .join(translation_clean_df, on="product_category_name_pt", how="left")
        .withColumn(
            "product_category_name",
            F.coalesce(F.col("product_category_name_en"), F.col("product_category_name_pt"))
        )
        .drop("product_category_name_en")
    )

    products_valid_df = (
        products_enriched_df
        .filter(F.col("product_id").isNotNull())
        .dropDuplicates(["product_id"])
    )

    window_spec = Window.orderBy("product_id")

    dim_product_df = (
        products_valid_df
        .withColumn("product_key", F.row_number().over(window_spec))
        .select(
            "product_key",
            "product_id",
            "product_category_name_pt",
            "product_category_name",
            "product_name_length",
            "product_description_length",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        )
    )

    prepare_target_table()

    (
        dim_product_df.write
        .mode("append")
        .jdbc(url=jdbc_url, table="warehouse.dim_product", properties=props)
    )

    print("dim_product loaded successfully")
    print("rows:", dim_product_df.count())

    spark.stop()


if __name__ == "__main__":
    main()
import logging
import os
from typing import Any, Dict

import psycopg2
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from scripts.common.db import get_jdbc_config
from scripts.common.paths import get_data_paths
from scripts.common.spark_session import create_spark

logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
)
logger = logging.getLogger('order_items_consumer')


def get_config() -> Dict[str, Any]:
    _, checkpoint_path = get_data_paths()

    return {
        'kafka_bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
        'topic':                   os.getenv('KAFKA_TOPIC_ORDER_ITEMS', 'olist.order_items'),
        'checkpoint_location':     os.path.join(checkpoint_path, 'order_items_stream_kafka'),
        'trigger_once':            os.getenv('STREAM_TRIGGER_ONCE', 'true').lower() == 'true',
        'starting_offsets':        os.getenv('KAFKA_STARTING_OFFSETS', 'earliest'),
        'output_table':            'staging.order_items_stream_kafka',
    }


def prepare_target_table() -> None:
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=int(os.getenv('POSTGRES_PORT', '5432')),
        dbname=os.getenv('POSTGRES_DB', 'olist_dw'),
        user=os.getenv('POSTGRES_USER', 'airflow'),
        password=os.getenv('POSTGRES_PASSWORD', 'airflow'),
    )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute('CREATE SCHEMA IF NOT EXISTS staging;')
    cur.execute("""
        CREATE TABLE IF NOT EXISTS staging.order_items_stream_kafka (
            order_id TEXT,
            order_item_id INT,
            product_id TEXT,
            seller_id TEXT,
            shipping_limit_date TIMESTAMP,
            price DOUBLE PRECISION,
            freight_value DOUBLE PRECISION,
            kafka_ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (order_id, order_item_id)
    );
    """)

    cur.close()
    conn.close()
    logger.info('Target table staging.order_items_stream_kafka is ready')


def build_stream(spark, config: Dict[str, Any]) -> DataFrame:
    logger.info('Reading Kafka topic %s', config['topic'])

    raw_df = (
        spark.readStream
        .format('kafka')
        .option('kafka.bootstrap.servers', config['kafka_bootstrap_servers'])
        .option('subscribe', config['topic'])
        .option('startingOffsets', config['starting_offsets'])
        .option('failOnDataLoss', 'false')
        .load()
    )

    # Schema sesuai dengan yang dikirim producer
    schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("order_item_id", IntegerType(), True),
            StructField("product_id", StringType(), True),
            StructField("seller_id", StringType(), True),
            StructField("shipping_limit_date", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("freight_value", DoubleType(), True),
    ])

    parsed_df = (
        raw_df
        .selectExpr('CAST(key AS STRING) AS kafka_key', 'CAST(value AS STRING) AS kafka_value')
        .select(
            'kafka_key',
            F.from_json(F.col('kafka_value'), schema).alias('data'),
        )
        .select('kafka_key', 'data.*')
    )

    clean_df = (
        parsed_df
        .withColumn("shipping_limit_date", F.to_timestamp("shipping_limit_date"))
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("order_item_id").isNotNull())
        .filter(F.col("product_id").isNotNull())
        .filter(F.col("seller_id").isNotNull())
        .filter(F.col("price").isNotNull())
        .filter(F.col("freight_value").isNotNull())
        .filter(F.col("price") >= 0)
        .filter(F.col("freight_value") >= 0)
        .drop("kafka_key")
        .dropDuplicates(["order_id", "order_item_id"])
    )

    return clean_df


def upsert_order_items(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        logger.info('Batch %s is empty, skipping', batch_id)
        return

    jdbc_url, props = get_jdbc_config()
    temp_table = f'staging.order_items_stream_kafka_tmp_{batch_id}'

    row_count = batch_df.count()
    logger.info('Processing batch %s with %s rows', batch_id, row_count)

    (
        batch_df.write
        .mode('overwrite')
        .jdbc(url=jdbc_url, table=temp_table, properties=props)
    )

    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=int(os.getenv('POSTGRES_PORT', '5432')),
        dbname=os.getenv('POSTGRES_DB', 'olist_dw'),
        user=os.getenv('POSTGRES_USER', 'airflow'),
        password=os.getenv('POSTGRES_PASSWORD', 'airflow'),
    )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"""
    INSERT INTO staging.order_items_stream_kafka (
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date,
        price,
        freight_value
    )
    SELECT
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date,
        price,
        freight_value
    FROM {temp_table}
    ON CONFLICT (order_id, order_item_id) DO UPDATE SET
        product_id = EXCLUDED.product_id,
        seller_id = EXCLUDED.seller_id,
        shipping_limit_date = EXCLUDED.shipping_limit_date,
        price = EXCLUDED.price,
        freight_value = EXCLUDED.freight_value;
    """)


    cur.execute(f'DROP TABLE IF EXISTS {temp_table};')
    cur.close()
    conn.close()

    logger.info('Batch %s upserted successfully', batch_id)


def main() -> None:
    config = get_config()
    logger.info('order items consumer config: %s', config)

    prepare_target_table()

    spark = create_spark('olist-order-items-consumer')
    stream_df = build_stream(spark, config)

    writer = (
        stream_df.writeStream
        .foreachBatch(upsert_order_items)
        .option('checkpointLocation', config['checkpoint_location'])
    )

    if config['trigger_once']:
        query = writer.trigger(once=True).start()
    else:
        query = writer.start()

    logger.info('Streaming query started')
    query.awaitTermination()
    logger.info('Streaming query finished')

    spark.stop()


if __name__ == '__main__':
    main()

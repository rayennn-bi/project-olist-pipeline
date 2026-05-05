import os
from typing import Dict, Tuple

import psycopg2
from pyspark.sql import SparkSession


def get_db_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=int(os.getenv('POSTGRES_PORT', '5432')),
        dbname=os.getenv('POSTGRES_DB', 'olist_dw'),
        user=os.getenv('POSTGRES_USER', 'airflow'),
        password=os.getenv('POSTGRES_PASSWORD', 'airflow'),
    )


def get_jdbc_config() -> Tuple[str, Dict[str, str]]:
    jdbc_url = (
        f"jdbc:postgresql://"
        f"{os.getenv('POSTGRES_HOST', 'postgres')}:"
        f"{os.getenv('POSTGRES_PORT', '5432')}/"
        f"{os.getenv('POSTGRES_DB', 'olist_dw')}"
    )
    props = {
        'user':     os.getenv('POSTGRES_USER', 'airflow'),
        'password': os.getenv('POSTGRES_PASSWORD', 'airflow'),
        'driver':   'org.postgresql.Driver',
    }
    return jdbc_url, props


def get_checkpoint_path(name: str) -> str:
    data_path = os.getenv('DATA_PATH')
    if not data_path:
        raise ValueError('DATA_PATH environment variable is not set')
    return os.path.join(data_path, 'checkpoints', name)


def create_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config(
            'spark.jars.packages',
            'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,'
            'org.postgresql:postgresql:42.7.3',
        )
        .getOrCreate()
    )

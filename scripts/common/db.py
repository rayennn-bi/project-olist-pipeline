import os
from typing import Dict, Tuple


def get_jdbc_config() -> Tuple[str, Dict]:
    jdbc_url = os.getenv("JDBC_URL", "jdbc:postgresql://postgres:5432/olist_dw")
    props = {
        "user": os.getenv("POSTGRES_USER", "airflow"),
        "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
        "driver": "org.postgresql.Driver",
    }
    return jdbc_url, props
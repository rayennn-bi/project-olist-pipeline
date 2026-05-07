from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

from kafka.admin import KafkaAdminClient, NewTopic

# -----------------------
# Kafka topic init
# -----------------------
def create_kafka_topics():
    admin = KafkaAdminClient(
        bootstrap_servers="kafka:9092",
        client_id="airflow-topic-init"
    )

    topics = [
        "olist.orders",
        "olist.order_items",
        "olist.payments",
    ]

    existing = set(admin.list_topics())

    new_topics = [
        NewTopic(name=t, num_partitions=1, replication_factor=1)
        for t in topics if t not in existing
    ]

    if new_topics:
        admin.create_topics(new_topics)
        print("Created topics:", [t.name for t in new_topics])
    else:
        print("Topics already exist")

    admin.close()


# -----------------------
# DAG
# -----------------------
default_args = {
    "owner": "rian",
    "retries": 1,
}

with DAG(
    dag_id="olist_pipeline_v1",
    default_args=default_args,
    start_date=datetime(2026, 3, 19),
    schedule_interval=None,
    catchup=False,
) as dag:

    # -----------------------
    # 0. Download dataset from Kaggle
    # -----------------------
    download_kaggle = BashOperator(
        task_id="download_kaggle",
        bash_command="PYTHONPATH=/opt/airflow python /opt/airflow/scripts/ingest/download_data.py",
    )

    # -----------------------
    # 1. Init Kafka
    # -----------------------
    init_topics = PythonOperator(
        task_id="init_topics",
        python_callable=create_kafka_topics,
    )

    # -----------------------
    # 2. Producers
    # -----------------------
    with TaskGroup("producers") as producers:
        orders_producer = BashOperator(
            task_id="orders_producer",
            bash_command="PYTHONPATH=/opt/airflow python /opt/airflow/scripts/producer/orders_producer.py",
        )

        order_items_producer = BashOperator(
            task_id="order_items_producer",
            bash_command="PYTHONPATH=/opt/airflow python /opt/airflow/scripts/producer/order_items_producer.py",
        )

        payments_producer = BashOperator(
            task_id="payments_producer",
            bash_command="PYTHONPATH=/opt/airflow python /opt/airflow/scripts/producer/payments_producer.py",
        )

    # -----------------------
    # 2. Consumer
    # -----------------------
    with TaskGroup("consumer") as consumers:
        orders_consumer = BashOperator(
            task_id="orders_consumer",
            bash_command="PYTHONPATH=/opt/airflow python /opt/airflow/scripts/consumer/orders_consumer.py",
        )

        order_items_consumer = BashOperator(
            task_id="order_items_consumer",
            bash_command="PYTHONPATH=/opt/airflow python /opt/airflow/scripts/consumer/order_items_consumer.py",
        )

        payments_consumer = BashOperator(
            task_id="payments_consumer",
            bash_command="PYTHONPATH=/opt/airflow python /opt/airflow/scripts/consumer/payments_consumer.py",
        )

    # -----------------------
    # 3. Dimensions
    # -----------------------
    with TaskGroup("dimensions") as dimensions:
        dim_customer = BashOperator(
            task_id="dim_customer",
            bash_command="PYTHONPATH=/opt/airflow python /opt/airflow/scripts/dimension/dim_customer.py",
        )


        dim_product = BashOperator(
            task_id="dim_product",
            bash_command="PYTHONPATH=/opt/airflow python /opt/airflow/scripts/dimension/dim_products.py",
        )

        dim_seller = BashOperator(
            task_id="dim_seller",
            bash_command="PYTHONPATH=/opt/airflow python /opt/airflow/scripts/dimension/dim_sellers.py",
        )

    # -----------------------
    # 4. Facts
    # -----------------------
    with TaskGroup("facts") as facts:

        fact_sales = BashOperator(
            task_id="fact_sales",
            bash_command="PYTHONPATH=/opt/airflow python /opt/airflow/scripts/warehouse/fact_sales.py",
        )

        fact_sales_enriched = BashOperator(
            task_id="fact_sales_enriched",
            bash_command="PYTHONPATH=/opt/airflow python /opt/airflow/scripts/warehouse/fact_sales_enriched.py",
        )

        fact_sales >> fact_sales_enriched

    # -----------------------
    # 5. Quality
    # -----------------------
    
    with TaskGroup("quality") as quality:
        quality_fact_sales = BashOperator(
            task_id="quality_fact_sales",
            bash_command="PYTHONPATH=/opt/airflow python /opt/airflow/scripts/quality/quality_fact_sales.py",
        )

        quality_fact_sales_enriched = BashOperator(
            task_id="quality_fact_sales_enriched",
            bash_command="PYTHONPATH=/opt/airflow python /opt/airflow/scripts/quality/quality_fact_sales_enriched.py",
        )

        quality_fact_sales >> quality_fact_sales_enriched


    # -----------------------
    # FLOW
    # -----------------------
    download_kaggle >> init_topics >> producers >> consumers >> dimensions >> facts >> quality 
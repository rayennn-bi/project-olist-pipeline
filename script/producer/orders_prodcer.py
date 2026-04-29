import json
import pandas as pd
from kafka import KafkaProducer

def run():
    df = pd.read_csv('/opt/airflow/data/raw/olist_orders_dataset.csv')

    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    for _, row in df.iterrows():
        producer.send("olist.orders", row.to_dict())

    producer.flush()
    print("Orders sent to Kafka")

if __name__ == "__main__":
    run()
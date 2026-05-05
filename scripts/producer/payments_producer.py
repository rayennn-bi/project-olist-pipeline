import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
)
logger = logging.getLogger('payments_producer')


def get_config() -> Dict[str, Any]:
    data_path = os.getenv('DATA_PATH')
    if not data_path:
        raise ValueError('DATA_PATH environment variable is not set')

    return {
        'topic':             os.getenv('KAFKA_TOPIC_PAYMENTS', 'olist.payments'),
        'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
        'csv_path':          Path(data_path) / 'raw' / 'payments' / 'olist_order_payments_dataset.csv',
        'flush_every':       int(os.getenv('PRODUCER_FLUSH_EVERY', '5000')),
        'sleep_seconds':     float(os.getenv('PRODUCER_SLEEP_SECONDS', '0')),
        'sample_only':       os.getenv('PRODUCER_SAMPLE_ONLY', 'false').lower() == 'true',
        'sample_size':       int(os.getenv('PRODUCER_SAMPLE_SIZE', '100')),
    }


def ensure_topic(bootstrap_servers: str, topic: str) -> None:
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        admin.create_topics([
            NewTopic(name=topic, num_partitions=3, replication_factor=1)
        ])
        logger.info('Topic %s berhasil dibuat', topic)
    except TopicAlreadyExistsError:
        logger.info('Topic %s sudah ada', topic)
    finally:
        admin.close()


def load_and_clean(csv_path: Path) -> List[Dict[str, Any]]:
    if not csv_path.exists():
        raise FileNotFoundError(f'CSV tidak ditemukan: {csv_path}')

    logger.info('Membaca CSV dari %s', csv_path)

    # Dataset payments tidak punya kolom tanggal — tidak perlu parse_dates
    df = pd.read_csv(csv_path)

    df = df.astype({
        'order_id':               'string',
        'payment_sequential':     'Int64',
        'payment_type':           'string',
        'payment_installments':   'Int64',
        'payment_value':          'float64',
    })

    df = (
        df
        .dropna(subset=['order_id', 'payment_sequential', 'payment_type',
                        'payment_installments', 'payment_value'])
        .query('payment_value >= 0 and payment_installments >= 0')
        .drop_duplicates(subset=['order_id', 'payment_sequential'])
        .sort_values(['order_id', 'payment_sequential'])
    )
    logger.info('Clean records: %d', len(df))

    records = [
        {
            'order_id':             str(row['order_id']),
            'payment_sequential':   int(row['payment_sequential']),
            'payment_type':         str(row['payment_type']),
            'payment_installments': int(row['payment_installments']),
            'payment_value':        float(row['payment_value']),
        }
        for row in df.to_dict('records')
    ]

    logger.info('Records siap dikirim: %d', len(records))
    return records


def create_producer(bootstrap_servers: str) -> KafkaProducer:
    logger.info('Menghubungkan producer ke Kafka: %s', bootstrap_servers)

    producer = KafkaProducer(
        bootstrap_servers=[bootstrap_servers],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda v: v.encode('utf-8') if v else None,
        acks='all',
        retries=5,
        request_timeout_ms=30000,
        max_block_ms=30000,
    )

    if not producer.bootstrap_connected():
        producer.close()
        raise ConnectionError(
            f'Tidak dapat terhubung ke Kafka broker: {bootstrap_servers}'
        )

    logger.info('Kafka producer terhubung')
    return producer


def send_records(
    producer: KafkaProducer,
    topic: str,
    records: List[Dict[str, Any]],
    flush_every: int = 5000,
    sleep_seconds: float = 0.0,
) -> int:
    sent_count = 0

    for i, record in enumerate(records, start=1):
        # Key: order_id + payment_sequential karena satu order bisa punya
        # beberapa metode pembayaran (cicilan, voucher, dll)
        key = f"{record['order_id']}_{record['payment_sequential']}"

        future = producer.send(topic, key=key, value=record)
        future.get(timeout=30)
        sent_count += 1

        if i % flush_every == 0:
            producer.flush()
            logger.info('Flushed %d messages', i)

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    producer.flush()
    logger.info('Selesai — total terkirim: %d ke topic %s', sent_count, topic)
    return sent_count


def produce_payments_to_kafka(**kwargs) -> Dict[str, Any]:
    config = get_config()
    logger.info('Config: %s', config)

    ensure_topic(config['bootstrap_servers'], config['topic'])

    records = load_and_clean(config['csv_path'])

    if config['sample_only']:
        records = records[:config['sample_size']]
        logger.info('Sample mode aktif — mengirim %d records', len(records))

    producer = create_producer(config['bootstrap_servers'])
    try:
        sent = send_records(
            producer=producer,
            topic=config['topic'],
            records=records,
            flush_every=config['flush_every'],
            sleep_seconds=config['sleep_seconds'],
        )
    finally:
        producer.close()
        logger.info('Kafka producer ditutup')

    return {'dataset': 'payments', 'records_sent': sent}


if __name__ == '__main__':
    produce_payments_to_kafka()

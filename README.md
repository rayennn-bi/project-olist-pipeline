# Olist Data Engineering Pipeline 
An end-to-end data engineering project built on the <https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce> (Brazilian E-Commerce Public Dataset by Olist) . The stack combines batch processing, Kafka-based streaming, a PostgreSQL warehouse, Airflow orchestration, and dashboards

## Overview
This orchestration styles:
dags/olist_pipeline_v1.py : refined pipeline that starts with Kaggle download and uses Python jobs for more stages
The current dataset flow downloads the Kaggle ZIP into data/raw and extracts the CSV files into that different folder.

## Architecture
Kaggle -> data/raw -> Kafka -> Spark/PySpark -> PostgreSQL

## Tech Stack
  - Apache Airflow
  - Apache Kafka and Zookeeper
  - PySpark and Papermill
  - PostgreSQL
  - Docker Compose

Available local services:

 - Airflow UI: http://localhost:8080
 - Kafka UI: http://localhost:8085
 - Jupyter Lab: http://localhost:8889
 - PostgreSQL: localhost:5432

## Benchmark the Success Metric
You can benchmark the pipeline against this target:

  - process at least 100000 rows
  - finish in under 10 minutes

Default benchmark settings:

    DAG: olist_pipeline_v3
    final table: warehouse.fact_sales_enriched
    minimum rows: 100000
    maximum runtime: 900 seconds

Benchmark screenshot:

<img width="493" height="568" alt="image" src="https://github.com/user-attachments/assets/e3a683a3-0e63-45c2-94ee-5eaf433867c2" />

## Airflow DAG
<img width="1359" height="417" alt="image" src="https://github.com/user-attachments/assets/6a77e573-bc36-4597-ba80-8c339d153c19" />

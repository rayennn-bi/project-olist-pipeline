# 🛒 Olist Data Engineering Pipeline

> End-to-end data pipeline built on the [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — covering ingestion, streaming, transformation, warehousing, and dashboarding in a fully containerized environment.

---

## 📌 Overview

This project simulates a **production-grade data engineering workflow** using real e-commerce transaction data from Olist, a Brazilian marketplace. The pipeline handles everything from raw data ingestion to analytical-ready tables inside a PostgreSQL data warehouse.

**Pipeline entry point:**
- `dags/olist_pipeline_v1.py` — orchestrated DAG that handles Kaggle download, Kafka streaming, PySpark transformation, and warehouse loading end-to-end.

**Data flow:**
1. Downloads the Kaggle ZIP into `data/raw/`
2. Extracts CSVs into dedicated subdirectories
3. Produces records into Kafka topics
4. Consumes and transforms via PySpark
5. Loads into staging → warehouse schema in PostgreSQL

---

## 🏗️ Architecture

```
Kaggle Dataset
     │
     ▼
data/raw/ (CSV files)
     │
     ▼
Apache Kafka  ──── Zookeeper
     │
     ▼
PySpark  (Transformation)
     │
     ▼
PostgreSQL
├── staging.*        ← raw stream ingestion
└── warehouse.*      ← fact & dimension tables
     │
     ▼
Dashboard / BI Layer
```

---

## 🧰 Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow |
| Streaming | Apache Kafka + Zookeeper |
| Processing | PySpark + Papermill |
| Storage | PostgreSQL |
| Containerization | Docker Compose |
| Notebooks | Jupyter Lab |

---

## 🚀 Getting Started

### Prerequisites
- Docker & Docker Compose installed
- Kaggle API credentials (`~/.kaggle/kaggle.json`)

### Run the stack
```bash
docker compose up -d
```

### Trigger the pipeline
```bash
# Via Airflow UI
open http://localhost:8080

# Or via CLI
docker compose exec airflow-webserver \
  airflow dags trigger olist_pipeline_v1
```

---

## 🌐 Local Services

| Service | URL |
|---|---|
| Airflow UI | http://localhost:8080 |
| Kafka UI | http://localhost:8085 |
| Jupyter Lab | http://localhost:8889 |
| PostgreSQL | `localhost:5432` |

---

## 📊 Data Model

```
dim_customer ──┐
dim_product  ──┤
dim_seller   ──┼──► fact_sales ──► fact_sales_enriched
               │
               └── (joined & enriched via PySpark)
```

**Warehouse tables:**
- `warehouse.dim_customer` — customer dimension
- `warehouse.dim_product` — product dimension
- `warehouse.dim_seller` — seller dimension
- `warehouse.fact_sales` — core sales fact table
- `warehouse.fact_sales_enriched` — enriched sales with all dimensions joined

---

## ✅ Benchmark

The pipeline is benchmarked against two success criteria:

| Metric | Target |
|---|---|
| Minimum rows processed | ≥ 100,000 rows |
| Maximum pipeline runtime | ≤ 900 seconds (15 minutes) |

**Run the benchmark:**
```bash
bash scripts/shell/benchmark_olist_pipeline.sh
```

**Override defaults via environment:**
```bash
DAG_ID=olist_pipeline_v1 \
FINAL_TABLE=warehouse.fact_sales_enriched \
MIN_ROWS=100000 \
MAX_SECONDS=900 \
bash scripts/shell/benchmark_olist_pipeline.sh
```

**Benchmark output:**
```
================================================
   OLIST PIPELINE BENCHMARK
================================================
[OK]   Service 'airflow-webserver' aktif
[OK]   Service 'postgres' aktif

[INFO] Benchmark configuration
       DAG_ID         : olist_pipeline_v1
       FINAL_TABLE    : warehouse.fact_sales_enriched
       MIN_ROWS       : 100000
       MAX_SECONDS    : 900

[INFO] Triggering DAG 'olist_pipeline_v1'...
[INFO] Current DAG state: running | elapsed=00m 15s
  ...
[INFO] Current DAG state: success | elapsed=03m 05s

────────────────────────────────────────────────────────────
  Benchmark Summary
────────────────────────────────────────────────────────────
  DAG ID                 : olist_pipeline_v1
  Final table            : warehouse.fact_sales_enriched
  DAG state              : success
  Elapsed time           : 03m 05s (185s)
  Final row count        : 112650
  Row target             : 100000 -> PASS
  Time target            : 900s   -> PASS

  Overall result         : PASS
────────────────────────────────────────────────────────────
```

> 📷 **Screenshot:**

<img width="493" height="568" alt="image" src="https://github.com/user-attachments/assets/e3a683a3-0e63-45c2-94ee-5eaf433867c2" />

---

## 🔁 Airflow DAG

The DAG is structured in layered task groups:

```
download_kaggle
      │
  init_topics
      │
  producers ──── orders_producer
             ├── order_items_producer
             └── payments_producer
      │
  consumer  ──── orders_consumer
             ├── order_items_consumer
             └── payments_consumer
      │
  dimensions ─── dim_customer
              ├── dim_product
              └── dim_seller
      │
   facts ──── fact_sales
          └── fact_sales_enriched
      │
  quality ─── quality_fact_sales
          └── quality_fact_sales_enriched
```

> 📷 **DAG Screenshot:**
>
<img width="1359" height="417" alt="image" src="https://github.com/user-attachments/assets/6a77e573-bc36-4597-ba80-8c339d153c19" />

--
## warehouse schema
<img width="1266" height="687" alt="image" src="https://github.com/user-attachments/assets/4bf83507-4e1c-402a-8cb1-4064a2f617a4" />


## dashboard analyttic RFM
<img width="1912" height="928" alt="image" src="https://github.com/user-attachments/assets/8ea46ad1-4563-482d-95d5-7a3890c159f8" />
<img width="1858" height="976" alt="image" src="https://github.com/user-attachments/assets/586897b6-72d7-454b-9917-0e388396dfee" />
---

## 📁 Project Structure

```
project-olist-pipeline/
├── dags/
│   └── olist_pipeline_v1.py       # Main Airflow DAG
├── data/
│   └── raw/                        # Downloaded CSV files
│       ├── customers/
│       ├── orders/
│       ├── order_items/
│       ├── payments/
│       ├── products/
│       └── sellers/
├── scripts/
│   ├── common/
│   ├── consumer/
│   ├── dimension/
│   ├── ingest/
│   ├── producer/
│   ├── quality/
│   └── shell/
│       └── benchmark_olist_pipeline.sh
├── notebooks/
├── plugins/
├── docker-compose.yaml
├── airflow.cfg
└── .env
```

---

## 📄 Dataset

**Source:** [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

Contains ~100k orders from 2016–2018 made at multiple marketplaces in Brazil, with information on order status, price, payment, freight performance, customer location, product attributes, and seller reviews.

---

## 👤 Author

Built as a data engineering portfolio project demonstrating real-world pipeline design with modern open-source tools.








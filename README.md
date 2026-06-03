# Automated Multi-Stage Data Pipeline Platform

> An end-to-end data engineering platform that ingests SEC financial statement data, orchestrates multi-format ETL pipelines via Apache Airflow, stages data in AWS S3, and serves it through a cloud data warehouse (Snowflake) — complete with a REST API and an interactive analytics dashboard.

## Live Links

[![Codelabs](https://img.shields.io/badge/codelabs-4285F4?style=for-the-badge&logo=codelabs&logoColor=white)](https://codelabs-preview.appspot.com/?file_id=10LyLUw6ExvnydJ-WWU9VY5ZdL0ZN6cSuQ4gK6o7-hZo#0)
[![Demo Video](https://img.shields.io/badge/Demo-YouTube-red?style=for-the-badge&logo=youtube&logoColor=white)](https://youtu.be/SFfq909AHa0)

| Service | URL |
|---|---|
| FastAPI Backend | https://damg7245-team5-assignment2.onrender.com |
| Streamlit Analytics App | https://damg7245team5assignment2-duhefdi76eqengrappe588s.streamlit.app/ |

---

## Overview

Public SEC financial filings contain high-value data — but querying them at scale requires a proper data engineering foundation. This platform automates the full lifecycle:

- **Ingestion** — scrapes quarterly ZIP archives from SEC.gov programmatically
- **Multi-format transformation** — processes the same source data three ways (raw TSV, JSON, and denormalized fact tables) to benchmark storage and query tradeoffs
- **Cloud staging** — uploads all artifacts to AWS S3 before loading
- **Warehouse loading** — bulk loads into Snowflake using `COPY INTO` for performance at scale
- **Data access layer** — FastAPI REST API for programmatic access
- **Analytics UI** — Streamlit dashboard for interactive SQL exploration and CSV export

This project demonstrates core data engineering competencies: pipeline orchestration, ELT design patterns, cloud storage integration, data modeling (star schema fact tables), and serving structured data through APIs.

---

## Architecture

```
┌─────────────┐     ┌──────────────────────────────────────┐     ┌───────────────┐
│   SEC.gov   │────►│         Apache Airflow (Docker)       │────►│    AWS S3     │
│ (source)    │     │                                        │     │  (staging)    │
└─────────────┘     │  ┌──────────┐  ┌──────┐  ┌────────┐  │     └──────┬────────┘
                    │  │ Raw DAG  │  │ JSON │  │ Denorm │  │            │
                    │  │          │  │ DAG  │  │  DAG   │  │            ▼
                    │  └──────────┘  └──────┘  └────────┘  │     ┌───────────────┐
                    └──────────────────────────────────────┘     │   Snowflake   │
                                                                  │  (warehouse)  │
                                                                  └──────┬────────┘
                                                                         │
                                                          ┌──────────────┴──────────────┐
                                                          │                             │
                                                   ┌──────▼──────┐           ┌─────────▼──────┐
                                                   │  FastAPI    │           │   Streamlit    │
                                                   │  REST API   │           │   Dashboard    │
                                                   └─────────────┘           └────────────────┘
```

### Data Model — Three Storage Strategies

| Pipeline | Storage Pattern | Snowflake Tables | Use Case |
|---|---|---|---|
| **Raw** | Tab-delimited TSV | `NUM_TABLE`, `SUB_TABLE`, `PRE_TABLE`, `TAG_TABLE` | Faithful source replica, fast ingestion |
| **JSON** | Semi-structured (VARIANT) | `NUM_JSON_TABLE`, `SUB_JSON_TABLE`, `PRE_JSON_TABLE`, `TAG_JSON_TABLE` | Flexible schema, nested access |
| **Denormalized** | Star schema fact tables | `FACT_BALANCE_SHEET`, `FACT_CASH_FLOW`, `FACT_INCOME_STATEMENT` | Analytics-ready, BI tool friendly |

Running all three pipelines on the same source data enables a direct, apples-to-apples comparison of ingestion performance, storage cost, and query ergonomics — a common evaluation exercise in data engineering.

---

## Key Engineering Decisions

| Decision | Approach | Why |
|---|---|---|
| Orchestration | Apache Airflow with CeleryExecutor | Distributed task execution, retry logic, DAG visibility |
| Intermediate storage | AWS S3 | Decouples ingestion from loading; enables re-processing without re-downloading |
| Bulk loading | Snowflake `COPY INTO` via external stages | Orders of magnitude faster than row-by-row inserts |
| Data formats | TSV, JSON (VARIANT), Denormalized | Benchmarks tradeoffs across storage patterns |
| Containerization | Docker Compose | Reproducible local environment; identical to production |

---

## Tech Stack

[![Python](https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=blue)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/fastapi-109989?style=for-the-badge&logo=FASTAPI&logoColor=white)](https://fastapi.tiangolo.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=Streamlit&logoColor=white)](https://streamlit.io/)
[![Apache Airflow](https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)](https://airflow.apache.org/)
[![Amazon AWS](https://img.shields.io/badge/Amazon_AWS-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white)](https://aws.amazon.com/)
[![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)](https://www.snowflake.com/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=Docker&logoColor=white)](https://www.docker.com)
[![GitHub](https://img.shields.io/badge/GitHub-100000?style=for-the-badge&logo=github&logoColor=white)](https://github.com/)

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.10.5 (CeleryExecutor) |
| Task queue | Redis + Celery |
| Ingestion | Python, BeautifulSoup, requests |
| Transformation | pandas |
| Cloud storage | AWS S3 (boto3) |
| Data warehouse | Snowflake |
| API | FastAPI + Uvicorn |
| Analytics UI | Streamlit |
| Containerization | Docker, Docker Compose |
| Airflow metadata DB | PostgreSQL 13 |

---

## Project Structure

```
.
├── backend/
│   └── main.py                          # FastAPI — table listing, preview, ad-hoc SQL
├── frontend/
│   └── app.py                           # Streamlit analytics dashboard
├── airflow/
│   ├── docker-compose.yaml              # Airflow + Celery + Redis + Postgres stack
│   └── dags/
│       ├── sec_data_raw_dag.py          # Pipeline 1: raw TSV ingestion
│       ├── sec_data_json_processing.py  # Pipeline 2: JSON transformation
│       └── sec_denormalized.py          # Pipeline 3: denormalized fact tables
├── Diagrams/                            # Architecture and flow diagrams
├── Documentation/
│   ├── Comparison_Storage_Methods.docx  # TSV vs JSON vs Denormalized analysis
│   ├── Database_Schema_Design.docx      # Snowflake schema design
│   └── Post_Upload_Testing.docx         # Data validation results
├── requirements.txt
└── AiDisclosure.md
```

---

## Prerequisites

- Python 3.x
- Docker Desktop
- AWS account with an S3 bucket
- Snowflake account (free trial works)
- Postman (for API testing)

---

## Local Setup

### 1. Clone the repository

```bash
git clone <repository_url>
cd DAMG7245_Team5_Assignment2
```

### 2. Create a virtual environment

```bash
python -m venv venv
# Windows:
venv\Scripts\activate
# macOS/Linux:
source venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

Create a `.env` file in the project root:

```env
# AWS
AWS_ACCESS_KEY_ID=<your_access_key>
AWS_SECRET_ACCESS_KEY=<your_secret_key>
AWS_REGION=us-east-1
S3_BUCKET_NAME=<your_bucket_name>

# Snowflake
SNOWFLAKE_USER=<your_username>
SNOWFLAKE_PASSWORD=<your_password>
SNOWFLAKE_ACCOUNT=<your_account_id>
SNOWFLAKE_WAREHOUSE=<your_warehouse>
SNOWFLAKE_DATABASE=<your_database>
SNOWFLAKE_SCHEMA=<your_schema>
SNOWFLAKE_ROLE=<your_role>
```

### 5. Start Airflow (Docker)

```bash
cd airflow
docker-compose up --build
```

Airflow UI → **http://localhost:8080** (default: `airflow` / `airflow`)

To trigger a pipeline manually, click the DAG in the UI and pass a JSON config:

```json
{ "year": "2023", "quarter": "Q1" }
```

### 6. Start the FastAPI backend

```bash
cd backend
uvicorn main:app --reload
```

API → **http://127.0.0.1:8000** | Interactive docs → **http://127.0.0.1:8000/docs**

### 7. Start the Streamlit dashboard

```bash
cd frontend
streamlit run app.py
```

---

## API Reference

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/tables/{data_type}` | List available tables (`Raw`, `JSON`, or `Denormalized`) |
| `GET` | `/table_data/{table_name}` | Preview first 10 rows of any table |
| `POST` | `/execute_query/` | Run an ad-hoc SQL query against Snowflake |

**Example:**

```bash
curl -X POST https://damg7245-team5-assignment2.onrender.com/execute_query/ \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM FACT_INCOME_STATEMENT LIMIT 10"}'
```

---

## Pipeline Walkthrough

### Pipeline 1 — Raw (`sec_data_raw_dag.py`)

```
Scrape SEC.gov index
  → Download quarterly ZIP (e.g. 2023-Q1.zip)
  → Extract num.txt, sub.txt, pre.txt, tag.txt
  → Upload TSV files to S3
  → Create Snowflake external stage pointing to S3
  → COPY INTO raw tables
  → Verify row counts
```

### Pipeline 2 — JSON (`sec_data_json_processing.py`)

```
Download + extract same ZIP
  → Convert TSV → JSON with pandas
  → Upload JSON files to S3
  → COPY INTO Snowflake VARIANT columns
  → Verify row counts
```

### Pipeline 3 — Denormalized (`sec_denormalized.py`)

```
Download + extract + preprocess
  → Upload to S3
  → Load into Snowflake staging tables
  → Transform into FACT_BALANCE_SHEET,
     FACT_CASH_FLOW, FACT_INCOME_STATEMENT
```

---

## Analytics Dashboard (Streamlit)

The Streamlit app is designed for data analysts who want to explore the warehouse without writing backend code:

- Switch between Raw, JSON, and Denormalized datasets
- Dynamically list available tables per dataset
- Run any SQL query against Snowflake in real time
- See query execution time and result row count
- Export any result to CSV with a single click

---

## References

- [SEC Financial Statement Data Sets](https://www.sec.gov/dera/data/financial-statements)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Snowflake Documentation](https://docs.snowflake.com/)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Streamlit Documentation](https://docs.streamlit.io/)

---

## Team

| Name | NUID | Contribution |
|---|---|---|
| Pranjal Mahajan | 002375449 | 33.33% |
| Srushti Patil | 002345025 | 33.33% |
| Ram Putcha | 002304724 | 33.33% |

See [AiDisclosure.md](AiDisclosure.md) for details on AI tools used in this project.

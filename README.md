# Website Traffic ETL Pipeline

A production-style ETL pipeline built with **Apache Airflow** and **Google Cloud Storage**, running locally in Docker.

```
GCS (CSV) → Airflow → Transform → Branch → Notify
```

## Pipeline Overview

The `traffic_analysis` DAG processes website traffic data through these stages:

```
                        ┌─────────────────┐
                        │ read_traffic_   │
                        │     data        │
                        └────────┬────────┘
                                 │
                        ┌────────▼────────┐
                        │   filter_ips    │
                        │  (drop bottom   │
                        │    20% IPs)     │
                        └────────┬────────┘
                                 │
                        ┌────────▼────────┐
                        │   split_am_pm   │
                        └───┬─────────┬───┘
                            │         │
               ┌────────────▼─┐     ┌─▼────────────┐
               │  filter_am   │     │  filter_pm   │
               └──────┬───────┘     └──────┬───────┘
                      │                    │
             ┌────────▼────────┐    ┌──────▼───────┐
             │  day_of_week    │    │send_email_pm │
             │   (branch)      │    └──────────────┘
             └───┬─────────┬───┘
                 │         │
      Weekend    │         │    Weekday
       ┌─────────▼───┐   ┌─▼─────────────┐
       │send_email_am│   │ do_nothing_am │
       └─────────────┘   └───────────────┘
```

**What it does:**
1. Downloads traffic CSV from GCS
2. Filters out low-traffic IPs (bottom 20th percentile)
3. Splits data into AM/PM segments
4. PM branch: always sends an email report
5. AM branch: sends email only on weekends

## Tech Stack

| Component | Purpose |
|-----------|---------|
| Apache Airflow 2.7 | Orchestration |
| Docker Compose | Local runtime |
| Google Cloud Storage | Data lake |
| Python + Pandas | Transformations |
| Gmail SMTP | Notifications |

## Prerequisites

- Docker & Docker Compose
- Google Cloud SDK (`gcloud`)
- A GCP project with billing enabled
- Gmail account with [App Password](https://support.google.com/accounts/answer/185833) for SMTP

## Quick Start

### 1. Clone and configure

```bash
git clone git@github.com:JosephGillData/website-traffic-etl-airflow-dev.git
cd website-traffic-etl-airflow-dev

cp .env.example .env
# Edit .env with your values (see Configuration below)
```

### 2. Load environment variables

```bash
set -a && source .env && set +a
```

### 3. Authenticate with GCP

```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project "$PROJECT_ID"
gcloud auth application-default set-quota-project "$PROJECT_ID"
```

### 4. Create GCS bucket and upload data

```bash
gcloud storage buckets create "gs://$GCS_BUCKET" --location=EU --uniform-bucket-level-access
gcloud storage cp ./data/traffic_data.csv "gs://$GCS_BUCKET/data/traffic_data.csv"
```

### 5. Prepare local directories and permissions

```bash
mkdir -p logs sqlite
chmod -R 777 logs sqlite
chmod 755 ~/.config ~/.config/gcloud
chmod 644 ~/.config/gcloud/application_default_credentials.json
```

### 6. Start Airflow

```bash
docker compose up -d
```

Wait ~30 seconds, then verify:

```bash
docker compose ps
```

### 7. Create the GCP connection (once it's running)

```bash
docker compose exec airflow airflow connections add google_cloud_default \
  --conn-type google_cloud_platform \
  --conn-extra '{"project":"'"$PROJECT_ID"'","num_retries":5}'
```

### 8. Open the UI

Navigate to [http://localhost:8080](http://localhost:8080)

**Login:** `admin` / `admin`

Trigger the `traffic_analysis` DAG manually to run the pipeline.

## Configuration

Create `.env` from the example and set these variables:

| Variable | Description |
|----------|-------------|
| `PROJECT_ID` | Your GCP project ID |
| `BILLING_ACCOUNT_ID` | GCP billing account (for initial setup) |
| `GCS_BUCKET` | Globally unique bucket name |
| `SMTP_MAIL_FROM` | Gmail address for sending alerts |
| `SMTP_APP_PASSWORD` | Gmail app password ([create one here](https://support.google.com/accounts/answer/185833)) |
| `AIRFLOW_UID` | Linux user ID for Docker (usually `5000` or output of `id -u`) |

## Project Structure

```
.
├── dags/
│   ├── traffic_analysis.py   # Main ETL pipeline
│   ├── sanity_composer.py    # Simple health check DAG
│   └── fail_task.py          # Email alert test DAG
├── data/
│   └── traffic_data.csv      # Sample traffic data
├── logs/                     # Airflow logs (gitignored)
├── sqlite/                   # Airflow metadata (gitignored)
├── docker-compose.yaml
├── requirements.txt
└── .env.example
```

## Sample Data

The pipeline expects a CSV with this schema:

| Column | Type | Description |
|--------|------|-------------|
| `bf_date` | date | Traffic date |
| `bf_time` | time | Traffic timestamp |
| `id` | int | Session ID |
| `ip` | string | Source IP address |
| `gbps` | float | Traffic volume in Gbps |

## Useful Commands

```bash
# Stop Airflow
docker compose down

# View logs
docker compose logs -f airflow

# Full reset
docker compose down --remove-orphans && docker compose up -d

# Check running containers
docker compose ps
```

## Architecture Notes

- **Local**: Airflow runs in a single container with SQLite + SequentialExecutor
- **GCS Integration**: Uses Application Default Credentials (ADC) mounted read-only
- **Intermediate Storage**: Transformed data stored as Parquet in GCS (not XCom)
- **Branching**: `BranchPythonOperator` routes AM traffic based on day of week

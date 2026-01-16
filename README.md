# Website Traffic ETL with Apache Airflow (GCP)

This repository demonstrates an **end-to-end Data Engineering ETL pipeline** built with **Apache Airflow**, running **locally in Docker**, and integrating with [**Google Cloud Platform (GCP)**](https://console.cloud.google.com).

The project is intentionally designed as a **portfolio-quality example** for Data Engineering interviews, showcasing production-minded practices such as:

- Airflow DAG design
- Dockerized local development
- GCP authentication via Application Default Credentials (ADC)
- Secure configuration via environment variables
- Cloud Storage–based ingestion
- Clear operational setup and teardown via CLI

## What This Project Does

At a high level, this project:

1. **Reads website traffic data** from a CSV file stored in **Google Cloud Storage**
2. **Processes and splits traffic data** by:
   - AM vs PM
   - Day of week
3. **Conditionally triggers downstream tasks** in Airflow
4. **Sends email notifications** based on business logic
5. Runs **end-to-end in Apache Airflow**, orchestrated locally using Docker

This mirrors a common real-world pattern:

> Cloud object storage → Airflow ingestion → transformation → branching → notification

## Architecture Overview

### Local Development
- Apache Airflow runs inside Docker
- DAGs are mounted into the container
- Logs and metadata are persisted locally

### Cloud Integration
- Google Cloud Storage (GCS) hosts source data
- Authentication uses **GCP Application Default Credentials**
- Airflow connects to GCP using the `google_cloud_default` connection

<pre>
Local Machine
├── Docker + Airflow
│   ├── DAG orchestration
│   ├── PythonOperators
│   └── Email notifications
│
└── Google Cloud Platform
    └── Cloud Storage (CSV source data)
</pre>

## Tech Stack

- Apache Airflow
- Docker & Docker Compose
- Python
- Google Cloud Platform
  - Cloud Storage
  - gcloud CLI
- Linux / CLI-first workflow

## Prerequisites

You must have the following installed locally:

- Python 3.10+
- Docker & Docker Compose
- Google Cloud SDK (`gcloud`)
- A GCP project with billing enabled

## Setup Instructions (End-to-End)

#### Clone the Repository

```bash
cd ~
git clone git@github.com:JosephGillData/website-traffic-etl-airflow-dev.git
cd website-traffic-etl-airflow-dev
```

#### Create and Activate Virtual Environment

```bash
python -m venv venv
source venv/bin/activate
```

#### Install Python Dependencies
```bash
pip install -r requirements.txt
```

#### Create Environment Configuration
```bash
cp .env.example .env
# Edit .env and set required values
```

#### Load the variables into your shell so Docker Compose can access them:

```bash
set -a
source .env
set +a
```

### Google Cloud Setup

#### Authenticate with GCP

```bash
gcloud auth login
gcloud auth application-default login
```

#### Create or Select a GCP Project

```bash
gcloud projects create "$PROJECT_ID" --name="PROJECT_NAME"
```

#### Configure Project, Billing, and Storage

```bash
gcloud config set project "$PROJECT_ID"
gcloud billing projects link "$PROJECT_ID" --billing-account="$BILLING_ACCOUNT_ID"
gcloud auth application-default set-quota-project "$PROJECT_ID"

gcloud storage buckets create "gs://$GCS_BUCKET" \
  --location=EU \
  --uniform-bucket-level-access

gcloud storage cp ./data/traffic_data.csv \
  "gs://$GCS_BUCKET/data/traffic_data.csv"
```

### Docker + Airflow Preparation

When running Airflow locally, Docker containers must be able to write logs and metadata to the host filesystem.

```bash
mkdir -p logs sqlite
chmod -R 777 logs sqlite

chmod 755 ~/.config
chmod 755 ~/.config/gcloud
chmod 644 ~/.config/gcloud/application_default_credentials.json
```

This ensures the Airflow container can read your GCP credentials via ADC.

### Start Airflow

```bash
docker compose up -d
```

Wait ~30 seconds for initialization, then verify:

```bash
docker compose ps
```

Airflow UI will be available at: [http://localhost:8080](http://localhost:8080)

### Configure Airflow GCP Connection

Once Airflow is running, create the GCP connection used by the DAG:

```bash
docker compose exec airflow airflow connections add google_cloud_default \
  --conn-type google_cloud_platform \
  --conn-extra '{"project":"'"$PROJECT_ID"'","num_retries":5}'
```

This connection allows Airflow to authenticate to GCP using ADC.

### Running the Pipeline

1. Open the Airflow UI
2. Enable the traffic_analysis DAG
3. Trigger a manual run
4. Observe task execution, branching, and email notifications

The pipeline runs **end-to-end** using data pulled directly from GCS.

### Stopping / Resetting Airflow

```bash
docker compose down
```

To fully reset and redeploy (including logs):

```bash
docker compose down --remove-orphans
docker compose up -d
docker compose logs -f airflow
```
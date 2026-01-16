# Website Traffic ETL with Apache Airflow (GCP)

An end-to-end **Data Engineering ETL pipeline** orchestrated with **Apache Airflow**, running **locally in Docker**, and integrating with **Google Cloud Storage (GCS)** on **Google Cloud Platform (GCP)**.

This repo is built as a **portfolio project for Data Engineering interviews** and focuses on demonstrating real-world patterns:

- Airflow DAG design (task dependencies, branching)
- Dockerized local orchestration
- GCP authentication via **Application Default Credentials (ADC)**
- Secure configuration via `.env` environment variables
- Cloud Storage ingestion (CSV in → pipeline processes it)
- CLI-first setup, reproducible local runs

## What this pipeline does

At a high level, the DAG:

1. **Downloads website traffic data** from a CSV stored in **Google Cloud Storage**
2. **Transforms and splits** the dataset by:
   - AM vs PM
   - Day of week
3. **Branches conditionally** based on business rules
4. **Sends email notifications** depending on outcomes
5. Runs end-to-end in **Airflow** (locally via Docker)

This mirrors a common production pattern:

> Cloud object storage → Airflow ingestion → transformation → branching → notifications

## Architecture

### Local (Docker)
- Airflow runs inside a Docker container
- `./dags` is mounted into the container
- `./logs` and `./sqlite` are mounted to persist logs + metadata locally

### Cloud (GCP)
- Source data lives in **GCS**
- Authentication uses **ADC** mounted read-only into the container
- Airflow uses the `google_cloud_default` connection for GCP hooks

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

## Configuration

Create a .env file from the example:

```bash
cp .env.example .env
```

Edit the variables

| Variable             | Description                                                                                        | Used by                        |
| -------------------- | -------------------------------------------------------------------------------------------------- | ------------------------------ |
| `PROJECT_ID`         | Google Cloud project ID (e.g. `website-traffic-etl-dev`)                                           | `gcloud`, Airflow GCP hooks    |
| `BILLING_ACCOUNT_ID` | Billing account ID linked to the GCP project                                                       | `gcloud billing projects link` |
| `GCS_BUCKET`         | Name of the GCS bucket storing the source CSV (must be globally unique)                            | Airflow DAG, GCS hooks         |
| `SMTP_MAIL_FROM`     | Email address used as the sender for Airflow notifications                                         | Airflow SMTP config            |
| `SMTP_APP_PASSWORD`  | Gmail app password for SMTP authentication                                                         | Airflow SMTP config            |
| `AIRFLOW_UID`        | Linux user ID used by the Airflow Docker container to prevent permission issues on mounted volumes | Docker Compose / Airflow       |

Load the variables into your shell (so Docker Compose can access them):

```bash
set -a
source .env
set +a
```

## Setup (End-to-End)

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
pip install -r requirements.txt
```

#### Authenticate with GCP

```bash
gcloud auth login
gcloud auth application-default login
```

#### Create/select a GCP project

If you already have a project, skip creation.

```bash
gcloud projects create "$PROJECT_ID" --name="INSERT_PROJECT_NAME"
```

Set it as your active project:

```bash
gcloud config set project "$PROJECT_ID"
```

Link billing (requires permissions on the billing account + project):

```bash
gcloud billing projects link "$PROJECT_ID" --billing-account="$BILLING_ACCOUNT_ID"
```

Set quota project for ADC (helps avoid “quota project” / ADC warnings and misbilling):

```bash
gcloud auth application-default set-quota-project "$PROJECT_ID"
```

#### Create a GCS bucket + upload the CSV

```bash
gcloud storage buckets create "gs://$GCS_BUCKET" --location=EU --uniform-bucket-level-access

gcloud storage cp ./data/traffic_data.csv "gs://$GCS_BUCKET/data/traffic_data.csv"
```

#### Prepare local folders for Docker volume permissions

Airflow writes logs and SQLite metadata into mounted volumes.

```bash
mkdir -p logs sqlite
chmod -R 777 logs sqlite
```

#### Ensure the Airflow container can read ADC credentials

Because the container runs as a different user, the ADC file must be readable inside Docker.

```bash
chmod 755 ~/.config
chmod 755 ~/.config/gcloud
chmod 644 ~/.config/gcloud/application_default_credentials.json
```

### Run Airflow locally

#### Start Airflow

```bash
docker compose up -d
```

Wait ~30 seconds, then verify:

```bash
docker compose ps
```

Open the UI: [http://localhost:8080](http://localhost:8080)

#### Create the Airflow GCP connection (after Airflow is running)

```bash
docker compose exec airflow airflow connections add google_cloud_default \
  --conn-type google_cloud_platform \
  --conn-extra '{"project":"'"$PROJECT_ID"'","num_retries":5}'
```

This connection allows Airflow to authenticate to GCP using ADC.

#### Run the Pipeline

1. Open the Airflow UI
2. Trigger a manual run
3. Observe task execution, branching, and email notifications

The pipeline pulls data directly from GCS and runs end-to-end.

#### Stop / reset

Stop containers:

```bash
docker compose down
```

Full reset (including cleaning up orphan containers):

```bash
docker compose down --remove-orphans
docker compose up -d
docker compose logs -f airflow
```
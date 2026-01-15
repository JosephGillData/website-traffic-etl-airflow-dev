# Website Traffic ETL with Apache Airflow

An ETL pipeline using Apache Airflow that analyzes website traffic data and sends daily email reports with the top 3 IP addresses by traffic volume. This project practices CI/CD deployment where developments can be made in a dev (local) envronment and be pushed into a live (GCP) environment.

## Author

**Joseph Gill** - [joegilldata.com](https://joegilldata.com)

## TODO
- Improve the read me by running through it end to end
- Work on improved deployment via CI/CD pipelines in GitHub Actions
- Change it to a live data source (make the repo actually cool/useful)

## Tech Stack

- **Airflow 2.7.0** - Workflow orchestration
- **PostgreSQL 15** - Metadata database
- **Python 3.11** - Runtime (inside container)
- **pandas/numpy** - Data processing

## Prerequisites

- Google Cloud account
- Create a new GCP project
- Select a billing account for your GCP project
- `gcloud` CLI installed
- `gsutil` (included with gcloud)
- Ubuntu / WSL terminal recommended

## Project Structure

```
website-traffic-etl-airflow/
├── dags/
│   └── task-3.py           # Main ETL DAG
├── data/
│   └── traffic_data.csv    # Sample traffic data (61K rows)
├── logs/                   # Airflow logs (auto-generated) 
├── plugins/                # Custom Airflow plugins
├── docker-compose.yaml     # Docker services configuration
├── .env                    # Environment variables (AIRFLOW_UID)
└── README.md
```

## Where Things Live

| What | Location |
|------|----------|
| DAGs (your pipelines) | `./dags/` |
| Logs | `./logs/` |
| Input data | `./data/` |
| Airflow UI | http://localhost:8080 |

## The Pipeline

The `task_3` DAG runs daily at midnight and:

1. **Extracts** traffic data from CSV
2. **Transforms** by filtering low-traffic IPs and splitting AM/PM
3. **Loads** by sending email reports (requires SMTP config)

```
read_traffic_data → filter_ips → split_am_pm → filter_am → day_of_week → [do_nothing_am, send_email_am]
                                             → filter_pm → send_email_pm
```

## Local Development

### Clone and navigate to the repo

```bash
# In your WSL terminal (Ubuntu)
cd ~
git clone https://github.com/YOUR_USERNAME/website-traffic-etl-airflow.git
cd website-traffic-etl-airflow
```

### Load environment variables into your shell

Copy the .env.example file as a .env file and update the variables.

Load the variables defined in your .env file into your current shell session so they can be used by CLI commands (such as gcloud, gsutil, and setup scripts).

```bash
set -a
source .env
set +a
```

Within your GCP account, create a project and within that project create two buckets.

```bash
gsutil mb gs://traffic-data-dev
gsutil mb gs://traffic-data-prod
```

Upload your traffic_data.csv file to the buckets in ./data/traffic_data.csv.

### Set your user ID (avoids permission issues)

```bash
# Check your user ID
id -u

# If it's not 1000, update .env:
echo "AIRFLOW_UID=$(id -u)" > .env
```

## Authenticate with Google Cloud

Log in with you Google account. 

```bash
gcloud init
gcloud auth application-default login
```

### Ensuring Airflow has access to login credentials json file

When running Airflow locally in Docker, Application Default Credentials must be readable by the container user.
The gcloud auth application-default login file is created with restrictive permissions, which can cause Permission denied errors inside Docker.
Fix by making the file world-readable:

```bash
chmod 644 ~/.config/gcloud/application_default_credentials.json
```

### Start Airflow

```bash
docker compose up -d
```

Wait ~30 seconds for initialization, then check status:

```bash
docker compose ps
```

All services should show "healthy" or "running".

### Access the Airflow UI

Open **http://localhost:8080** in your browser.

| | |
|---|---|
| **Username** | `admin` |
| **Password** | `admin` |

### Test run a DAG

1. Find `traffic_analysis` in the DAG list
2. Click the **Play** button to trigger a manual run

### Stop Airflow

```bash
docker compose down
```

### Redeploy the ariflow model with logs

```bash
docker compose down --remove-orphans
docker compose up -d
docker compose logs -f airflow
```

## Deploy to Google Cloud (Cloud Composer)

This guide explains how to deploy the **Website Traffic ETL with Apache Airflow**
project to **Google Cloud Platform** using **Cloud Composer (managed Airflow)**,
entirely via the **command line**.

Cloud Composer runs Airflow on **Google Kubernetes Engine (GKE)** and removes the
need to manage Airflow infrastructure yourself.

### Enable the required APIs.

```bash
gcloud services enable \
  composer.googleapis.com \
  storage.googleapis.com \
  compute.googleapis.com \
  iam.googleapis.com \
  cloudresourcemanager.googleapis.com
```

Create a service account for the cloud composer.

```bash
gcloud iam service-accounts create $COMPOSER_SA \
  --display-name "Cloud Composer Service Account"
```

Grant the required roles.

```bash
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/composer.worker"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/storage.admin"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/logging.logWriter"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/monitoring.metricWriter"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:service-<PROJECT_NUMBER>@cloudcomposer-accounts.iam.gserviceaccount.com" \
  --role="roles/composer.ServiceAgentV2Ext"
```

### Create the Cloud Composer environment

Create the Cloud Composer environment that will run your Airflow DAGs.
This step typically takes 10–25 minutes to complete.

During creation, we explicitly configure Airflow to use the built-in SMTP email backend and Gmail’s SMTP settings. This ensures that task failure notifications and other Airflow emails are sent using the specified Google email address, rather than the default SendGrid integration.

```bash
gcloud composer environments create "$ENV_NAME" \
  --location "$REGION" \
  --service-account "$SA_EMAIL" \
  --image-version "composer-2.16.1-airflow-2.9.3" \
  --env-variables=GCS_BUCKET="$GCS_BUCKET_LIVE" \
  --airflow-configs=email-email_backend=airflow.utils.email.send_email_smtp \
  --airflow-configs=smtp-smtp_host="smtp.gmail.com" \
  --airflow-configs=smtp-smtp_port="587" \
  --airflow-configs=smtp-smtp_starttls="True" \
  --airflow-configs=smtp-smtp_ssl="False" \
  --airflow-configs=smtp-smtp_mail_from="$SMTP_MAIL_FROM"
```

### Export the Composer GCS bucket for reuse

Cloud Composer creates a Google Cloud Storage bucket internally to store DAGs, logs, and runtime files. This bucket name is not automatically exposed to your local shell, but you often need it for tasks like uploading data or configuring Airflow variables.

Note that this might be wrong as we are not allowing the user to set the production bucket.

```bash
export DAG_GCS_PREFIX=$(gcloud composer environments describe "$ENV_NAME" \
  --location "$REGION" \
  --format="value(config.dagGcsPrefix)")

export GCS_BUCKET=$(echo "$DAG_GCS_PREFIX" | sed -E 's#^gs://([^/]+)/.*#\1#')

printenv DAG_GCS_PREFIX GCS_BUCKET
```

### Configure SMTP credentials in Airflow

Airflow’s SMTP email backend requires authentication credentials in order to send emails through Gmail. While SMTP host, port, and TLS settings are configured at environment creation time, sensitive credentials (username and password) must be stored separately.

In this step, we create (or replace) the smtp_default Airflow connection and securely store the Gmail login and app password. Airflow automatically uses this connection when sending email notifications via the SMTP backend, so DAG code does not need to reference credentials directly.

```bash
gcloud composer environments run "$ENV_NAME" \
  --location "$REGION" \
  connections -- add smtp_default \
  --conn-type email \
  --conn-host smtp.gmail.com \
  --conn-login joegilldata@gmail.com \
  --conn-password "$SMTP_APP_PASSWORD" \
  --conn-port 587 \
  --conn-extra '{"disable_ssl": true, "disable_tls": false}'
```

If required, you can see if the smtp_default exists.

```bash
gcloud composer environments run "$ENV_NAME" --location "$REGION" connections -- get smtp_default
```

If required, you can delete the smtp_default code.

```bash
gcloud composer environments run "$ENV_NAME" --location "$REGION" connections -- delete smtp_default
```

### Store the Composer GCS bucket in Airflow

Cloud Composer creates and manages a Google Cloud Storage bucket as part of the environment infrastructure. While this bucket is known to Composer, it is not automatically available inside Airflow DAGs.

To make the bucket name accessible at runtime, we store it as an Airflow Variable. This allows DAG code to reference the correct bucket without hardcoding environment-specific values.

```bash
export DAG_GCS_PREFIX=$(gcloud composer environments describe "$ENV_NAME" \
  --location "$REGION" \
  --format="value(config.dagGcsPrefix)")

export GCS_BUCKET=$(echo "$DAG_GCS_PREFIX" | sed -E 's#^gs://([^/]+)/.*#\1#')

printenv DAG_GCS_PREFIX GCS_BUCKET
```

### Verify Airflow variables

List the Airflow variables available to DAGs at runtime. This confirms that required configuration values (such as the Composer GCS bucket name) have been successfully stored in the Airflow metadata database.

You should see only variables that have been explicitly set for DAG runtime use. For this project, that means the GCS_BUCKET variable. You should not see Airflow configuration settings or values from your local environment.

```bash
gcloud composer environments run "$ENV_NAME" \
  --location "$REGION" \
  variables -- list
```

### Sync DAGs to Cloud Composer

Mirror the local dags/ directory to the Cloud Composer–managed DAGs bucket so changes appear in the Airflow UI. This command keeps the remote directory in sync with the local repo, removing stale DAGs and excluding Python cache artifacts (__pycache__, .pyc).
Sync local repo to GCP Composer

```bash
gsutil -m rsync -r -d -x '(.*/__pycache__/.*|.*\.pyc$)' ./dags "$DAG_GCS_PREFIX"
```

## Troubleshooting

### "Cannot connect to Docker daemon"

```bash
# If using Docker Desktop, make sure it's running
# If using Docker Engine in WSL:
sudo service docker start
```

### "Permission denied" on logs/

```bash
# Set correct ownership
sudo chown -R $(id -u):$(id -g) logs/
```

### DAG not appearing

```bash
# Check for syntax errors
docker compose exec airflow-webserver python /opt/airflow/dags/task-3.py

# Or check scheduler logs
docker compose logs airflow-scheduler
```

### Airflow Bash Terminal

When the Airflow container is being run, you can excecute commands within it by creating a bach terminal.

```bash
docker compose exec airflow bash
```

Inside this bash terminal, you can see which google auth files are available at run time.

```bash
ls /home/airflow/.config/gcloud/application_default_credentials.json
```

You can exit the Airflow contianer by running.

```bash
exit
```
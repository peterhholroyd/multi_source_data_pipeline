# Companies House Streaming API - GCP Architecture & Implementation
# ====================================================================

"""
GCP ARCHITECTURE OVERVIEW:
--------------------------

1. COMPUTE: Cloud Run (serverless, auto-scaling)
   - Runs Python streaming client
   - Scales to 0 when not needed
   - Always-on instance for continuous streaming

2. STORAGE: BigQuery
   - Dataset: business_register
   - Table: companies_house
   - Streaming inserts for real-time data

3. SECRETS: Secret Manager
   - Stores Companies House API key
   - IAM-controlled access

4. LOGGING: Cloud Logging
   - Structured logs for monitoring
   - Error tracking and alerting

5. MONITORING: Cloud Monitoring
   - Custom metrics for stream health
   - Alerting policies

6. IAM: Service Account with minimal permissions
   - BigQuery Data Editor
   - Secret Manager Secret Accessor
   - Logging Writer

SECURITY FEATURES:
------------------
- Service account with least privilege
- API keys in Secret Manager
- VPC Service Controls (optional)
- Private IP for Cloud Run (optional)
- Encrypted data at rest (default)
- TLS for data in transit
"""

# main.py - Companies House Streaming Client
# ===========================================

import os
import json
import time
import logging
from datetime import datetime
from typing import Dict, Any, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google.cloud import bigquery
from google.cloud import secretmanager
from google.cloud import logging as cloud_logging

# Initialize Google Cloud clients
bq_client = bigquery.Client()
secret_client = secretmanager.SecretManagerServiceClient()

# Setup structured logging
logging_client = cloud_logging.Client()
logging_client.setup_logging()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class CompaniesHouseStreamer:
    """Streams data from Companies House API to BigQuery"""
    
    def __init__(self):
        self.project_id = os.environ.get('GCP_PROJECT_ID')
        self.dataset_id = 'business_register'
        self.table_id = 'companies_house'
        self.api_key = self._get_api_key()
        self.stream_url = 'https://stream.companieshouse.gov.uk/companies'
        self.session = self._create_session()
        self.table_ref = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
        
    def _get_api_key(self) -> str:
        """Retrieve API key from Secret Manager"""
        try:
            secret_name = os.environ.get('SECRET_NAME', 'companies-house-api-key')
            name = f"projects/{self.project_id}/secrets/{secret_name}/versions/latest"
            response = secret_client.access_secret_version(request={"name": name})
            api_key = response.payload.data.decode('UTF-8')
            logger.info("Successfully retrieved API key from Secret Manager")
            return api_key
        except Exception as e:
            logger.error(f"Failed to retrieve API key: {e}", exc_info=True)
            raise
    
    def _create_session(self) -> requests.Session:
        """Create HTTP session with retry logic"""
        session = requests.Session()
        retry_strategy = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        return session
    
    def _ensure_table_exists(self):
        """Create BigQuery table if it doesn't exist"""
        try:
            bq_client.get_table(self.table_ref)
            logger.info(f"Table {self.table_ref} already exists")
        except Exception:
            schema = [
                bigquery.SchemaField("event_type", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("company_number", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("company_name", "STRING"),
                bigquery.SchemaField("company_status", "STRING"),
                bigquery.SchemaField("event_timestamp", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("ingestion_timestamp", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("raw_data", "JSON", mode="REQUIRED"),
            ]
            
            table = bigquery.Table(self.table_ref, schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="event_timestamp"
            )
            
            bq_client.create_table(table)
            logger.info(f"Created table {self.table_ref}")
    
    def _transform_event(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform Companies House event to BigQuery row format"""
        now = datetime.utcnow().isoformat()
        
        # Extract key fields
        resource = event_data.get('resource_kind', '')
        event_type = event_data.get('event', {}).get('type', 'unknown')
        
        data = event_data.get('data', {})
        
        row = {
            'event_type': event_type,
            'company_number': data.get('company_number', ''),
            'company_name': data.get('company_name', ''),
            'company_status': data.get('company_status', ''),
            'event_timestamp': event_data.get('event', {}).get('published_at', now),
            'ingestion_timestamp': now,
            'raw_data': json.dumps(event_data)
        }
        
        return row
    
    def _write_to_bigquery(self, rows: list):
        """Write rows to BigQuery using streaming insert"""
        try:
            errors = bq_client.insert_rows_json(self.table_ref, rows)
            
            if errors:
                logger.error(f"BigQuery insert errors: {errors}")
                return False
            else:
                logger.info(f"Successfully inserted {len(rows)} rows to BigQuery")
                return True
                
        except Exception as e:
            logger.error(f"Failed to write to BigQuery: {e}", exc_info=True)
            return False
    
    def stream(self, timepoint: Optional[int] = None):
        """
        Stream events from Companies House API
        
        Args:
            timepoint: Optional starting point for the stream (for recovery)
        """
        self._ensure_table_exists()
        
        # Setup streaming request
        params = {'timepoint': timepoint} if timepoint else {}
        auth = (self.api_key, '')
        
        logger.info(f"Starting Companies House stream from timepoint: {timepoint or 'latest'}")
        
        batch = []
        batch_size = 100  # Batch inserts for efficiency
        last_timepoint = timepoint
        events_processed = 0
        
        try:
            with self.session.get(
                self.stream_url,
                auth=auth,
                params=params,
                stream=True,
                timeout=(10, 300)  # Connect timeout, read timeout
            ) as response:
                
                response.raise_for_status()
                logger.info(f"Connected to stream. Status: {response.status_code}")
                
                for line in response.iter_lines():
                    if not line:
                        continue
                    
                    try:
                        # Parse event
                        event_data = json.loads(line.decode('utf-8'))
                        
                        # Update timepoint for recovery
                        if 'event' in event_data:
                            last_timepoint = event_data['event'].get('timepoint', last_timepoint)
                        
                        # Transform and batch
                        row = self._transform_event(event_data)
                        batch.append(row)
                        events_processed += 1
                        
                        # Write batch
                        if len(batch) >= batch_size:
                            self._write_to_bigquery(batch)
                            logger.info(
                                f"Progress: {events_processed} events processed, "
                                f"last timepoint: {last_timepoint}"
                            )
                            batch = []
                        
                    except json.JSONDecodeError as e:
                        logger.warning(f"Failed to parse event: {e}")
                        continue
                    except Exception as e:
                        logger.error(f"Error processing event: {e}", exc_info=True)
                        continue
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Stream connection error: {e}", exc_info=True)
            # Write remaining batch
            if batch:
                self._write_to_bigquery(batch)
            raise
        except Exception as e:
            logger.error(f"Unexpected error in stream: {e}", exc_info=True)
            # Write remaining batch
            if batch:
                self._write_to_bigquery(batch)
            raise
        finally:
            logger.info(
                f"Stream ended. Total events processed: {events_processed}, "
                f"Last timepoint: {last_timepoint}"
            )


def main():
    """Main entry point"""
    logger.info("Starting Companies House Streaming Service")
    
    # Get starting timepoint from environment (for recovery)
    timepoint = os.environ.get('STARTING_TIMEPOINT')
    if timepoint:
        timepoint = int(timepoint)
    
    streamer = CompaniesHouseStreamer()
    
    # Run with automatic restart on failure
    max_retries = 5
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            streamer.stream(timepoint=timepoint)
            retry_count = 0  # Reset on success
        except Exception as e:
            retry_count += 1
            wait_time = min(2 ** retry_count, 300)  # Exponential backoff, max 5 min
            logger.error(
                f"Stream failed (attempt {retry_count}/{max_retries}). "
                f"Retrying in {wait_time}s: {e}"
            )
            time.sleep(wait_time)
    
    logger.critical("Max retries reached. Service stopping.")


if __name__ == '__main__':
    main()


# ==============================================================================
# DEPLOYMENT FILES
# ==============================================================================

# requirements.txt
# ----------------
"""
google-cloud-bigquery==3.14.1
google-cloud-secret-manager==2.18.1
google-cloud-logging==3.9.0
requests==2.31.0
urllib3==2.1.0
"""

# Dockerfile
# ----------
"""
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main.py .

# Run as non-root user
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Start the service
CMD ["python", "main.py"]
"""

# terraform/main.tf (Infrastructure as Code)
# -------------------------------------------
"""
terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Service Account
resource "google_service_account" "companies_house_sa" {
  account_id   = "companies-house-streamer"
  display_name = "Companies House Streamer Service Account"
}

# IAM Roles
resource "google_project_iam_member" "bq_data_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.companies_house_sa.email}"
}

resource "google_project_iam_member" "bq_user" {
  project = var.project_id
  role    = "roles/bigquery.user"
  member  = "serviceAccount:${google_service_account.companies_house_sa.email}"
}

resource "google_project_iam_member" "secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.companies_house_sa.email}"
}

resource "google_project_iam_member" "logging_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.companies_house_sa.email}"
}

# BigQuery Dataset
resource "google_bigquery_dataset" "business_register" {
  dataset_id                 = "business_register"
  location                   = var.region
  default_table_expiration_ms = null
  
  access {
    role          = "OWNER"
    user_by_email = google_service_account.companies_house_sa.email
  }
}

# Secret Manager Secret
resource "google_secret_manager_secret" "api_key" {
  secret_id = "companies-house-api-key"
  
  replication {
    auto {}
  }
}

# Cloud Run Service
resource "google_cloud_run_v2_service" "companies_house_streamer" {
  name     = "companies-house-streamer"
  location = var.region
  
  template {
    service_account = google_service_account.companies_house_sa.email
    
    containers {
      image = "gcr.io/${var.project_id}/companies-house-streamer:latest"
      
      env {
        name  = "GCP_PROJECT_ID"
        value = var.project_id
      }
      
      env {
        name  = "SECRET_NAME"
        value = google_secret_manager_secret.api_key.secret_id
      }
      
      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
      }
    }
    
    scaling {
      min_instance_count = 1  # Always on for continuous streaming
      max_instance_count = 1  # Single instance sufficient
    }
  }
  
  traffic {
    type    = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
    percent = 100
  }
}

variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "europe-west2"  # London region for UK data
}
"""

# deploy.sh (Deployment Script)
# ------------------------------
"""
#!/bin/bash
set -e

PROJECT_ID="your-project-id"
REGION="europe-west2"
SERVICE_NAME="companies-house-streamer"

echo "Building Docker image..."
docker build -t gcr.io/${PROJECT_ID}/${SERVICE_NAME}:latest .

echo "Pushing to Container Registry..."
docker push gcr.io/${PROJECT_ID}/${SERVICE_NAME}:latest

echo "Deploying to Cloud Run..."
gcloud run deploy ${SERVICE_NAME} \
  --image gcr.io/${PROJECT_ID}/${SERVICE_NAME}:latest \
  --region ${REGION} \
  --platform managed \
  --min-instances 1 \
  --max-instances 1 \
  --memory 512Mi \
  --cpu 1 \
  --no-allow-unauthenticated \
  --service-account companies-house-streamer@${PROJECT_ID}.iam.gserviceaccount.com

echo "Deployment complete!"
"""
# multi_source_data_pipeline
# Companies House Streamer

GCP-based service for streaming Companies House UK data to BigQuery.

## Features
- Real-time streaming from Companies House API
- Secure credential management with Secret Manager
- Automated BigQuery ingestion
- Comprehensive logging and monitoring
- Resilient with automatic retry logic

## Architecture
- **Compute**: Cloud Run
- **Storage**: BigQuery
- **Secrets**: Secret Manager
- **Logging**: Cloud Logging

## Prerequisites
- GCP Project
- Companies House API Key
- gcloud CLI installed

## Deployment

See [deployment instructions](docs/DEPLOYMENT.md) for details.

## License
MIT

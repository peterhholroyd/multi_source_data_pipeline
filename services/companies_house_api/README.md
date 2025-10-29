# Companies House API Service

GCP-based Python service for extracting data from Companies House UK public API and storing it in BigQuery.

## Overview

This service implements a 3-step workflow to search, retrieve, and store Companies House data:

1. **Step 1: Search** - Search for companies matching a postcode pattern (e.g., `BN6*`)
2. **Step 2: Profiles** - Retrieve detailed company profiles and store in BigQuery
3. **Step 3: Addresses** - Retrieve registered office addresses and store in BigQuery

## Architecture

### GCP Components

- **Compute**: Cloud Run (serverless, auto-scaling)
- **Storage**: BigQuery
  - Dataset: `business_register`
  - Tables: `companies_house_profile`, `companies_house_addresses`
- **Secrets**: Secret Manager
  - `CH-REST-API`: Companies House API key
  - `GOOGLE_API_KEY`: Google API key
- **Logging**: Cloud Logging (logs to `linknsync` project)
- **Project**: `multi-source-data-pipeline`

### Security Features

- API keys stored in GCP Secret Manager
- Service account with least privilege IAM roles
- Encrypted data at rest (default in GCP)
- TLS for data in transit
- Structured logging for audit trails

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `GCP_PROJECT_ID` | GCP project ID | `multi-source-data-pipeline` |
| `LOG_PROJECT` | Project for logging | `linknsync` |
| `DATASET_ID` | BigQuery dataset | `business_register` |
| `SEARCH_POSTCODE` | Postcode pattern to search | `BN6*` |
| `MAX_RESULTS` | Max companies to process | None (all) |
| `BATCH_SIZE` | BigQuery batch insert size | 100 |

### Secrets (in Secret Manager)

- `CH-REST-API`: Companies House API key
- `GOOGLE_API_KEY`: Google API key

## BigQuery Schema

### Dataset: `business_register`

Auto-created if it doesn't exist.

### Table: `companies_house_profile`

Schema is automatically inferred from the API response of `GET /company/{companyNumber}`.

Example fields:
- `company_number` (STRING)
- `company_name` (STRING)
- `company_status` (STRING)
- `date_of_creation` (STRING)
- `jurisdiction` (STRING)
- `type` (STRING)
- All other fields from the API response
- `ingestion_timestamp` (TIMESTAMP)
- `source` (STRING)

### Table: `companies_house_addresses`

Schema is automatically inferred from the API response of `GET /company/{companyNumber}/registered-office-address`.

Example fields:
- `company_number` (STRING)
- `address_line_1` (STRING)
- `address_line_2` (STRING)
- `locality` (STRING)
- `postal_code` (STRING)
- `country` (STRING)
- All other fields from the API response
- `ingestion_timestamp` (TIMESTAMP)
- `source` (STRING)

## Installation

### Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export GCP_PROJECT_ID="multi-source-data-pipeline"
export LOG_PROJECT="linknsync"
export SEARCH_POSTCODE="BN6*"

# Run the service
python -m services.companies_house_api.main
```

### GCP Deployment

See [Deployment Guide](#deployment) below.

## Usage

### Running the Service

```python
from services.companies_house_api import CompaniesHouseService, Config

# Create configuration
config = Config.from_env()

# Create and run service
service = CompaniesHouseService(config)
result = service.run()

print(f"Companies found: {result['companies_found']}")
print(f"Profiles stored: {result['profiles_stored']}")
print(f"Addresses stored: {result['addresses_stored']}")
```

### Command Line

```bash
python -m services.companies_house_api.main
```

## API Rate Limiting

The service implements respectful rate limiting:
- 0.5 second delay between search result pages
- 1 second delay every 10 company detail requests
- Exponential backoff retry on errors (429, 500, 502, 503, 504)
- Maximum 3 retries per request

## Error Handling

- **Authentication errors**: Check API key in Secret Manager
- **Not found errors**: Logged but don't stop execution
- **Network errors**: Automatic retry with exponential backoff
- **BigQuery errors**: Detailed error messages in logs

All errors are logged to the `linknsync` project in Cloud Logging.

## Monitoring

### Logs

View logs in Cloud Logging (linknsync project):

```bash
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=companies-house-api" \
  --project=linknsync \
  --limit=50
```

### Metrics to Monitor

- Number of companies found
- Success/failure rates for profiles and addresses
- API request latencies
- BigQuery insert success rates
- Service execution duration

## Deployment

### Prerequisites

1. **GCP Project**: `multi-source-data-pipeline`
2. **Service Account** with roles:
   - `roles/bigquery.dataEditor`
   - `roles/bigquery.user`
   - `roles/secretmanager.secretAccessor`
   - `roles/logging.logWriter`
3. **Secrets in Secret Manager**:
   - `CH-REST-API`: Your Companies House API key
   - `GOOGLE_API_KEY`: Your Google API key

### Deploy to Cloud Run

```bash
cd services/companies_house_api/deploy

# Build and deploy
./deploy.sh
```

Or manually:

```bash
# Build Docker image
docker build -t gcr.io/multi-source-data-pipeline/companies-house-api:latest .

# Push to Container Registry
docker push gcr.io/multi-source-data-pipeline/companies-house-api:latest

# Deploy to Cloud Run
gcloud run deploy companies-house-api \
  --image gcr.io/multi-source-data-pipeline/companies-house-api:latest \
  --region europe-west2 \
  --platform managed \
  --memory 512Mi \
  --cpu 1 \
  --no-allow-unauthenticated \
  --project multi-source-data-pipeline
```

### Schedule with Cloud Scheduler

Run the service on a schedule:

```bash
# Create Cloud Scheduler job (runs daily at 2am)
gcloud scheduler jobs create http companies-house-daily \
  --schedule="0 2 * * *" \
  --uri="https://companies-house-api-xxx.run.app" \
  --http-method=POST \
  --oidc-service-account-email=companies-house-sa@multi-source-data-pipeline.iam.gserviceaccount.com \
  --location=europe-west2 \
  --project=multi-source-data-pipeline
```

## Development

### Project Structure

```
services/companies_house_api/
├── __init__.py                    # Package initialization
├── main.py                        # Main orchestrator (3-step workflow)
├── config.py                      # Configuration management
├── companies_house_client.py      # REST API client
├── bigquery_manager.py            # BigQuery operations
├── secrets_manager.py             # Secret Manager integration
├── logging_config.py              # Logging setup
├── requirements.txt               # Python dependencies
├── README.md                      # This file
└── deploy/                        # Deployment files
    ├── Dockerfile
    └── deploy.sh
```

### Running Tests

```bash
# Unit tests
pytest tests/

# Integration tests (requires GCP credentials)
pytest tests/integration/
```

## Troubleshooting

### Common Issues

1. **Authentication Failed**
   - Check Secret Manager has `CH-REST-API` secret
   - Verify service account has `secretmanager.secretAccessor` role

2. **BigQuery Permission Denied**
   - Verify service account has `bigquery.dataEditor` role
   - Check dataset exists and is in EU region

3. **No Companies Found**
   - Verify postcode pattern (e.g., `BN6*`)
   - Check Companies House API is accessible
   - Verify API key is valid

4. **Logs Not Appearing**
   - Check `linknsync` project exists
   - Verify service account has `logging.logWriter` role
   - Check Cloud Logging for service name `companies-house-api`

## References

- [Companies House API Documentation](https://developer.company-information.service.gov.uk/)
- [GCP BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [GCP Secret Manager Documentation](https://cloud.google.com/secret-manager/docs)
- [GCP Cloud Logging Documentation](https://cloud.google.com/logging/docs)

## License

Proprietary - Internal use only

## Support

For issues or questions, contact the data engineering team or check project logs in `linknsync`.

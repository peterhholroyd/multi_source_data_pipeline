#!/bin/bash
set -e

# Configuration
PROJECT_ID="multi-source-data-pipeline"
REGION="europe-west2"  # London region for UK data
SERVICE_NAME="companies-house-api"
IMAGE_NAME="gcr.io/${PROJECT_ID}/${SERVICE_NAME}"
SERVICE_ACCOUNT="${SERVICE_NAME}-sa@${PROJECT_ID}.iam.gserviceaccount.com"

echo "=========================================="
echo "Companies House API - Deployment Script"
echo "=========================================="
echo "Project: ${PROJECT_ID}"
echo "Region: ${REGION}"
echo "Service: ${SERVICE_NAME}"
echo "=========================================="

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null; then
    echo "Error: gcloud CLI is not installed"
    echo "Install from: https://cloud.google.com/sdk/docs/install"
    exit 1
fi

# Set the project
echo "Setting GCP project..."
gcloud config set project ${PROJECT_ID}

# Enable required APIs
echo "Enabling required GCP APIs..."
gcloud services enable \
    cloudbuild.googleapis.com \
    run.googleapis.com \
    secretmanager.googleapis.com \
    bigquery.googleapis.com \
    logging.googleapis.com

# Build the Docker image
echo "Building Docker image..."
cd ..  # Go to service root
docker build -t ${IMAGE_NAME}:latest -f deploy/Dockerfile .

# Push to Container Registry
echo "Pushing image to Container Registry..."
docker push ${IMAGE_NAME}:latest

# Deploy to Cloud Run
echo "Deploying to Cloud Run..."
gcloud run deploy ${SERVICE_NAME} \
    --image ${IMAGE_NAME}:latest \
    --region ${REGION} \
    --platform managed \
    --memory 512Mi \
    --cpu 1 \
    --timeout 3600 \
    --max-instances 1 \
    --min-instances 0 \
    --no-allow-unauthenticated \
    --service-account ${SERVICE_ACCOUNT} \
    --set-env-vars "GCP_PROJECT_ID=${PROJECT_ID},LOG_PROJECT=linknsync,DATASET_ID=business_register,SEARCH_POSTCODE=BN6*"

echo "=========================================="
echo "Deployment Complete!"
echo "=========================================="

# Get the service URL
SERVICE_URL=$(gcloud run services describe ${SERVICE_NAME} \
    --region ${REGION} \
    --format 'value(status.url)')

echo "Service URL: ${SERVICE_URL}"
echo ""
echo "To invoke the service:"
echo "  gcloud run services proxy ${SERVICE_NAME} --region ${REGION}"
echo ""
echo "To view logs:"
echo "  gcloud logging read \"resource.type=cloud_run_revision AND resource.labels.service_name=${SERVICE_NAME}\" --project=linknsync --limit=50"
echo ""
echo "=========================================="

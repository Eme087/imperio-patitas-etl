#!/bin/bash
# deploy.sh - Script de deploy para Cloud Run

set -e

PROJECT_ID="tu-proyecto-gcp"
REGION="us-central1"
SERVICE_NAME="imperio-patitas-etl"

echo "🚀 Desplegando Imperio Patitas ETL en Cloud Run..."

# 1. Construir y subir imagen
echo "📦 Construyendo imagen Docker..."
docker build -t gcr.io/${PROJECT_ID}/${SERVICE_NAME}:latest .

echo "⬆️ Subiendo imagen a Container Registry..."
docker push gcr.io/${PROJECT_ID}/${SERVICE_NAME}:latest

# 2. Deploy a Cloud Run
echo "☁️ Desplegando en Cloud Run..."
gcloud run deploy ${SERVICE_NAME} \
  --image gcr.io/${PROJECT_ID}/${SERVICE_NAME}:latest \
  --region ${REGION} \
  --platform managed \
  --allow-unauthenticated \
  --memory 2Gi \
  --cpu 2 \
  --timeout 3600 \
  --concurrency 1 \
  --max-instances 1 \
  --set-env-vars BIGQUERY_PROJECT=${PROJECT_ID},BIGQUERY_DATASET=imperio_patitas \
  --update-secrets BSALE_API_TOKEN=bsale-api-token:latest

# 3. Obtener URL del servicio
SERVICE_URL=$(gcloud run services describe ${SERVICE_NAME} --region=${REGION} --format="value(status.url)")

echo "✅ Deploy completado!"
echo "🌐 URL del servicio: ${SERVICE_URL}"
echo "📋 Endpoints disponibles:"
echo "   - Health: ${SERVICE_URL}/scheduler/health"
echo "   - ETL Diario: ${SERVICE_URL}/api/v1/scheduler/etl/daily"
echo "   - ETL Incremental: ${SERVICE_URL}/api/v1/scheduler/etl/incremental"

# 4. Configurar Cloud Scheduler (si no existe)
echo "⏰ Configurando Cloud Scheduler..."

# ETL Diario (6:00 AM)
gcloud scheduler jobs create http etl-daily \
  --schedule="0 6 * * *" \
  --uri="${SERVICE_URL}/api/v1/scheduler/etl/daily" \
  --http-method=POST \
  --oidc-service-account-email=scheduler-sa@${PROJECT_ID}.iam.gserviceaccount.com \
  --location=${REGION} || echo "Job etl-daily ya existe"

# ETL Incremental (cada 4 horas)  
gcloud scheduler jobs create http etl-incremental \
  --schedule="0 */4 * * *" \
  --uri="${SERVICE_URL}/api/v1/scheduler/etl/incremental?days=1" \
  --http-method=POST \
  --oidc-service-account-email=scheduler-sa@${PROJECT_ID}.iam.gserviceaccount.com \
  --location=${REGION} || echo "Job etl-incremental ya existe"

echo "🎉 Deploy y configuración completados!"

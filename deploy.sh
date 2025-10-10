#!/bin/bash
set -euo pipefail

# 1. Configuración de variables
export PROJECT_ID="imperio-patitas-cloud"
export REGION="southamerica-west1"
export SERVICE_NAME="imperio-patitas-etl"
export IMAGE_REPO="etl-repo"

# URL completa de la imagen del contenedor
export IMAGE_URL="${REGION}-docker.pkg.dev/${PROJECT_ID}/${IMAGE_REPO}/${SERVICE_NAME}:latest"

echo ">>> Iniciando despliegue del servicio: ${SERVICE_NAME}"

# 2. Construir y subir la imagen a Artifact Registry
echo "--> Paso 1/2: Construyendo y subiendo la imagen..."
gcloud builds submit --tag "${IMAGE_URL}" .

# 3. Desplegar la nueva imagen en Cloud Run
#    Hemos eliminado las banderas --timeout y --set-env-vars para que
#    respete la configuración existente en la consola de Cloud Run.
echo "--> Paso 2/2: Desplegando en Cloud Run..."
gcloud run deploy "${SERVICE_NAME}" \
  --image "${IMAGE_URL}" \
  --region "${REGION}" \
  --platform "managed" \
  --allow-unauthenticated \
  --max-instances "1" \
  --set-secrets="BSALE_API_TOKEN=imperio-patitas-bsale-token:latest"

echo ">>> ¡Despliegue completado!"
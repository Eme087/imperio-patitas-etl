# Imperio Patitas ETL - Cloud Run

Este proyecto es un servicio ETL (Extract, Transform, Load) construido con **FastAPI** y desplegado en **Google Cloud Run**. Su propósito es extraer datos de la API de **Bsale**, transformarlos y cargarlos de manera eficiente en **Google BigQuery** con integridad de datos.

## 🚀 Características

- **🔄 Sincronización Automática**: Cloud Scheduler ejecuta el ETL diariamente a las 2:00 AM (Chile)
- **🛡️ Sin Duplicados**: Operaciones MERGE en BigQuery evitan datos duplicados
- **📊 BigQuery Native**: Carga directa usando el cliente oficial de Google Cloud
- **✅ Validación Estricta**: Productos sin precio/costo válido son rechazados automáticamente
- **🎯 Reglas de Negocio**: 
  - Precios desde lista 2 de Bsale (obligatorio)
  - Costos desde endpoint específico de Bsale
  - Cálculo automático: costo = precio × 0.65 (cuando no hay historial)
- **🏗️ Arquitectura Cloud**: Desplegado en Cloud Run con escalado automático
- **🔐 Seguridad**: Autenticación OIDC y variables de entorno seguras

## 📋 Entidades Sincronizadas

| Entidad | Tabla BigQuery | Clave Única | Descripción |
|---------|----------------|-------------|-------------|
| **Clientes** | `cliente` | `id_bsale` | Información de clientes (RUT opcional) |
| **Productos** | `producto` | `id_bsale` | Variantes con precios y costos validados |
| **Documentos** | `documento_venta` | `id_bsale` | Facturas, boletas y otros documentos |
| **Detalles** | `detalle_documento` | `id_documento + id_producto` | Líneas de documentos |

## 🏗️ Arquitectura Cloud

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  Cloud Scheduler │───►│   Cloud Run      │───►│   BigQuery      │
│                 │    │                  │    │                 │
│ Diario 2:00 AM  │    │ FastAPI ETL      │    │ Dataset:        │
│ (Chile)         │    │ Auto-scaling     │    │ imperio_patitas │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │   Bsale API      │
                       │                  │
                       │ - Products       │
                       │ - Clients        │
                       │ - Documents      │
                       │ - Price Lists    │
                       │ - Costs          │
                       └──────────────────┘
```

## 📁 Estructura del Proyecto

```
imperio-patitas-etl/
│
├── app/
│   ├── api/
│   │   ├── endpoints.py           # Endpoints principales ETL
│   │   └── scheduler_endpoints.py # Endpoints para Cloud Scheduler
│   ├── core/
│   │   └── config.py             # Configuración (BigQuery + Bsale)
│   ├── db/
│   │   ├── bigquery_client.py    # Cliente BigQuery con MERGE
│   │   └── models.py             # Esquemas de BigQuery
│   ├── services/
│   │   ├── bsale_client.py       # Cliente API Bsale
│   │   └── etl_service.py        # Lógica ETL principal
│   └── main.py                   # Aplicación FastAPI
│
├── Dockerfile                    # Configuración contenedor
├── cloudbuild.yaml              # Google Cloud Build
├── deploy.sh                    # Script de despliegue
├── requirements.txt             # Dependencias Python
└── documentacion.txt           # Documentación API Bsale
```

## 🛠️ Configuración

### Variables de Entorno (Cloud Run)

```bash
# Configuración BigQuery
BIGQUERY_PROJECT=imperio-patitas-cloud
BIGQUERY_DATASET=imperio_patitas_bsale

# Token Bsale (desde Secret Manager)
BSALE_API_TOKEN=<secret>
```

### Tablas BigQuery

Las tablas se crean automáticamente con los esquemas definidos en `app/db/models.py`:

- `cliente`: Información de clientes
- `producto`: Variantes con precios y costos
- `documento_venta`: Documentos de venta
- `detalle_documento`: Líneas de documentos

## 📡 API Endpoints

### Producción (Cloud Run)
- **Base URL**: `https://imperio-patitas-etl-24590285888.southamerica-west1.run.app`

### Endpoints Principales

| Método | Endpoint | Descripción |
|--------|----------|-------------|
| `POST` | `/api/v1/etl/sync/all` | Sincronización completa |
| `POST` | `/api/v1/etl/sync/clients` | Solo clientes |
| `POST` | `/api/v1/etl/sync/products` | Solo productos |
| `POST` | `/api/v1/etl/sync/documents` | Solo documentos |
| `GET` | `/health` | Health check |
| `GET` | `/docs` | Documentación Swagger |

### Ejemplos de Uso

```bash
# Sincronización completa
curl -X POST "https://imperio-patitas-etl-24590285888.southamerica-west1.run.app/api/v1/etl/sync/all"

# Solo productos
curl -X POST "https://imperio-patitas-etl-24590285888.southamerica-west1.run.app/api/v1/etl/sync/products"

# Health check
curl "https://imperio-patitas-etl-24590285888.southamerica-west1.run.app/health"
```

## ⏰ Programación Automática

### Cloud Scheduler

- **Job**: `etl-bsale-daily`
- **Horario**: Todos los días a las 2:00 AM (Chile)
- **Endpoint**: `/api/v1/etl/sync/all`
- **Autenticación**: OIDC Token
- **Ubicación**: `southamerica-east1`

## 🔍 Lógica de Negocio

### Validación de Productos

1. **Precio obligatorio**: Debe existir en lista 2 de Bsale
2. **Costo inteligente**:
   - Si existe historial de costo → usar `averageCost`
   - Si NO existe historial → calcular `precio × 0.65`
3. **Rechazo automático**: Productos sin precio válido son omitidos

### Prevención de Duplicados

```sql
-- Ejemplo de operación MERGE usado internamente
MERGE `proyecto.dataset.producto` AS target
USING (SELECT * FROM UNNEST([...])) AS source
ON target.id_bsale = source.id_bsale
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...
```

## 📊 Monitoreo

### Cloud Run Logs
```bash
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=imperio-patitas-etl" --limit=50
```

### BigQuery Queries
```sql
-- Verificar datos cargados
SELECT COUNT(*) FROM `imperio-patitas-cloud.imperio_patitas_bsale.producto`;
SELECT COUNT(*) FROM `imperio-patitas-cloud.imperio_patitas_bsale.cliente`;
```

## 🚀 Despliegue

El proyecto se despliega automáticamente en Cloud Run:

```bash
# Despliegue manual
gcloud run deploy imperio-patitas-etl \
  --source . \
  --region southamerica-west1 \
  --set-env-vars BIGQUERY_PROJECT=imperio-patitas-cloud,BIGQUERY_DATASET=imperio_patitas_bsale
```

## ✅ Estado del Proyecto

- **✅ Desplegado**: Cloud Run en producción
- **✅ Programado**: Scheduler diario configurado
- **✅ Datos limpios**: Sin duplicados en BigQuery
- **✅ Validación estricta**: Precios y costos obligatorios
- **✅ Monitoreo**: Logs en Cloud Logging

---

**🏢 Imperio Patitas - ETL v2.0**  
*Sincronización automática Bsale → BigQuery*
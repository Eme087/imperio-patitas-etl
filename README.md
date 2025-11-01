# Servicio ETL para Imperio Patitas

Este proyecto es un servicio de ETL (Extract, Transform, Load) construido con **FastAPI**. Su propósito es extraer datos de la API de **Bsale**, transformarlos y cargarlos de manera eficiente en una base de datos MySQL, optimizada para entornos como Google Cloud SQL.

## Características

- **API para Sincronización**: Expone endpoints RESTful para iniciar la sincronización de diferentes entidades.
- **Arquitectura Modular**: El código está organizado por responsabilidades (`api`, `services`, `db`, `core`), facilitando su mantenimiento y escalabilidad.
- **Configuración Segura**: Carga variables sensibles (como tokens de API y URLs de base de datos) desde un archivo `.env` usando Pydantic.
- **ORM con SQLAlchemy**: Define los modelos de la base de datos y gestiona las sesiones de forma robusta.
- **Carga de Datos Eficiente (UPSERT)**: Utiliza sentencias `INSERT ... ON DUPLICATE KEY UPDATE` para insertar o actualizar registros de forma atómica. Esto es crucial para mantener los datos sincronizados sin generar duplicados.
- **Procesamiento por Lotes (Chunking)**: La sincronización de entidades grandes (como los documentos de venta) se realiza en lotes para optimizar el uso de memoria y asegurar la fiabilidad del proceso, haciendo commit a la base de datos después de cada lote.
- **Cliente de API Centralizado**: Toda la comunicación con la API de Bsale se gestiona a través de un único cliente que maneja la autenticación y la paginación.
- **Manejo de Dependencias**: Utiliza un archivo `requirements.txt` para una fácil instalación del entorno.

---

## Arquitectura del Proyecto

La estructura del proyecto está diseñada para ser clara y escalable:

```
imperio-patitas-etl/
# Servicio ETL para Imperio Patitas

Este proyecto es un servicio ETL (Extract, Transform, Load) construido con **FastAPI**. Extrae datos de la API de **Bsale**, los transforma y los carga en **Google BigQuery** (antes MySQL).

## Características

- **API para Sincronización**: Endpoints RESTful para iniciar la sincronización de entidades.
- **Arquitectura Modular**: Código organizado por responsabilidades (`api`, `services`, `db`, `core`).
- **Configuración Segura**: Variables sensibles desde `.env` usando Pydantic.
- **Carga en BigQuery**: Los datos se insertan directamente en BigQuery usando el cliente oficial de Google Cloud.
- **Procesamiento por Lotes**: Sincronización eficiente y robusta, con control de integridad.
- **Cliente de API Centralizado**: Toda la comunicación con Bsale se gestiona en un único cliente.
- **Reglas de Negocio Estrictas**: El ETL extrae precios solo de la lista 2 de Bsale. Si falta el precio de una variante, el proceso se detiene y alerta (no se rellena con cero).
- **Manejo de Dependencias**: Instalación fácil vía `requirements.txt`.

---

## Arquitectura del Proyecto

La estructura del proyecto está diseñada para ser clara y escalable:

```
imperio-patitas-etl/
│
├── app/
│   ├── api/
│   │   └── endpoints.py      # Endpoints de la API.
│   ├── core/
│   │   └── config.py         # Configuración y variables de entorno.
│   ├── db/
│   │   └── bigquery_client.py # Cliente para Google BigQuery.
│   ├── services/
│   │   ├── bsale_client.py   # Cliente para la API de Bsale.
│   │   └── etl_service.py    # Lógica ETL principal.
│   └── main.py               # Entrada FastAPI.
│
├── .env                      # Variables de entorno.
└── requirements.txt          # Dependencias.
```

---

## Guía de Instalación y Uso

### 1. Prerrequisitos

- Python 3.8 o superior.
- Acceso a Google BigQuery y credenciales de GCP.

### 2. Clonar el Repositorio

```bash
git clone <url-del-repositorio>
cd imperio-patitas-etl
```

### 3. Configurar el Entorno

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 4. Configurar Variables de Entorno

Crea un archivo `.env` en la raíz con:

```ini
# Configuración de BigQuery
BIGQUERY_PROJECT_ID="tu_project_id"
BIGQUERY_DATASET="tu_dataset"
BIGQUERY_TABLE_CLIENTS="tabla_clientes"
BIGQUERY_TABLE_PRODUCTS="tabla_productos"
BIGQUERY_TABLE_DOCUMENTS="tabla_documentos"

# Token de acceso para la API de Bsale
BSALE_API_TOKEN="tu_token_secreto_de_bsale"
```

Asegúrate de tener configuradas las credenciales de GCP (Application Default Credentials).

### 5. Ejecutar la Aplicación

```bash
uvicorn app.main:app --reload
```

La API estará disponible en `http://127.0.0.1:8000/docs`.

---

## Endpoints de la API

### Sincronizar Entidades

- **POST** `/etl/sync/{entity}`  
  Sincroniza una entidad (`clients`, `products`, `documents`, `all`).  
  Si falta el precio de una variante en la lista 2, el proceso se detiene y alerta.

**Ejemplo:**

```bash
curl -X POST "http://127.0.0.1:8000/etl/sync/all"
```

### Chequeo de Salud

- **GET** `/health`  
  Verifica que el servicio está activo.
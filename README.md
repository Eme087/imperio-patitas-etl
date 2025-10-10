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
│
├── app/
│   ├── api/
│   │   └── endpoints.py      # Define los endpoints de la API (rutas).
│   ├── core/
│   │   └── config.py         # Gestiona la configuración desde el archivo .env.
│   ├── db/
│   │   ├── base.py           # Configura el motor y la sesión de SQLAlchemy.
│   │   └── models.py         # Define las tablas de la BBDD como modelos de SQLAlchemy.
│   ├── services/
│   │   ├── bsale_client.py   # Cliente para interactuar con la API de Bsale.
│   │   └── etl_service.py    # Contiene toda la lógica de ETL (extracción, transformación, carga).
│   └── main.py               # Punto de entrada de la aplicación FastAPI.
│
├── .env                      # Archivo para variables de entorno (local).
└── requirements.txt          # Dependencias de Python.
```

---

## Guía de Instalación y Uso

### 1. Prerrequisitos

- Python 3.8 o superior.
- Una base de datos MySQL accesible.

### 2. Clonar el Repositorio

```bash
git clone <url-del-repositorio>
cd imperio-patitas-etl
```

### 3. Configurar el Entorno

Se recomienda utilizar un entorno virtual:

```bash
python -m venv venv
source venv/bin/activate  # En Windows: venv\Scripts\activate
```

Instala las dependencias:

```bash
pip install -r requirements.txt
```

### 4. Configurar Variables de Entorno

CrSe ha creado un archivo llamado `.env` en la raíz del proyecto y añade las siguientes variables con tus propios valores:

```ini
# URL de conexión a tu base de datos MySQL
DATABASE_URL="mysql+pymysql://tu_usuario:tu_contraseña@host_de_tu_bbdd/nombre_de_la_bbdd" # Para Cloud SQL, usa el formato con unix_socket

# Token de acceso para la API de Bsale
BSALE_API_TOKEN="tu_token_secreto_de_bsale"
```

### 5. Ejecutar la Aplicación

Usa `uvicorn` para iniciar el servidor de desarrollo:

```bash
uvicorn app.main:app --reload
```

El servidor estará disponible en `http://127.0.0.1:8000`. Puedes acceder a la documentación interactiva de la API (generada por Swagger) en `http://127.0.0.1:8000/docs`.

---

## Endpoints de la API

### Sincronizar Entidades

- **Endpoint**: `/etl/sync/{entity}`
- **Método**: `POST`
- **Descripción**: Ejecuta el proceso de sincronización para una entidad específica.
- **Parámetros de Ruta**:
  - `entity` (string): La entidad a sincronizar. Valores válidos:
    - `clients`: Sincroniza solo los clientes.
    - `products`: Sincroniza productos y variantes.
    - `documents`: Sincroniza documentos de venta y sus detalles.
    - `all`: Ejecuta la sincronización completa en orden (clientes, productos, documentos).
- **Parámetros de Query (Opcional)**:
  - `start_date` (string, formato `YYYY-MM-DD`): Usado solo con `entity=documents` o `entity=all` para sincronizar documentos a partir de una fecha específica.

**Ejemplo de uso con cURL:**

```bash
# Ejecutar una sincronización completa
curl -X POST "http://127.0.0.1:8000/etl/sync/all"

# Ejecutar una sincronización completa, aplicando el filtro de fecha a los documentos
curl -X POST "http://127.0.0.1:8000/etl/sync/all?start_date=2024-07-01"

# Sincronizar solo los productos
curl -X POST "http://127.0.0.1:8000/etl/sync/products"
```

### Chequeo de Salud

- **URL**: `/health`
- **Método**: `GET`
- **Descripción**: Endpoint simple para verificar que el servicio está en funcionamiento. Devuelve `{"status": "ok"}`.
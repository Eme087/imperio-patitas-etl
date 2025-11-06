# main.py
from fastapi import FastAPI
from datetime import datetime
import os
from app.api import endpoints
from app.api import scheduler_endpoints

app = FastAPI(
    title="Imperio Patitas ETL - Cloud Run",
    description="ETL robusto para extraer, transformar y cargar datos desde Bsale a BigQuery con integridad de datos.",
    version="2.0.0-cloud-run"
)

# Incluimos las rutas definidas en el módulo de endpoints
app.include_router(endpoints.router, prefix="/api/v1")
app.include_router(scheduler_endpoints.router, prefix="/api/v1")

@app.get("/")
def root():
    """Endpoint raíz para Cloud Run health check"""
    return {
        "service": "Imperio Patitas ETL",
        "status": "running",
        "timestamp": datetime.now().isoformat(),
        "version": "2.0.0-cloud-run",
        "description": "ETL con integridad de datos para Bsale → BigQuery"
    }

@app.get("/health", tags=["Monitoring"])
def health_check():
    """
    Endpoint de monitoreo para verificar que el servicio está activo.
    """
    return {"status": "ok"}

# Configuración para Cloud Run
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
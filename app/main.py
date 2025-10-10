# main.py
from fastapi import FastAPI
from app.api import endpoints

app = FastAPI(
    title="Servicio ETL para Imperio Patitas",
    description="API para extraer, transformar y cargar datos desde Bsale a Cloud SQL.",
    version="1.0.0"
)

# Incluimos las rutas definidas en el módulo de endpoints
app.include_router(endpoints.router)

@app.get("/health", tags=["Monitoring"])
def health_check():
    """
    Endpoint de monitoreo para verificar que el servicio está activo.
    """
    return {"status": "ok"}
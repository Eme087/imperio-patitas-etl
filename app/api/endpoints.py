# app/api/endpoints.py
from fastapi import APIRouter, Depends, HTTPException
from datetime import date
from typing import Optional
import logging

from app.core.config import settings
from app.db.bigquery_client import get_bq_writer
from app.services import etl_service

router = APIRouter()

# Inyección de dependencias para obtener el writer de BigQuery
def get_db():
    writer = get_bq_writer()
    yield writer

@router.post("/etl/clean-and-reload", tags=["ETL"])
def clean_and_reload(db=Depends(get_db)):
    """
    LIMPIA COMPLETAMENTE todas las tablas de BigQuery y recarga todos los datos desde cero.
    ⚠️ CUIDADO: Esto elimina TODOS los datos existentes y los reemplaza.
    """
    try:
        logging.info("🧹 INICIANDO LIMPIEZA COMPLETA Y RECARGA DE DATOS")
        
        # Limpiar todas las tablas
        logging.info("Eliminando todos los datos de las tablas...")
        db.query(f"DELETE FROM `{settings.BIGQUERY_PROJECT}.{settings.BIGQUERY_DATASET}.cliente` WHERE TRUE")
        db.query(f"DELETE FROM `{settings.BIGQUERY_PROJECT}.{settings.BIGQUERY_DATASET}.producto` WHERE TRUE")
        db.query(f"DELETE FROM `{settings.BIGQUERY_PROJECT}.{settings.BIGQUERY_DATASET}.documento_venta` WHERE TRUE")
        db.query(f"DELETE FROM `{settings.BIGQUERY_PROJECT}.{settings.BIGQUERY_DATASET}.detalle_documento` WHERE TRUE")
        
        logging.info("✅ Tablas limpiadas. Iniciando recarga completa...")
        
        # Recargar todos los datos
        etl_service.sync_clients(db)
        etl_service.sync_products(db)
        etl_service.sync_documents(db)
        
        return {
            "status": "LIMPIEZA Y RECARGA COMPLETADA",
            "message": "Todas las tablas fueron limpiadas y recargadas con datos frescos sin duplicados"
        }
        
    except Exception as e:
        logging.error(f"Error durante limpieza y recarga: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@router.post("/etl/sync/{entity}", tags=["ETL"])
def run_sync(entity: str, start_date: Optional[str] = None, db=Depends(get_db)):
    """
    Ejecuta la sincronización para una entidad específica.
    - Entidades válidas: 'clients', 'products', 'documents', 'all'.
    - Para 'documents' y 'all', se puede usar el parámetro opcional 'start_date' (formato YYYY-MM-DD).
    """
    try:
        logging.info(f"Marcador: inicio run_sync para entidad '{entity}'")
        if entity == "all":
            logging.info("Marcador: sync_clients")
            etl_service.sync_clients(db)
            logging.info("Marcador: sync_products")
            etl_service.sync_products(db)
            if hasattr(db, "commit"):
                db.commit()
            logging.info("Marcador: sync_documents")
            etl_service.sync_documents(db, start_date=start_date)
        elif entity == "clients":
            logging.info("Marcador: sync_clients")
            etl_service.sync_clients(db)
            if hasattr(db, "commit"):
                db.commit()
        elif entity == "products":
            logging.info("Marcador: sync_products")
            etl_service.sync_products(db)
            if hasattr(db, "commit"):
                db.commit()
        elif entity == "documents":
            logging.info("Marcador: sync_documents")
            etl_service.sync_documents(db, start_date=start_date)
        else:
            logging.error(f"Marcador: entidad '{entity}' no encontrada")
            raise HTTPException(status_code=404, detail=f"Entidad '{entity}' no encontrada.")
        logging.info(f"Marcador: fin run_sync para entidad '{entity}'")
        return {"status": "sincronización completada", "entity": entity}

    except Exception as e:
        logging.error(f"Marcador: excepción en run_sync para entidad '{entity}' - {e}")
        # Si el objeto db tiene rollback, lo ejecutamos
        if hasattr(db, "rollback"):
            db.rollback()
        logging.exception(f"Error en la sincronización de '{entity}'")
        raise HTTPException(status_code=500, detail=f"Error en la sincronización de '{entity}': {e}. Revise los logs del servidor para más detalles.")
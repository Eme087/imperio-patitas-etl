# app/api/endpoints.py
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from datetime import date
from typing import Optional
import logging

from app.db.base import SessionLocal
from app.core.config import settings
from app.db.bigquery_client import get_bq_writer
from app.services import etl_service

router = APIRouter()

# Inyección de dependencias para obtener la sesión de la base de datos.
def get_db():
    # Marcador: inicio get_db
    if settings.BIGQUERY_DATASET:
        writer = get_bq_writer()
        # Marcador: yield BigQuery writer
        yield writer
        # Marcador: fin get_db BigQuery
    else:
        db = SessionLocal()
        try:
            # Marcador: yield SQLAlchemy session
            yield db
        finally:
            db.close()
            # Marcador: fin get_db SQLAlchemy

@router.post("/etl/sync/{entity}", tags=["ETL"])
def run_sync(entity: str, start_date: Optional[str] = None, db: Session = Depends(get_db)):
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
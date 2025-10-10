# app/api/endpoints.py
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from datetime import date
from typing import Optional
import logging

from app.db.base import SessionLocal
from app.services import etl_service

router = APIRouter()

# Inyección de dependencias para obtener la sesión de la base de datos.
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.post("/etl/sync/{entity}", tags=["ETL"])
def run_sync(entity: str, start_date: Optional[str] = None, db: Session = Depends(get_db)):
    """
    Ejecuta la sincronización para una entidad específica.
    - Entidades válidas: 'clients', 'products', 'documents', 'all'.
    - Para 'documents' y 'all', se puede usar el parámetro opcional 'start_date' (formato YYYY-MM-DD).
    """
    try:
        if entity == "all":
            etl_service.sync_clients(db)
            etl_service.sync_products(db)
            # Hacemos commit aquí para asegurar que clientes y productos estén guardados
            # antes de empezar con los documentos.
            db.commit() 
            # La función de documentos gestiona sus propios commits por lotes.
            etl_service.sync_documents(db, start_date=start_date)
        elif entity == "clients":
            etl_service.sync_clients(db)
            db.commit()
        elif entity == "products":
            etl_service.sync_products(db)
            db.commit()
        elif entity == "documents":
            etl_service.sync_documents(db, start_date=start_date)
        else:
            raise HTTPException(status_code=404, detail=f"Entidad '{entity}' no encontrada.")
        
        return {"status": "sincronización completada", "entity": entity}

    except Exception as e:
        db.rollback()
        # ¡Mejora Clave! Registramos el traceback completo en los logs.
        logging.exception(f"Error en la sincronización de '{entity}'")
        # Luego, levantamos la excepción HTTP para notificar al cliente.
        raise HTTPException(status_code=500, detail=f"Error en la sincronización de '{entity}'. Revise los logs del servidor para más detalles.")
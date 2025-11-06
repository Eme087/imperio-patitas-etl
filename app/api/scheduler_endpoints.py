# app/api/scheduler_endpoints.py - ENDPOINTS PARA CLOUD SCHEDULER
from fastapi import APIRouter, Depends, HTTPException, Request
from datetime import date, datetime, timedelta
from typing import Optional
import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor

from app.core.config import settings
from app.db.bigquery_client import get_bq_writer
from app.services import etl_service

router = APIRouter()

def get_db():
    writer = get_bq_writer()
    yield writer

@router.post("/scheduler/etl/daily", tags=["Scheduler"])
async def run_daily_etl(request: Request, db=Depends(get_db)):
    """
    Endpoint para Cloud Scheduler - ETL diario completo
    Se ejecuta todos los días a las 6:00 AM
    """
    start_time = datetime.now()
    logging.info(f"🚀 Iniciando ETL diario via Cloud Scheduler - {start_time}")
    
    # Verificar que la request viene de Cloud Scheduler
    user_agent = request.headers.get("user-agent", "")
    if "Google-Cloud-Scheduler" not in user_agent:
        logging.warning(f"⚠️ Request no viene de Cloud Scheduler: {user_agent}")
        # En producción, podrías rechazar requests no autorizadas
        # raise HTTPException(status_code=403, detail="Forbidden")
    
    try:
        # Ejecutar ETL en thread pool para no bloquear
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor(max_workers=1) as executor:
            await loop.run_in_executor(executor, _run_complete_etl, db)
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        result = {
            "status": "success",
            "message": "ETL diario completado exitosamente",
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": duration.total_seconds(),
            "executed_by": "cloud_scheduler"
        }
        
        logging.info(f"✅ ETL diario completado: {duration}")
        return result
        
    except Exception as e:
        logging.error(f"🔴 Error en ETL diario: {e}")
        
        # Rollback si es posible
        if hasattr(db, "rollback"):
            try:
                db.rollback()
            except:
                pass
        
        raise HTTPException(
            status_code=500, 
            detail=f"Error en ETL diario: {str(e)}"
        )

@router.post("/scheduler/etl/incremental", tags=["Scheduler"])
async def run_incremental_etl(request: Request, days: int = 1, db=Depends(get_db)):
    """
    Endpoint para ETL incremental - solo documentos recientes
    Útil para ejecuciones más frecuentes (cada 4 horas)
    """
    start_time = datetime.now()
    logging.info(f"🔄 Iniciando ETL incremental ({days} días) - {start_time}")
    
    try:
        # Solo sincronizar documentos de los últimos X días
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor(max_workers=1) as executor:
            await loop.run_in_executor(
                executor, 
                etl_service.sync_documents, 
                db, 
                start_date
            )
        
        # Commit si es necesario
        if hasattr(db, "commit"):
            db.commit()
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        result = {
            "status": "success",
            "message": f"ETL incremental completado ({days} días)",
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": duration.total_seconds(),
            "days_processed": days,
            "start_date": start_date
        }
        
        logging.info(f"✅ ETL incremental completado: {duration}")
        return result
        
    except Exception as e:
        logging.error(f"🔴 Error en ETL incremental: {e}")
        
        if hasattr(db, "rollback"):
            try:
                db.rollback()
            except:
                pass
        
        raise HTTPException(
            status_code=500, 
            detail=f"Error en ETL incremental: {str(e)}"
        )

@router.get("/scheduler/health", tags=["Scheduler"])
async def health_check():
    """
    Health check para Cloud Run
    """
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "imperio-patitas-etl",
        "bigquery_configured": bool(settings.BIGQUERY_PROJECT and settings.BIGQUERY_DATASET),
        "bsale_configured": bool(settings.BSALE_API_TOKEN)
    }

@router.post("/scheduler/etl/test", tags=["Scheduler"])  
async def test_etl(entity: str = "clients", limit: int = 10, db = Depends(get_db)):
    """
    Endpoint de prueba para validar ETL sin ejecutar todo
    """
    start_time = datetime.now()
    logging.info(f"🧪 Iniciando prueba ETL - {entity}")
    
    try:
        if entity == "clients":
            # Probar solo algunos clientes
            from app.services.bsale_client import bsale_client
            clients = bsale_client.get_clients()[:limit]
            result = {
                "entity": "clients",
                "sample_count": len(clients),
                "first_client": clients[0] if clients else None
            }
        elif entity == "products":
            from app.services.bsale_client import bsale_client
            products = bsale_client._get_all_pages("products.json", params={"limit": limit})
            result = {
                "entity": "products", 
                "sample_count": len(products),
                "first_product": products[0] if products else None
            }
        else:
            raise HTTPException(status_code=400, detail="Entity must be 'clients' or 'products'")
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        result.update({
            "status": "success",
            "duration_seconds": duration.total_seconds(),
            "timestamp": start_time.isoformat()
        })
        
        return result
        
    except Exception as e:
        logging.error(f"🔴 Error en prueba ETL: {e}")
        raise HTTPException(status_code=500, detail=str(e))

def _run_complete_etl(db):
    """Función auxiliar para ejecutar ETL completo (sincrona)"""
    logging.info("📋 Ejecutando sincronización completa...")
    
    # 1. Clientes
    logging.info("👥 Sincronizando clientes...")
    etl_service.sync_clients(db)
    
    # 2. Productos  
    logging.info("📦 Sincronizando productos...")
    etl_service.sync_products(db)
    
    # 3. Documentos (últimos 7 días para no sobrecargar)
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    logging.info(f"📄 Sincronizando documentos desde {start_date}...")
    etl_service.sync_documents(db, start_date=start_date)
    
    # Commit final
    if hasattr(db, "commit"):
        db.commit()
        logging.info("✅ Commit final ejecutado")
    
    logging.info("🎉 ETL completo finalizado")

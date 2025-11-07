import os
import gspread
from google.oauth2.service_account import Credentials
from app.core.config import settings
import logging

SCOPE = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
]

class SheetsSync:
    def __init__(self, sheet_id: str = None, credentials_file: str = None):
        self.sheet_id = sheet_id or settings.GOOGLE_SHEETS_DOC_ID
        self.credentials_file = credentials_file or settings.GOOGLE_SHEETS_CREDENTIALS
        
        if not self.sheet_id or not self.credentials_file:
            raise ValueError("Google Sheets configuración faltante: GOOGLE_SHEETS_DOC_ID y GOOGLE_SHEETS_CREDENTIALS son requeridos")
        
        creds = Credentials.from_service_account_file(self.credentials_file, scopes=SCOPE)
        self.gc = gspread.authorize(creds)
        self.sh = self.gc.open_by_key(self.sheet_id)

    def upsert_table(self, table_name: str, rows: list):
        """
        Crea o reemplaza la hoja con el nombre table_name y carga los datos (incluye encabezados).
        """
        if not rows:
            logging.info(f"📋 No hay datos para sincronizar en Google Sheets: {table_name}")
            return
            
        # Si existe, elimina la hoja
        try:
            worksheet = self.sh.worksheet(table_name)
            self.sh.del_worksheet(worksheet)
            logging.info(f"🗑️ Hoja '{table_name}' eliminada para recrearla")
        except gspread.exceptions.WorksheetNotFound:
            logging.info(f"📄 Creando nueva hoja: {table_name}")
            
        # Crea la hoja
        worksheet = self.sh.add_worksheet(title=table_name, rows=len(rows)+10, cols=50)
        
        # Escribir encabezados y datos
        headers = list(rows[0].keys())
        values = [headers] + [[str(row.get(h, '')) for h in headers] for row in rows]
        worksheet.update('A1', values)
        logging.info(f"✅ Hoja '{table_name}' actualizada con {len(rows)} registros")

    def sync_all(self, data_dict: dict):
        """
        data_dict: {'cliente': [...], 'producto': [...], ...}
        """
        logging.info(f"📊 Iniciando sincronización a Google Sheets...")
        for table, rows in data_dict.items():
            try:
                self.upsert_table(table, rows)
            except Exception as e:
                logging.error(f"🔴 Error sincronizando hoja '{table}' a Google Sheets: {e}")
        logging.info(f"✅ Sincronización a Google Sheets completada")


def get_sheets_sync():
    """Factory function para obtener instancia de SheetsSync si está configurado"""
    if settings.GOOGLE_SHEETS_DOC_ID and settings.GOOGLE_SHEETS_CREDENTIALS:
        return SheetsSync()
    return None

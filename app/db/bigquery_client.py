"""Small BigQuery helper to insert JSON rows into BigQuery tables.

This module provides a lightweight wrapper with a simple `insert_rows(table, rows)`
method so it can be used as a drop-in replacement in the ETL code path.
"""
from typing import List, Dict, Any
import os
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError
from app.core.config import settings


class BigQueryWriter:
    def __init__(self, project: str = None, dataset: str = None):
        self.project = project or settings.BIGQUERY_PROJECT
        self.dataset = dataset or settings.BIGQUERY_DATASET
        if not self.dataset:
            raise ValueError("BIGQUERY_DATASET must be set to use BigQueryWriter")

        # Client uses Application Default Credentials or GOOGLE_APPLICATION_CREDENTIALS
        self.client = bigquery.Client(project=self.project)

    def _table_ref(self, table_name: str) -> str:
        if self.project:
            return f"{self.project}.{self.dataset}.{table_name}"
        return f"{self.dataset}.{table_name}"

    def insert_rows(self, table_name: str, rows: List[Dict[str, Any]]):
        """Insert a list of JSON rows into the target BigQuery table.

        Uses insert_rows_json which performs streaming inserts.
        Returns the API response (list of errors) or empty list on success.
        """
        table_id = self._table_ref(table_name)
        try:
            errors = self.client.insert_rows_json(table_id, rows)
            if errors:
                # Raise an exception so caller can handle/rollback if needed
                raise GoogleAPIError(f"BigQuery insert errors: {errors}")
            return []
        except Exception:
            # Re-raise for the caller; preserve stack trace
            raise

    def query(self, sql: str):
        """Execute a SQL query on BigQuery.
        
        Used for operations like DELETE, UPDATE, MERGE, etc.
        """
        try:
            query_job = self.client.query(sql)
            result = query_job.result()  # Wait for the job to complete
            return result
        except Exception:
            # Re-raise for the caller; preserve stack trace
            raise

    def ensure_table_exists(self, table_name: str, schema: List[bigquery.SchemaField]):
        """Create a table if it doesn't exist.
        
        Args:
            table_name: Name of the table to create
            schema: List of SchemaField objects defining the table structure
        """
        table_id = self._table_ref(table_name)
        try:
            self.client.get_table(table_id)
            # Table exists, do nothing
        except Exception:
            # Table doesn't exist, create it
            table = bigquery.Table(table_id, schema=schema)
            self.client.create_table(table)
            print(f"✅ Tabla {table_name} creada exitosamente")

    def ensure_all_tables(self):
        """Create all required tables for the ETL if they don't exist."""
        from google.cloud.bigquery import SchemaField
        
        # Schema for cliente table
        cliente_schema = [
            SchemaField("id", "INTEGER", mode="REQUIRED"),
            SchemaField("codigo", "STRING"),
            SchemaField("razon_social", "STRING"),
            SchemaField("nombre_fantasia", "STRING"),
            SchemaField("rut", "STRING"),
            SchemaField("giro", "STRING"),
            SchemaField("direccion", "STRING"),
            SchemaField("comuna", "STRING"),
            SchemaField("ciudad", "STRING"),
            SchemaField("email", "STRING"),
            SchemaField("telefono", "STRING"),
            SchemaField("celular", "STRING"),
            SchemaField("estado", "INTEGER"),
            SchemaField("fecha_creacion", "TIMESTAMP"),
            SchemaField("fecha_actualizacion", "TIMESTAMP"),
        ]
        
        # Schema for producto table
        producto_schema = [
            SchemaField("id", "INTEGER", mode="REQUIRED"),
            SchemaField("nombre", "STRING"),
            SchemaField("descripcion", "STRING"),
            SchemaField("clasificacion", "STRING"),
            SchemaField("precio", "FLOAT"),
            SchemaField("precio_costo", "FLOAT"),
            SchemaField("codigo_barra", "STRING"),
            SchemaField("estado", "INTEGER"),
            SchemaField("fecha_creacion", "TIMESTAMP"),
            SchemaField("fecha_actualizacion", "TIMESTAMP"),
        ]
        
        # Schema for documento_venta table
        documento_schema = [
            SchemaField("id", "INTEGER", mode="REQUIRED"),
            SchemaField("numero", "INTEGER"),
            SchemaField("tipo_documento", "STRING"),
            SchemaField("cliente_id", "INTEGER"),
            SchemaField("fecha_emision", "DATE"),
            SchemaField("fecha_vencimiento", "DATE"),
            SchemaField("subtotal", "FLOAT"),
            SchemaField("impuesto", "FLOAT"),
            SchemaField("descuento", "FLOAT"),
            SchemaField("total", "FLOAT"),
            SchemaField("estado", "INTEGER"),
            SchemaField("fecha_creacion", "TIMESTAMP"),
            SchemaField("fecha_actualizacion", "TIMESTAMP"),
        ]
        
        # Schema for detalle_documento table
        detalle_schema = [
            SchemaField("id", "INTEGER", mode="REQUIRED"),
            SchemaField("documento_id", "INTEGER"),
            SchemaField("producto_id", "INTEGER"),
            SchemaField("cantidad", "FLOAT"),
            SchemaField("precio_unitario", "FLOAT"),
            SchemaField("descuento", "FLOAT"),
            SchemaField("subtotal", "FLOAT"),
            SchemaField("fecha_creacion", "TIMESTAMP"),
            SchemaField("fecha_actualizacion", "TIMESTAMP"),
        ]
        
        self.ensure_table_exists("cliente", cliente_schema)
        self.ensure_table_exists("producto", producto_schema)
        self.ensure_table_exists("documento_venta", documento_schema)
        self.ensure_table_exists("detalle_documento", detalle_schema)


def get_bq_writer() -> BigQueryWriter:
    return BigQueryWriter()

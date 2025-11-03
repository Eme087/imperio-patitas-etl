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


def get_bq_writer() -> BigQueryWriter:
    return BigQueryWriter()

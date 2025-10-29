"""
BigQuery Manager for dataset and table operations
Handles auto-creation of datasets and tables with dynamic schema inference
"""
from typing import List, Dict, Any, Optional
from google.cloud import bigquery
from google.api_core import exceptions

from .config import Config
from .logging_config import get_logger

logger = get_logger(__name__)


class BigQueryManager:
    """Manages BigQuery datasets, tables, and data insertion"""

    def __init__(self, config: Config):
        """
        Initialize BigQuery manager

        Args:
            config: Service configuration
        """
        self.config = config
        self.client = bigquery.Client(project=config.project_id)

    def ensure_dataset_exists(self, dataset_id: str) -> None:
        """
        Ensure dataset exists, create if it doesn't

        Args:
            dataset_id: Dataset ID to check/create
        """
        dataset_ref = f"{self.config.project_id}.{dataset_id}"

        try:
            self.client.get_dataset(dataset_ref)
            logger.info(f"Dataset {dataset_ref} already exists")
        except exceptions.NotFound:
            logger.info(f"Creating dataset: {dataset_ref}")

            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "EU"  # UK data should be in EU region
            dataset.description = "Business register data from Companies House"

            dataset = self.client.create_dataset(dataset, timeout=30)
            logger.info(f"Created dataset: {dataset_ref}")
        except Exception as e:
            logger.error(f"Error ensuring dataset exists: {e}", exc_info=True)
            raise

    def infer_schema_from_data(
        self,
        sample_data: Dict[str, Any]
    ) -> List[bigquery.SchemaField]:
        """
        Infer BigQuery schema from sample data

        Args:
            sample_data: Sample data dictionary

        Returns:
            List of BigQuery schema fields
        """
        schema = []

        for key, value in sample_data.items():
            # Normalize field name (lowercase, replace hyphens with underscores)
            field_name = key.lower().replace('-', '_').replace(' ', '_')

            # Infer type
            if value is None:
                field_type = "STRING"
            elif isinstance(value, bool):
                field_type = "BOOLEAN"
            elif isinstance(value, int):
                field_type = "INTEGER"
            elif isinstance(value, float):
                field_type = "FLOAT"
            elif isinstance(value, dict):
                field_type = "JSON"
            elif isinstance(value, list):
                field_type = "JSON"  # Store arrays as JSON
            else:
                field_type = "STRING"

            schema.append(
                bigquery.SchemaField(
                    field_name,
                    field_type,
                    mode="NULLABLE"
                )
            )

        # Add metadata fields
        schema.extend([
            bigquery.SchemaField("ingestion_timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
        ])

        return schema

    def ensure_table_exists(
        self,
        table_ref: str,
        sample_data: Optional[Dict[str, Any]] = None,
        schema: Optional[List[bigquery.SchemaField]] = None
    ) -> None:
        """
        Ensure table exists, create if it doesn't

        Args:
            table_ref: Fully qualified table reference
            sample_data: Sample data to infer schema from
            schema: Explicit schema (if not inferring)
        """
        try:
            self.client.get_table(table_ref)
            logger.info(f"Table {table_ref} already exists")
        except exceptions.NotFound:
            logger.info(f"Creating table: {table_ref}")

            if schema is None and sample_data:
                schema = self.infer_schema_from_data(sample_data)
            elif schema is None:
                raise ValueError(
                    "Either sample_data or schema must be provided to create table"
                )

            table = bigquery.Table(table_ref, schema=schema)

            # Add clustering for better query performance
            if any(field.name == 'company_number' for field in schema):
                table.clustering_fields = ['company_number']

            table = self.client.create_table(table, timeout=30)
            logger.info(
                f"Created table: {table_ref} with {len(schema)} fields"
            )
        except Exception as e:
            logger.error(f"Error ensuring table exists: {e}", exc_info=True)
            raise

    def normalize_data_for_bq(
        self,
        data: Dict[str, Any],
        source: str = "companies_house_api"
    ) -> Dict[str, Any]:
        """
        Normalize data for BigQuery insertion

        Args:
            data: Raw data dictionary
            source: Data source identifier

        Returns:
            Normalized data dictionary
        """
        from datetime import datetime
        import json

        normalized = {}

        for key, value in data.items():
            # Normalize field name
            field_name = key.lower().replace('-', '_').replace(' ', '_')

            # Convert complex types to JSON strings
            if isinstance(value, (dict, list)):
                normalized[field_name] = json.dumps(value)
            elif value is None:
                normalized[field_name] = None
            else:
                normalized[field_name] = value

        # Add metadata
        normalized['ingestion_timestamp'] = datetime.utcnow().isoformat()
        normalized['source'] = source

        return normalized

    def insert_rows(
        self,
        table_ref: str,
        rows: List[Dict[str, Any]],
        create_table_if_missing: bool = True,
        sample_data: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Insert rows into BigQuery table

        Args:
            table_ref: Fully qualified table reference
            rows: List of row dictionaries to insert
            create_table_if_missing: Auto-create table if it doesn't exist
            sample_data: Sample data for schema inference if creating table

        Returns:
            True if successful, False otherwise
        """
        if not rows:
            logger.warning("No rows to insert")
            return True

        try:
            # Ensure dataset exists
            dataset_id = table_ref.split('.')[1]
            self.ensure_dataset_exists(dataset_id)

            # Ensure table exists
            if create_table_if_missing:
                sample = sample_data or rows[0]
                self.ensure_table_exists(table_ref, sample_data=sample)

            # Normalize all rows
            normalized_rows = [
                self.normalize_data_for_bq(row) for row in rows
            ]

            # Insert rows
            errors = self.client.insert_rows_json(
                table_ref,
                normalized_rows,
                retry=bigquery.DEFAULT_RETRY
            )

            if errors:
                logger.error(f"BigQuery insert errors: {errors}")
                return False
            else:
                logger.info(
                    f"Successfully inserted {len(rows)} rows into {table_ref}"
                )
                return True

        except Exception as e:
            logger.error(
                f"Failed to insert rows into {table_ref}: {e}",
                exc_info=True
            )
            return False

    def batch_insert(
        self,
        table_ref: str,
        all_rows: List[Dict[str, Any]],
        batch_size: int = 100
    ) -> tuple[int, int]:
        """
        Insert rows in batches

        Args:
            table_ref: Fully qualified table reference
            all_rows: All rows to insert
            batch_size: Number of rows per batch

        Returns:
            Tuple of (successful_count, failed_count)
        """
        if not all_rows:
            return 0, 0

        logger.info(
            f"Inserting {len(all_rows)} rows in batches of {batch_size}"
        )

        successful = 0
        failed = 0

        # Ensure table exists with first row as sample
        self.ensure_table_exists(table_ref, sample_data=all_rows[0])

        # Process in batches
        for i in range(0, len(all_rows), batch_size):
            batch = all_rows[i:i + batch_size]

            if self.insert_rows(
                table_ref,
                batch,
                create_table_if_missing=False
            ):
                successful += len(batch)
            else:
                failed += len(batch)

            logger.info(
                f"Progress: {successful + failed}/{len(all_rows)} rows processed"
            )

        logger.info(
            f"Batch insert complete. Success: {successful}, Failed: {failed}"
        )

        return successful, failed

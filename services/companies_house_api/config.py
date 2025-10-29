"""
Configuration management for Companies House API Service
"""
import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class Config:
    """Service configuration"""

    # GCP Configuration
    project_id: str = "multi-source-data-pipeline"
    log_project: str = "linknsync"

    # BigQuery Configuration
    dataset_id: str = "business_register"
    profile_table_id: str = "companies_house_profile"
    address_table_id: str = "companies_house_addresses"

    # Secret Manager Configuration
    ch_secret_name: str = "CH-REST-API"
    google_secret_name: str = "GOOGLE_API_KEY"

    # Companies House API Configuration
    ch_base_url: str = "https://api.company-information.service.gov.uk"
    ch_search_endpoint: str = "/search/companies"
    ch_company_endpoint: str = "/company/{company_number}"

    # Search Configuration
    search_postcode: str = "BN6*"
    items_per_page: int = 100
    max_results: Optional[int] = None  # None = fetch all

    # Retry Configuration
    max_retries: int = 3
    retry_backoff_factor: float = 2.0
    request_timeout: int = 30

    # Batch Configuration
    batch_size: int = 100  # For BigQuery inserts

    @classmethod
    def from_env(cls) -> 'Config':
        """Create config from environment variables"""
        config = cls()

        # Override with environment variables if present
        config.project_id = os.environ.get('GCP_PROJECT_ID', config.project_id)
        config.log_project = os.environ.get('LOG_PROJECT', config.log_project)
        config.dataset_id = os.environ.get('DATASET_ID', config.dataset_id)
        config.search_postcode = os.environ.get('SEARCH_POSTCODE', config.search_postcode)

        # Numeric overrides
        if max_results := os.environ.get('MAX_RESULTS'):
            config.max_results = int(max_results)

        if batch_size := os.environ.get('BATCH_SIZE'):
            config.batch_size = int(batch_size)

        return config

    def get_table_ref(self, table_id: str) -> str:
        """Get fully qualified table reference"""
        return f"{self.project_id}.{self.dataset_id}.{table_id}"

    @property
    def profile_table_ref(self) -> str:
        """Get profile table reference"""
        return self.get_table_ref(self.profile_table_id)

    @property
    def address_table_ref(self) -> str:
        """Get address table reference"""
        return self.get_table_ref(self.address_table_id)

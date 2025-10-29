"""
GCP Secret Manager integration for secure credential management
"""
from typing import Dict
from google.cloud import secretmanager
from google.api_core import exceptions

from .logging_config import get_logger

logger = get_logger(__name__)


class SecretsManager:
    """Manages secrets from GCP Secret Manager"""

    def __init__(self, project_id: str):
        """
        Initialize Secret Manager client

        Args:
            project_id: GCP project ID containing the secrets
        """
        self.project_id = project_id
        self.client = secretmanager.SecretManagerServiceClient()
        self._cache: Dict[str, str] = {}

    def get_secret(self, secret_name: str, version: str = "latest") -> str:
        """
        Retrieve a secret from Secret Manager

        Args:
            secret_name: Name of the secret (e.g., 'CH-REST-API')
            version: Secret version (default: 'latest')

        Returns:
            Secret value as string

        Raises:
            Exception: If secret cannot be retrieved
        """
        # Check cache first
        cache_key = f"{secret_name}:{version}"
        if cache_key in self._cache:
            logger.debug(f"Using cached secret: {secret_name}")
            return self._cache[cache_key]

        try:
            # Build the resource name
            name = (
                f"projects/{self.project_id}/secrets/{secret_name}/"
                f"versions/{version}"
            )

            logger.info(f"Retrieving secret: {secret_name}")

            # Access the secret
            response = self.client.access_secret_version(request={"name": name})
            secret_value = response.payload.data.decode('UTF-8')

            # Cache the secret
            self._cache[cache_key] = secret_value

            logger.info(f"Successfully retrieved secret: {secret_name}")
            return secret_value

        except exceptions.NotFound:
            logger.error(f"Secret not found: {secret_name}")
            raise ValueError(
                f"Secret '{secret_name}' not found in project '{self.project_id}'. "
                f"Please ensure the secret exists in GCP Secret Manager."
            )
        except exceptions.PermissionDenied:
            logger.error(f"Permission denied for secret: {secret_name}")
            raise PermissionError(
                f"Permission denied accessing secret '{secret_name}'. "
                f"Ensure the service account has 'secretmanager.secretAccessor' role."
            )
        except Exception as e:
            logger.error(f"Failed to retrieve secret '{secret_name}': {e}", exc_info=True)
            raise

    def get_companies_house_api_key(self, secret_name: str = "CH-REST-API") -> str:
        """
        Get Companies House API key

        Args:
            secret_name: Name of the CH API secret

        Returns:
            API key
        """
        return self.get_secret(secret_name)

    def get_google_api_key(self, secret_name: str = "GOOGLE_API_KEY") -> str:
        """
        Get Google API key

        Args:
            secret_name: Name of the Google API secret

        Returns:
            API key
        """
        return self.get_secret(secret_name)

    def clear_cache(self):
        """Clear the secrets cache"""
        self._cache.clear()
        logger.info("Secrets cache cleared")

"""
Logging configuration for Companies House API Service
Logs to GCP Cloud Logging in the 'linknsync' project
"""
import logging
import sys
from typing import Optional
from google.cloud import logging as cloud_logging


def setup_logging(
    project_id: str = "linknsync",
    service_name: str = "companies-house-api",
    level: int = logging.INFO
) -> logging.Logger:
    """
    Configure structured logging to GCP Cloud Logging

    Args:
        project_id: GCP project for logs (linknsync)
        service_name: Name of the service for log identification
        level: Logging level

    Returns:
        Configured logger instance
    """
    try:
        # Initialize Cloud Logging client for linknsync project
        client = cloud_logging.Client(project=project_id)

        # Setup Cloud Logging handler
        handler = client.get_default_handler()
        cloud_handler = cloud_logging.handlers.CloudLoggingHandler(
            client,
            name=service_name
        )

        # Configure root logger
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[cloud_handler]
        )

        # Also add console handler for local debugging
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)

        # Get logger for this service
        logger = logging.getLogger(service_name)
        logger.addHandler(console_handler)
        logger.setLevel(level)

        logger.info(
            f"Logging initialized for service '{service_name}' "
            f"in project '{project_id}'"
        )

        return logger

    except Exception as e:
        # Fallback to console logging if Cloud Logging fails
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler(sys.stdout)]
        )
        logger = logging.getLogger(service_name)
        logger.warning(
            f"Failed to initialize Cloud Logging: {e}. "
            f"Using console logging instead."
        )
        return logger


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Get a logger instance

    Args:
        name: Logger name (defaults to 'companies-house-api')

    Returns:
        Logger instance
    """
    return logging.getLogger(name or 'companies-house-api')

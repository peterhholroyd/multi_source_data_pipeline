"""
Companies House REST API Client
Handles search, company details, and address retrieval
"""
import time
from typing import List, Dict, Any, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import Config
from .logging_config import get_logger

logger = get_logger(__name__)


class CompaniesHouseClient:
    """Client for Companies House REST API"""

    def __init__(self, api_key: str, config: Config):
        """
        Initialize Companies House API client

        Args:
            api_key: Companies House API key
            config: Service configuration
        """
        self.api_key = api_key
        self.config = config
        self.base_url = config.ch_base_url
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create HTTP session with retry logic"""
        session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=self.config.max_retries,
            backoff_factor=self.config.retry_backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        # Set authentication (API key as username, empty password)
        session.auth = (self.api_key, '')

        return session

    def search_companies(
        self,
        postcode: str,
        items_per_page: int = 100,
        max_results: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Search for companies by postcode

        Args:
            postcode: Postcode pattern to search (e.g., 'BN6*')
            items_per_page: Number of results per page
            max_results: Maximum total results to fetch (None = all)

        Returns:
            List of company search results
        """
        logger.info(f"Searching for companies with postcode: {postcode}")

        url = f"{self.base_url}{self.config.ch_search_endpoint}"
        all_companies = []
        start_index = 0
        total_fetched = 0

        while True:
            try:
                params = {
                    'q': postcode,
                    'items_per_page': items_per_page,
                    'start_index': start_index
                }

                logger.debug(f"Fetching results from index {start_index}")

                response = self.session.get(
                    url,
                    params=params,
                    timeout=self.config.request_timeout
                )
                response.raise_for_status()

                data = response.json()
                items = data.get('items', [])

                if not items:
                    logger.info("No more results to fetch")
                    break

                all_companies.extend(items)
                total_fetched += len(items)

                logger.info(
                    f"Fetched {len(items)} companies "
                    f"(total: {total_fetched})"
                )

                # Check if we've reached max_results
                if max_results and total_fetched >= max_results:
                    all_companies = all_companies[:max_results]
                    logger.info(f"Reached max_results limit: {max_results}")
                    break

                # Check if there are more results
                total_results = data.get('total_results', 0)
                if start_index + items_per_page >= total_results:
                    logger.info("Fetched all available results")
                    break

                start_index += items_per_page

                # Rate limiting - be respectful to the API
                time.sleep(0.5)

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    logger.warning("Search endpoint returned 404")
                    break
                elif e.response.status_code == 401:
                    logger.error("Authentication failed - check API key")
                    raise ValueError("Invalid Companies House API key")
                else:
                    logger.error(f"HTTP error during search: {e}")
                    raise
            except Exception as e:
                logger.error(f"Error searching companies: {e}", exc_info=True)
                raise

        logger.info(f"Search complete. Total companies found: {len(all_companies)}")
        return all_companies

    def get_company_profile(self, company_number: str) -> Dict[str, Any]:
        """
        Get detailed company profile

        Args:
            company_number: Company number (e.g., '12345678')

        Returns:
            Company profile data
        """
        url = f"{self.base_url}/company/{company_number}"

        try:
            logger.debug(f"Fetching company profile: {company_number}")

            response = self.session.get(
                url,
                timeout=self.config.request_timeout
            )
            response.raise_for_status()

            data = response.json()
            logger.debug(f"Successfully retrieved profile for: {company_number}")

            return data

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(f"Company not found: {company_number}")
                return {}
            else:
                logger.error(
                    f"HTTP error fetching profile for {company_number}: {e}"
                )
                raise
        except Exception as e:
            logger.error(
                f"Error fetching profile for {company_number}: {e}",
                exc_info=True
            )
            raise

    def get_registered_office_address(
        self,
        company_number: str
    ) -> Dict[str, Any]:
        """
        Get registered office address for a company

        Args:
            company_number: Company number

        Returns:
            Registered office address data
        """
        url = f"{self.base_url}/company/{company_number}/registered-office-address"

        try:
            logger.debug(
                f"Fetching registered office address: {company_number}"
            )

            response = self.session.get(
                url,
                timeout=self.config.request_timeout
            )
            response.raise_for_status()

            data = response.json()
            logger.debug(
                f"Successfully retrieved address for: {company_number}"
            )

            return data

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(
                    f"Address not found for company: {company_number}"
                )
                return {}
            else:
                logger.error(
                    f"HTTP error fetching address for {company_number}: {e}"
                )
                raise
        except Exception as e:
            logger.error(
                f"Error fetching address for {company_number}: {e}",
                exc_info=True
            )
            raise

    def get_company_data(
        self,
        company_number: str
    ) -> tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Get both company profile and address in one call

        Args:
            company_number: Company number

        Returns:
            Tuple of (profile_data, address_data)
        """
        profile = self.get_company_profile(company_number)
        address = self.get_registered_office_address(company_number)

        return profile, address

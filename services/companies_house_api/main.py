"""
Companies House API Service - Main Orchestrator
Implements 3-step workflow:
1. Search for companies with BN6 postcode
2. Retrieve company profiles and store in BigQuery
3. Retrieve registered office addresses and store in BigQuery
"""
import sys
import time
from typing import List, Dict, Any

from .config import Config
from .logging_config import setup_logging, get_logger
from .secrets_manager import SecretsManager
from .companies_house_client import CompaniesHouseClient
from .bigquery_manager import BigQueryManager


class CompaniesHouseService:
    """Main service orchestrator for Companies House data pipeline"""

    def __init__(self, config: Config):
        """
        Initialize the service

        Args:
            config: Service configuration
        """
        self.config = config
        self.logger = get_logger(__name__)

        # Initialize components
        self.logger.info("Initializing Companies House Service...")

        # Setup secrets manager
        self.secrets = SecretsManager(config.project_id)

        # Get API key from Secret Manager
        self.api_key = self.secrets.get_companies_house_api_key(
            config.ch_secret_name
        )

        # Initialize API client
        self.ch_client = CompaniesHouseClient(self.api_key, config)

        # Initialize BigQuery manager
        self.bq_manager = BigQueryManager(config)

        self.logger.info("Service initialized successfully")

    def step_1_search_companies(self) -> List[str]:
        """
        Step 1: Search for companies with specified postcode

        Returns:
            List of company numbers
        """
        self.logger.info("=" * 70)
        self.logger.info("STEP 1: Searching for companies")
        self.logger.info("=" * 70)

        try:
            search_results = self.ch_client.search_companies(
                postcode=self.config.search_postcode,
                items_per_page=self.config.items_per_page,
                max_results=self.config.max_results
            )

            # Extract company numbers
            company_numbers = [
                company.get('company_number')
                for company in search_results
                if company.get('company_number')
            ]

            self.logger.info(
                f"Step 1 complete: Found {len(company_numbers)} companies "
                f"with postcode {self.config.search_postcode}"
            )

            return company_numbers

        except Exception as e:
            self.logger.error(f"Step 1 failed: {e}", exc_info=True)
            raise

    def step_2_retrieve_and_store_profiles(
        self,
        company_numbers: List[str]
    ) -> tuple[int, int]:
        """
        Step 2: Retrieve company profiles and store in BigQuery

        Args:
            company_numbers: List of company numbers to process

        Returns:
            Tuple of (successful_count, failed_count)
        """
        self.logger.info("=" * 70)
        self.logger.info("STEP 2: Retrieving and storing company profiles")
        self.logger.info("=" * 70)

        if not company_numbers:
            self.logger.warning("No company numbers to process")
            return 0, 0

        profiles = []
        failed = 0

        for idx, company_number in enumerate(company_numbers, 1):
            try:
                self.logger.info(
                    f"Fetching profile {idx}/{len(company_numbers)}: "
                    f"{company_number}"
                )

                profile = self.ch_client.get_company_profile(company_number)

                if profile:
                    profiles.append(profile)
                else:
                    failed += 1
                    self.logger.warning(
                        f"No profile data for company: {company_number}"
                    )

                # Rate limiting
                if idx % 10 == 0:
                    time.sleep(1)

            except Exception as e:
                failed += 1
                self.logger.error(
                    f"Failed to fetch profile for {company_number}: {e}"
                )
                continue

        # Store in BigQuery
        if profiles:
            self.logger.info(
                f"Storing {len(profiles)} profiles in BigQuery..."
            )

            success_count, fail_count = self.bq_manager.batch_insert(
                table_ref=self.config.profile_table_ref,
                all_rows=profiles,
                batch_size=self.config.batch_size
            )

            self.logger.info(
                f"Step 2 complete: {success_count} profiles stored, "
                f"{fail_count + failed} failed"
            )

            return success_count, fail_count + failed
        else:
            self.logger.warning("No profiles to store")
            return 0, failed

    def step_3_retrieve_and_store_addresses(
        self,
        company_numbers: List[str]
    ) -> tuple[int, int]:
        """
        Step 3: Retrieve registered office addresses and store in BigQuery

        Args:
            company_numbers: List of company numbers to process

        Returns:
            Tuple of (successful_count, failed_count)
        """
        self.logger.info("=" * 70)
        self.logger.info("STEP 3: Retrieving and storing registered addresses")
        self.logger.info("=" * 70)

        if not company_numbers:
            self.logger.warning("No company numbers to process")
            return 0, 0

        addresses = []
        failed = 0

        for idx, company_number in enumerate(company_numbers, 1):
            try:
                self.logger.info(
                    f"Fetching address {idx}/{len(company_numbers)}: "
                    f"{company_number}"
                )

                address = self.ch_client.get_registered_office_address(
                    company_number
                )

                if address:
                    # Add company_number to address data for reference
                    address['company_number'] = company_number
                    addresses.append(address)
                else:
                    failed += 1
                    self.logger.warning(
                        f"No address data for company: {company_number}"
                    )

                # Rate limiting
                if idx % 10 == 0:
                    time.sleep(1)

            except Exception as e:
                failed += 1
                self.logger.error(
                    f"Failed to fetch address for {company_number}: {e}"
                )
                continue

        # Store in BigQuery
        if addresses:
            self.logger.info(
                f"Storing {len(addresses)} addresses in BigQuery..."
            )

            success_count, fail_count = self.bq_manager.batch_insert(
                table_ref=self.config.address_table_ref,
                all_rows=addresses,
                batch_size=self.config.batch_size
            )

            self.logger.info(
                f"Step 3 complete: {success_count} addresses stored, "
                f"{fail_count + failed} failed"
            )

            return success_count, fail_count + failed
        else:
            self.logger.warning("No addresses to store")
            return 0, failed

    def run(self) -> Dict[str, Any]:
        """
        Execute the complete 3-step workflow

        Returns:
            Summary statistics
        """
        start_time = time.time()

        self.logger.info("=" * 70)
        self.logger.info("STARTING COMPANIES HOUSE DATA PIPELINE")
        self.logger.info("=" * 70)
        self.logger.info(f"Project ID: {self.config.project_id}")
        self.logger.info(f"Dataset: {self.config.dataset_id}")
        self.logger.info(f"Search Postcode: {self.config.search_postcode}")
        self.logger.info("=" * 70)

        try:
            # Step 1: Search for companies
            company_numbers = self.step_1_search_companies()

            if not company_numbers:
                self.logger.warning("No companies found. Exiting.")
                return {
                    'status': 'success',
                    'companies_found': 0,
                    'profiles_stored': 0,
                    'addresses_stored': 0,
                    'duration_seconds': time.time() - start_time
                }

            # Step 2: Retrieve and store profiles
            profiles_success, profiles_failed = \
                self.step_2_retrieve_and_store_profiles(company_numbers)

            # Step 3: Retrieve and store addresses
            addresses_success, addresses_failed = \
                self.step_3_retrieve_and_store_addresses(company_numbers)

            # Summary
            duration = time.time() - start_time

            self.logger.info("=" * 70)
            self.logger.info("PIPELINE COMPLETE")
            self.logger.info("=" * 70)
            self.logger.info(f"Companies found: {len(company_numbers)}")
            self.logger.info(
                f"Profiles stored: {profiles_success} "
                f"(failed: {profiles_failed})"
            )
            self.logger.info(
                f"Addresses stored: {addresses_success} "
                f"(failed: {addresses_failed})"
            )
            self.logger.info(f"Duration: {duration:.2f} seconds")
            self.logger.info("=" * 70)

            return {
                'status': 'success',
                'companies_found': len(company_numbers),
                'profiles_stored': profiles_success,
                'profiles_failed': profiles_failed,
                'addresses_stored': addresses_success,
                'addresses_failed': addresses_failed,
                'duration_seconds': duration
            }

        except Exception as e:
            self.logger.error(
                f"Pipeline failed: {e}",
                exc_info=True
            )

            return {
                'status': 'failed',
                'error': str(e),
                'duration_seconds': time.time() - start_time
            }


def main():
    """Main entry point"""
    try:
        # Load configuration
        config = Config.from_env()

        # Setup logging to linknsync project
        logger = setup_logging(
            project_id=config.log_project,
            service_name="companies-house-api"
        )

        logger.info("Companies House API Service starting...")

        # Create and run service
        service = CompaniesHouseService(config)
        result = service.run()

        # Exit with appropriate code
        if result['status'] == 'success':
            logger.info("Service completed successfully")
            sys.exit(0)
        else:
            logger.error("Service failed")
            sys.exit(1)

    except Exception as e:
        print(f"Fatal error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()

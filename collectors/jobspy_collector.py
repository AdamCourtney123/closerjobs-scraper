import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging
from jobspy import scrape_jobs
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


class JobSpyCollector:
    """
    Collects jobs from major job boards using the JobSpy library.
    Supports LinkedIn, Indeed, Glassdoor, and ZipRecruiter.
    """

    # Search terms optimized for remote sales closer / appointment setter roles
    SEARCH_TERMS = [
        "remote sales closer",
        "high ticket closer",
        "appointment setter remote",
        "remote closer",
        "sales closer work from home",
        "commission sales closer",
        "high ticket sales remote",
        "remote appointment setter",
        "B2C closer remote",
        "inbound closer remote",
        "remote sales representative commission",
        "closer commission only",
        "setter sales remote",
    ]

    # Map JobSpy site names to our source enum
    SOURCE_MAP = {
        "linkedin": "LINKEDIN",
        "indeed": "INDEED",
        "glassdoor": "GLASSDOOR",
        "zip_recruiter": "ZIPRECRUITER",
    }

    def __init__(self, proxy_url: Optional[str] = None):
        self.proxy_url = proxy_url

    async def collect_jobs(
        self,
        sites: List[str] = ["linkedin", "indeed", "glassdoor", "zip_recruiter"],
        results_wanted: int = 50,
        hours_old: int = 24,
    ) -> List[Dict[str, Any]]:
        """
        Collect jobs from specified sites for all search terms.
        Returns deduplicated list of raw job data.
        """
        all_jobs: List[Dict[str, Any]] = []
        seen_fingerprints: set = set()

        for search_term in self.SEARCH_TERMS:
            try:
                jobs = await self._scrape_with_retry(
                    sites=sites,
                    search_term=search_term,
                    results_wanted=results_wanted,
                    hours_old=hours_old,
                )

                for job in jobs:
                    # Create fingerprint for deduplication
                    fingerprint = self._create_fingerprint(job)
                    if fingerprint not in seen_fingerprints:
                        seen_fingerprints.add(fingerprint)
                        job["_fingerprint"] = fingerprint
                        job["_search_term"] = search_term
                        all_jobs.append(job)

                logger.info(
                    f"Collected {len(jobs)} jobs for '{search_term}' "
                    f"({len(all_jobs)} total unique)"
                )

                # Small delay between searches to avoid rate limiting
                await asyncio.sleep(2)

            except Exception as e:
                logger.error(f"Error collecting jobs for '{search_term}': {e}")
                continue

        logger.info(f"Total unique jobs collected: {len(all_jobs)}")
        return all_jobs

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=30),
    )
    async def _scrape_with_retry(
        self,
        sites: List[str],
        search_term: str,
        results_wanted: int,
        hours_old: int,
    ) -> List[Dict[str, Any]]:
        """
        Scrape jobs with retry logic for resilience.
        Runs in a thread pool since jobspy is synchronous.
        """
        loop = asyncio.get_event_loop()

        def _scrape():
            try:
                # Build proxy config if available
                proxies = None
                if self.proxy_url:
                    proxies = [self.proxy_url]

                # Call JobSpy
                jobs_df = scrape_jobs(
                    site_name=sites,
                    search_term=search_term,
                    location="Remote",
                    results_wanted=results_wanted,
                    hours_old=hours_old,
                    country_indeed="USA",
                    proxies=proxies,
                    is_remote=True,
                )

                if jobs_df is None or jobs_df.empty:
                    return []

                # Convert DataFrame to list of dicts
                jobs = jobs_df.to_dict(orient="records")
                return jobs

            except Exception as e:
                logger.error(f"JobSpy scrape error: {e}")
                raise

        # Run synchronous scraping in thread pool
        return await loop.run_in_executor(None, _scrape)

    def _create_fingerprint(self, job: Dict[str, Any]) -> str:
        """
        Create a unique fingerprint for job deduplication.
        Based on title + company + location.
        """
        import hashlib

        title = str(job.get("title", "")).lower().strip()
        company = str(job.get("company", "")).lower().strip()
        location = str(job.get("location", "")).lower().strip()

        combined = f"{title}|{company}|{location}"
        return hashlib.md5(combined.encode()).hexdigest()

    async def collect_single_source(
        self,
        source: str,
        results_wanted: int = 50,
        hours_old: int = 24,
    ) -> List[Dict[str, Any]]:
        """
        Collect jobs from a single source.
        Useful for targeted scraping or debugging.
        """
        return await self.collect_jobs(
            sites=[source],
            results_wanted=results_wanted,
            hours_old=hours_old,
        )

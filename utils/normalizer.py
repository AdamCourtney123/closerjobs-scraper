import re
from typing import Dict, Any, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class JobNormalizer:
    """
    Normalizes raw job data from various sources to a standard schema.
    Handles extraction of salary, commission info, and job type classification.
    """

    # Map JobSpy site names to our source enum
    SOURCE_MAP = {
        "linkedin": "LINKEDIN",
        "indeed": "INDEED",
        "glassdoor": "GLASSDOOR",
        "zip_recruiter": "ZIPRECRUITER",
        "google": "DIRECT",
    }

    # Keywords for job type classification
    COMMISSION_ONLY_KEYWORDS = [
        "commission only",
        "commission-only",
        "100% commission",
        "straight commission",
        "no base",
        "commission based only",
    ]

    BASE_PLUS_COMMISSION_KEYWORDS = [
        "base plus commission",
        "base + commission",
        "salary plus commission",
        "salary + commission",
        "base salary",
        "guaranteed base",
        "draw against",
        "ote",
        "on target earnings",
    ]

    def normalize(self, raw_job: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize a raw job from JobSpy to our standard schema.
        """
        # Extract and clean basic fields
        title = self._clean_text(raw_job.get("title", ""))
        company = self._clean_text(raw_job.get("company", "Unknown Company"))
        description = self._clean_text(raw_job.get("description", ""))
        location = self._normalize_location(raw_job.get("location", "Remote"))

        # Determine source
        site = raw_job.get("site", "").lower()
        source = self.SOURCE_MAP.get(site, "DIRECT")

        # Extract salary information
        salary_min, salary_max = self._extract_salary(raw_job)

        # Extract commission information from description
        commission_info = self._extract_commission_info(description)

        # Determine job type
        job_type = self._determine_job_type(raw_job, description, salary_min)

        # Parse posted date
        posted_at = self._parse_date(raw_job.get("date_posted"))

        # Build normalized job
        normalized = {
            "title": title,
            "company_name": company,
            "description": description,
            "requirements": self._extract_requirements(description),
            "location": location,
            "source": source,
            "source_url": raw_job.get("job_url"),
            "application_url": raw_job.get("job_url"),
            "fingerprint": raw_job.get("_fingerprint", self._create_fingerprint(title, company, location)),
            "job_type": job_type,
            "salary_min": salary_min,
            "salary_max": salary_max,
            "commission_info": commission_info,
            "posted_at": posted_at,
        }

        return normalized

    def _clean_text(self, text: str) -> str:
        """Clean and normalize text content"""
        if not text:
            return ""

        # Remove excessive whitespace
        text = re.sub(r"\s+", " ", str(text))

        # Remove HTML tags if present
        text = re.sub(r"<[^>]+>", "", text)

        return text.strip()

    def _normalize_location(self, location: str) -> str:
        """Normalize location to standard format"""
        if not location:
            return "Remote"

        location = location.strip()

        # Check for remote indicators
        remote_patterns = [
            r"\bremote\b",
            r"\bwork from home\b",
            r"\bwfh\b",
            r"\banywhere\b",
        ]

        for pattern in remote_patterns:
            if re.search(pattern, location, re.IGNORECASE):
                # Extract any location qualifier
                if "us" in location.lower() or "usa" in location.lower():
                    return "Remote - US"
                if "worldwide" in location.lower() or "global" in location.lower():
                    return "Remote - Worldwide"
                return "Remote"

        return location

    def _extract_salary(self, raw_job: Dict[str, Any]) -> tuple[Optional[int], Optional[int]]:
        """Extract salary range from job data"""
        salary_min = None
        salary_max = None

        # Try to get from explicit fields
        if raw_job.get("min_amount"):
            try:
                salary_min = int(float(raw_job["min_amount"]))
            except (ValueError, TypeError):
                pass

        if raw_job.get("max_amount"):
            try:
                salary_max = int(float(raw_job["max_amount"]))
            except (ValueError, TypeError):
                pass

        # If no explicit salary, try to parse from description
        if not salary_min and not salary_max:
            description = raw_job.get("description", "")
            salary_min, salary_max = self._parse_salary_from_text(description)

        # Normalize to yearly if needed
        interval = raw_job.get("interval", "").lower()
        if interval == "hourly" and salary_min:
            salary_min = salary_min * 2080  # 40 hrs * 52 weeks
            if salary_max:
                salary_max = salary_max * 2080
        elif interval == "monthly" and salary_min:
            salary_min = salary_min * 12
            if salary_max:
                salary_max = salary_max * 12

        return salary_min, salary_max

    def _parse_salary_from_text(self, text: str) -> tuple[Optional[int], Optional[int]]:
        """Parse salary range from text content"""
        # Patterns for salary ranges
        patterns = [
            # $50,000 - $100,000
            r"\$\s*(\d{1,3}(?:,\d{3})*)\s*(?:-|to)\s*\$?\s*(\d{1,3}(?:,\d{3})*)",
            # $50K - $100K
            r"\$\s*(\d+)\s*[kK]\s*(?:-|to)\s*\$?\s*(\d+)\s*[kK]",
            # 50,000 - 100,000 per year
            r"(\d{1,3}(?:,\d{3})*)\s*(?:-|to)\s*(\d{1,3}(?:,\d{3})*)\s*(?:per year|annually|/year)",
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    min_val = match.group(1).replace(",", "")
                    max_val = match.group(2).replace(",", "")

                    salary_min = int(min_val)
                    salary_max = int(max_val)

                    # If values look like K notation
                    if salary_min < 1000:
                        salary_min *= 1000
                    if salary_max < 1000:
                        salary_max *= 1000

                    return salary_min, salary_max
                except (ValueError, IndexError):
                    continue

        return None, None

    def _extract_commission_info(self, description: str) -> Optional[str]:
        """Extract commission-related information from description"""
        commission_patterns = [
            r"(\d+(?:\.\d+)?%?\s*(?:-|to)\s*\d+(?:\.\d+)?%?\s*commission)",
            r"(commission\s*(?:of\s*)?\d+(?:\.\d+)?%)",
            r"(\d+(?:\.\d+)?%\s*(?:commission|per sale|per close))",
            r"(earn\s*\$[\d,]+\s*(?:-|to)\s*\$[\d,]+\s*per\s*(?:deal|sale|close|month))",
            r"(OTE\s*\$[\d,]+(?:k|K)?(?:\s*-\s*\$[\d,]+(?:k|K)?)?)",
            r"(uncapped\s*commission)",
        ]

        for pattern in commission_patterns:
            match = re.search(pattern, description, re.IGNORECASE)
            if match:
                return match.group(1).strip()

        return None

    def _determine_job_type(
        self,
        raw_job: Dict[str, Any],
        description: str,
        salary_min: Optional[int]
    ) -> str:
        """Determine the job type based on available information"""
        description_lower = description.lower()

        # Check for commission only keywords
        for keyword in self.COMMISSION_ONLY_KEYWORDS:
            if keyword in description_lower:
                return "COMMISSION_ONLY"

        # Check for base plus commission keywords
        for keyword in self.BASE_PLUS_COMMISSION_KEYWORDS:
            if keyword in description_lower:
                return "BASE_PLUS_COMMISSION"

        # Check job type field from source
        job_type = raw_job.get("job_type", "").lower()
        if "contract" in job_type:
            return "CONTRACT"
        if "part" in job_type:
            return "PART_TIME"

        # If salary is provided, likely has base
        if salary_min and salary_min > 20000:
            return "BASE_PLUS_COMMISSION"

        # Default to full time
        return "FULL_TIME"

    def _extract_requirements(self, description: str) -> Optional[str]:
        """Extract requirements section from description"""
        # Common section headers
        headers = [
            r"requirements?:",
            r"qualifications?:",
            r"what we'?re looking for:",
            r"you have:",
            r"must have:",
            r"required:",
        ]

        for header in headers:
            pattern = rf"({header}.*?)(?=\n\n|\Z|responsibilities|about us|benefits|what we offer)"
            match = re.search(pattern, description, re.IGNORECASE | re.DOTALL)
            if match:
                return match.group(1).strip()

        return None

    def _parse_date(self, date_str: Any) -> Optional[datetime]:
        """Parse date from various formats"""
        if not date_str:
            return None

        if isinstance(date_str, datetime):
            return date_str

        try:
            # Try common formats
            formats = [
                "%Y-%m-%d",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M:%SZ",
                "%m/%d/%Y",
            ]

            for fmt in formats:
                try:
                    return datetime.strptime(str(date_str), fmt)
                except ValueError:
                    continue

            return None
        except Exception:
            return None

    def _create_fingerprint(self, title: str, company: str, location: str) -> str:
        """Create fingerprint for deduplication"""
        import hashlib

        combined = f"{title.lower().strip()}|{company.lower().strip()}|{location.lower().strip()}"
        return hashlib.md5(combined.encode()).hexdigest()

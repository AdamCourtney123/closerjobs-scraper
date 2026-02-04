from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    # Database
    database_url: str = "postgresql://localhost:5432/closerjobs"

    # Redis (optional)
    redis_url: Optional[str] = None

    # Proxy
    proxy_url: Optional[str] = None

    # API Security
    api_key: str = "dev-api-key"

    # Scraping Configuration
    scrape_results_per_search: int = 50
    scrape_hours_old: int = 24

    # Rate Limiting (requests per minute per source)
    rate_limit_linkedin: int = 5
    rate_limit_indeed: int = 10
    rate_limit_glassdoor: int = 8
    rate_limit_ziprecruiter: int = 10

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()

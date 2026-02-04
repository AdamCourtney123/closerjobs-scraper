from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
import os
import hashlib

app = FastAPI(
    title="CloserJobs Scraper API",
    description="Job scraping service for remote sales closer positions",
    version="1.0.0",
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

API_KEY = os.getenv("API_KEY", "closerjobs-scraper-key-2024")
DATABASE_URL = os.getenv("DATABASE_URL")


# Auth dependency
async def verify_api_key(x_api_key: str = Header(None)):
    if x_api_key and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return x_api_key


class JobResult(BaseModel):
    title: str
    company: Optional[str] = None
    location: Optional[str] = None
    description: Optional[str] = None
    url: Optional[str] = None
    source: str
    posted_date: Optional[str] = None


class ScrapeResponse(BaseModel):
    success: bool
    jobs: List[JobResult]
    count: int
    errors: List[str]


class SyncResponse(BaseModel):
    success: bool
    jobs_found: int
    jobs_added: int
    jobs_updated: int
    errors: List[str]


def generate_fingerprint(title: str, company: str, url: str) -> str:
    data = f"{title}-{company or ''}-{url or ''}".lower()
    return hashlib.md5(data.encode()).hexdigest()


def map_source(source: str) -> str:
    source_map = {
        "indeed": "INDEED",
        "linkedin": "LINKEDIN",
        "glassdoor": "GLASSDOOR",
        "zip_recruiter": "ZIPRECRUITER",
        "ziprecruiter": "ZIPRECRUITER",
    }
    return source_map.get(source.lower(), "MANUAL")


def infer_job_type(title: str, description: str) -> str:
    text = f"{title} {description or ''}".lower()
    if "commission only" in text or "100% commission" in text:
        return "COMMISSION_ONLY"
    if "base plus commission" in text or "base + commission" in text or "base salary" in text:
        return "BASE_PLUS_COMMISSION"
    if "contract" in text:
        return "CONTRACT"
    if "part-time" in text or "part time" in text:
        return "PART_TIME"
    return "FULL_TIME"


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "version": "1.0.0"}


@app.get("/")
async def root():
    """Root endpoint"""
    return {"message": "CloserJobs Scraper API", "docs": "/docs"}


@app.post("/collect/jobspy", response_model=ScrapeResponse)
async def collect_from_jobspy(
    sources: List[str] = ["indeed", "linkedin", "glassdoor", "zip_recruiter"],
    results_wanted: int = 25,
    hours_old: int = 72,
):
    """
    Collect jobs from job boards using JobSpy.
    """
    errors = []
    jobs = []

    try:
        from jobspy import scrape_jobs

        search_terms = [
            "sales closer remote",
            "high ticket closer",
            "appointment setter remote",
            "remote sales representative",
        ]

        for term in search_terms[:2]:  # Limit to 2 searches to be faster
            try:
                results = scrape_jobs(
                    site_name=sources,
                    search_term=term,
                    location="remote",
                    results_wanted=results_wanted,
                    hours_old=hours_old,
                    country_indeed="USA",
                )

                if results is not None and len(results) > 0:
                    for _, row in results.iterrows():
                        jobs.append(JobResult(
                            title=str(row.get("title", "")),
                            company=str(row.get("company", "")) if row.get("company") else None,
                            location=str(row.get("location", "Remote")),
                            description=str(row.get("description", ""))[:2000] if row.get("description") else None,
                            url=str(row.get("job_url", "")) if row.get("job_url") else None,
                            source=str(row.get("site", "unknown")),
                            posted_date=str(row.get("date_posted", "")) if row.get("date_posted") else None,
                        ))
            except Exception as e:
                errors.append(f"Error searching '{term}': {str(e)}")

    except ImportError as e:
        errors.append(f"JobSpy not available: {str(e)}")
    except Exception as e:
        errors.append(f"Scraping error: {str(e)}")

    # Remove duplicates by URL
    seen_urls = set()
    unique_jobs = []
    for job in jobs:
        if job.url and job.url not in seen_urls:
            seen_urls.add(job.url)
            unique_jobs.append(job)
        elif not job.url:
            unique_jobs.append(job)

    return ScrapeResponse(
        success=len(unique_jobs) > 0,
        jobs=unique_jobs,
        count=len(unique_jobs),
        errors=errors,
    )


@app.post("/sync", response_model=SyncResponse)
async def sync_jobs(
    results_wanted: int = 25,
    hours_old: int = 72,
):
    """
    Scrape jobs and save directly to database.
    """
    import psycopg2
    from psycopg2.extras import execute_values

    errors = []
    jobs_found = 0
    jobs_added = 0
    jobs_updated = 0

    if not DATABASE_URL:
        return SyncResponse(
            success=False,
            jobs_found=0,
            jobs_added=0,
            jobs_updated=0,
            errors=["DATABASE_URL not configured"],
        )

    # First, scrape jobs
    scrape_result = await collect_from_jobspy(
        sources=["indeed", "linkedin"],
        results_wanted=results_wanted,
        hours_old=hours_old,
    )

    jobs_found = scrape_result.count
    errors.extend(scrape_result.errors)

    if not scrape_result.jobs:
        return SyncResponse(
            success=False,
            jobs_found=0,
            jobs_added=0,
            jobs_updated=0,
            errors=errors or ["No jobs found"],
        )

    # Connect to database and save jobs
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        for job in scrape_result.jobs:
            try:
                fingerprint = generate_fingerprint(job.title, job.company, job.url)
                source = map_source(job.source)
                job_type = infer_job_type(job.title, job.description)

                # Check if exists
                cur.execute("SELECT id FROM jobs WHERE fingerprint = %s", (fingerprint,))
                existing = cur.fetchone()

                if existing:
                    # Update last_seen_at
                    cur.execute(
                        "UPDATE jobs SET last_seen_at = NOW() WHERE id = %s",
                        (existing[0],)
                    )
                    jobs_updated += 1
                else:
                    # Insert new job
                    cur.execute("""
                        INSERT INTO jobs (
                            id, title, company_name, description, location,
                            source, source_url, fingerprint, job_type, status,
                            posted_at, last_seen_at, ote_based, created_at, updated_at
                        ) VALUES (
                            gen_random_uuid(), %s, %s, %s, %s,
                            %s, %s, %s, %s, 'ACTIVE',
                            %s, NOW(), %s, NOW(), NOW()
                        )
                    """, (
                        job.title,
                        job.company or "Unknown Company",
                        job.description or "No description available",
                        job.location or "Remote",
                        source,
                        job.url,
                        fingerprint,
                        job_type,
                        job.posted_date if job.posted_date and job.posted_date != "None" else None,
                        "ote" in (job.description or "").lower(),
                    ))
                    jobs_added += 1

            except Exception as e:
                errors.append(f"Error saving job '{job.title}': {str(e)}")

        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        errors.append(f"Database error: {str(e)}")
        return SyncResponse(
            success=False,
            jobs_found=jobs_found,
            jobs_added=jobs_added,
            jobs_updated=jobs_updated,
            errors=errors,
        )

    return SyncResponse(
        success=jobs_added > 0 or jobs_updated > 0,
        jobs_found=jobs_found,
        jobs_added=jobs_added,
        jobs_updated=jobs_updated,
        errors=errors,
    )


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)

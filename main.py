from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
import os

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


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)

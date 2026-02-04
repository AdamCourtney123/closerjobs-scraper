from fastapi import FastAPI, HTTPException, Depends, Header, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
import asyncio

from config import settings
from collectors.jobspy_collector import JobSpyCollector
from utils.database import Database, init_db
from utils.normalizer import JobNormalizer

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

# Initialize components
db = Database(settings.database_url)
collector = JobSpyCollector(proxy_url=settings.proxy_url)
normalizer = JobNormalizer()


# Auth dependency
async def verify_api_key(x_api_key: str = Header(...)):
    if x_api_key != settings.api_key:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return x_api_key


# Request/Response models
class ScrapeRequest(BaseModel):
    sources: List[str] = ["linkedin", "indeed", "glassdoor", "zip_recruiter"]
    hours_old: int = 24
    results_per_search: int = 50


class ScrapeResponse(BaseModel):
    success: bool
    run_id: str
    jobs_found: int
    jobs_added: int
    jobs_updated: int
    errors: List[str]
    duration_seconds: float


class HealthResponse(BaseModel):
    status: str
    version: str
    database_connected: bool


# Endpoints
@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    db_connected = await db.check_connection()
    return HealthResponse(
        status="healthy" if db_connected else "degraded",
        version="1.0.0",
        database_connected=db_connected,
    )


@app.post("/collect/jobspy", response_model=ScrapeResponse, dependencies=[Depends(verify_api_key)])
async def collect_from_jobspy(request: ScrapeRequest, background_tasks: BackgroundTasks):
    """
    Trigger job collection from major job boards via JobSpy.
    This runs synchronously and returns results when complete.
    """
    start_time = datetime.now()
    errors: List[str] = []

    # Create scraping run record
    run_id = await db.create_scraping_run(source="JOBSPY")

    try:
        # Collect jobs from all sources
        raw_jobs = await collector.collect_jobs(
            sites=request.sources,
            results_wanted=request.results_per_search,
            hours_old=request.hours_old,
        )

        # Normalize jobs
        normalized_jobs = []
        for job in raw_jobs:
            try:
                normalized = normalizer.normalize(job)
                normalized_jobs.append(normalized)
            except Exception as e:
                errors.append(f"Normalization error: {str(e)}")

        # Save to database
        jobs_added, jobs_updated = await db.upsert_jobs(normalized_jobs)

        # Update scraping run
        duration = (datetime.now() - start_time).total_seconds()
        await db.complete_scraping_run(
            run_id=run_id,
            status="COMPLETED",
            jobs_found=len(raw_jobs),
            jobs_added=jobs_added,
            jobs_updated=jobs_updated,
            errors=errors,
            duration=int(duration),
        )

        return ScrapeResponse(
            success=True,
            run_id=run_id,
            jobs_found=len(raw_jobs),
            jobs_added=jobs_added,
            jobs_updated=jobs_updated,
            errors=errors,
            duration_seconds=duration,
        )

    except Exception as e:
        await db.complete_scraping_run(
            run_id=run_id,
            status="FAILED",
            jobs_found=0,
            jobs_added=0,
            jobs_updated=0,
            errors=[str(e)],
            duration=int((datetime.now() - start_time).total_seconds()),
        )
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/collect/jobspy/async", dependencies=[Depends(verify_api_key)])
async def collect_from_jobspy_async(request: ScrapeRequest, background_tasks: BackgroundTasks):
    """
    Trigger job collection in the background.
    Returns immediately with a run_id to check status later.
    """
    run_id = await db.create_scraping_run(source="JOBSPY")

    async def run_collection():
        start_time = datetime.now()
        errors: List[str] = []

        try:
            raw_jobs = await collector.collect_jobs(
                sites=request.sources,
                results_wanted=request.results_per_search,
                hours_old=request.hours_old,
            )

            normalized_jobs = []
            for job in raw_jobs:
                try:
                    normalized = normalizer.normalize(job)
                    normalized_jobs.append(normalized)
                except Exception as e:
                    errors.append(f"Normalization error: {str(e)}")

            jobs_added, jobs_updated = await db.upsert_jobs(normalized_jobs)

            await db.complete_scraping_run(
                run_id=run_id,
                status="COMPLETED",
                jobs_found=len(raw_jobs),
                jobs_added=jobs_added,
                jobs_updated=jobs_updated,
                errors=errors,
                duration=int((datetime.now() - start_time).total_seconds()),
            )
        except Exception as e:
            await db.complete_scraping_run(
                run_id=run_id,
                status="FAILED",
                jobs_found=0,
                jobs_added=0,
                jobs_updated=0,
                errors=[str(e)],
                duration=int((datetime.now() - start_time).total_seconds()),
            )

    background_tasks.add_task(asyncio.create_task, run_collection())

    return {"success": True, "run_id": run_id, "status": "RUNNING"}


@app.get("/runs/{run_id}", dependencies=[Depends(verify_api_key)])
async def get_scraping_run(run_id: str):
    """Get the status of a scraping run"""
    run = await db.get_scraping_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    return run


@app.get("/stats", dependencies=[Depends(verify_api_key)])
async def get_stats():
    """Get scraping statistics"""
    return await db.get_stats()


@app.on_event("startup")
async def startup():
    await db.connect()


@app.on_event("shutdown")
async def shutdown():
    await db.disconnect()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)

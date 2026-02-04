import asyncpg
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import json
import logging
import uuid

logger = logging.getLogger(__name__)


class Database:
    """
    Async database client for the scraper service.
    Handles job storage and scraping run tracking.
    """

    def __init__(self, database_url: str):
        self.database_url = database_url
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        """Establish database connection pool"""
        try:
            self.pool = await asyncpg.create_pool(
                self.database_url,
                min_size=2,
                max_size=10,
                command_timeout=60,
            )
            logger.info("Database connection pool established")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    async def disconnect(self):
        """Close database connection pool"""
        if self.pool:
            await self.pool.close()
            logger.info("Database connection pool closed")

    async def check_connection(self) -> bool:
        """Check if database is reachable"""
        try:
            if not self.pool:
                return False
            async with self.pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            return True
        except Exception:
            return False

    async def create_scraping_run(self, source: str) -> str:
        """Create a new scraping run record"""
        run_id = str(uuid.uuid4())[:8]

        query = """
            INSERT INTO scraping_runs (id, source, status, started_at, jobs_found, jobs_added, jobs_updated)
            VALUES ($1, $2, 'RUNNING', $3, 0, 0, 0)
        """

        async with self.pool.acquire() as conn:
            await conn.execute(query, run_id, source, datetime.utcnow())

        return run_id

    async def complete_scraping_run(
        self,
        run_id: str,
        status: str,
        jobs_found: int,
        jobs_added: int,
        jobs_updated: int,
        errors: List[str],
        duration: int,
    ):
        """Update scraping run with completion status"""
        query = """
            UPDATE scraping_runs
            SET status = $2,
                jobs_found = $3,
                jobs_added = $4,
                jobs_updated = $5,
                errors = $6,
                completed_at = $7,
                duration = $8
            WHERE id = $1
        """

        async with self.pool.acquire() as conn:
            await conn.execute(
                query,
                run_id,
                status,
                jobs_found,
                jobs_added,
                jobs_updated,
                json.dumps(errors),
                datetime.utcnow(),
                duration,
            )

    async def get_scraping_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Get scraping run by ID"""
        query = """
            SELECT id, source, status, started_at, completed_at,
                   jobs_found, jobs_added, jobs_updated, errors, duration
            FROM scraping_runs
            WHERE id = $1
        """

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, run_id)
            if row:
                return dict(row)
            return None

    async def upsert_jobs(self, jobs: List[Dict[str, Any]]) -> Tuple[int, int]:
        """
        Insert or update jobs in the database.
        Returns (jobs_added, jobs_updated) counts.
        """
        if not jobs:
            return 0, 0

        jobs_added = 0
        jobs_updated = 0

        async with self.pool.acquire() as conn:
            for job in jobs:
                try:
                    # Check if job exists by fingerprint
                    existing = await conn.fetchval(
                        "SELECT id FROM jobs WHERE fingerprint = $1",
                        job["fingerprint"],
                    )

                    if existing:
                        # Update existing job
                        await conn.execute(
                            """
                            UPDATE jobs
                            SET title = $2,
                                description = $3,
                                requirements = $4,
                                location = $5,
                                job_type = $6,
                                salary_min = $7,
                                salary_max = $8,
                                commission_info = $9,
                                application_url = $10,
                                source_url = $11,
                                last_seen_at = $12,
                                updated_at = $12
                            WHERE fingerprint = $1
                            """,
                            job["fingerprint"],
                            job["title"],
                            job["description"],
                            job.get("requirements"),
                            job["location"],
                            job["job_type"],
                            job.get("salary_min"),
                            job.get("salary_max"),
                            job.get("commission_info"),
                            job.get("application_url"),
                            job.get("source_url"),
                            datetime.utcnow(),
                        )
                        jobs_updated += 1
                    else:
                        # Insert new job
                        job_id = str(uuid.uuid4())[:25]
                        await conn.execute(
                            """
                            INSERT INTO jobs (
                                id, company_name, title, description, requirements,
                                location, job_type, salary_min, salary_max, commission_info,
                                application_url, application_email, source, source_url,
                                fingerprint, status, posted_at, last_seen_at, created_at, updated_at
                            ) VALUES (
                                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                                $11, $12, $13, $14, $15, 'ACTIVE', $16, $17, $17, $17
                            )
                            """,
                            job_id,
                            job["company_name"],
                            job["title"],
                            job["description"],
                            job.get("requirements"),
                            job["location"],
                            job["job_type"],
                            job.get("salary_min"),
                            job.get("salary_max"),
                            job.get("commission_info"),
                            job.get("application_url"),
                            job.get("application_email"),
                            job["source"],
                            job.get("source_url"),
                            job["fingerprint"],
                            job.get("posted_at", datetime.utcnow()),
                            datetime.utcnow(),
                        )
                        jobs_added += 1

                except Exception as e:
                    logger.error(f"Error upserting job {job.get('title')}: {e}")
                    continue

        logger.info(f"Upserted jobs: {jobs_added} added, {jobs_updated} updated")
        return jobs_added, jobs_updated

    async def get_stats(self) -> Dict[str, Any]:
        """Get scraping statistics"""
        async with self.pool.acquire() as conn:
            # Total jobs
            total_jobs = await conn.fetchval(
                "SELECT COUNT(*) FROM jobs WHERE status = 'ACTIVE'"
            )

            # Jobs by source
            jobs_by_source = await conn.fetch(
                """
                SELECT source, COUNT(*) as count
                FROM jobs
                WHERE status = 'ACTIVE'
                GROUP BY source
                """
            )

            # Recent runs
            recent_runs = await conn.fetch(
                """
                SELECT id, source, status, jobs_found, jobs_added, started_at, duration
                FROM scraping_runs
                ORDER BY started_at DESC
                LIMIT 10
                """
            )

            # Jobs added today
            jobs_today = await conn.fetchval(
                """
                SELECT COUNT(*) FROM jobs
                WHERE created_at >= CURRENT_DATE
                """
            )

            return {
                "total_active_jobs": total_jobs,
                "jobs_added_today": jobs_today,
                "jobs_by_source": {row["source"]: row["count"] for row in jobs_by_source},
                "recent_runs": [dict(row) for row in recent_runs],
            }

    async def mark_stale_jobs(self, days_old: int = 30):
        """Mark jobs not seen in X days as expired"""
        query = """
            UPDATE jobs
            SET status = 'EXPIRED'
            WHERE last_seen_at < NOW() - INTERVAL '%s days'
            AND status = 'ACTIVE'
        """

        async with self.pool.acquire() as conn:
            result = await conn.execute(query, days_old)
            logger.info(f"Marked stale jobs as expired: {result}")

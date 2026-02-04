# CloserJobs Scraper Service

Python-based job scraping service that collects remote sales closer and appointment setter positions from major job boards.

## Features

- **Multi-source scraping**: LinkedIn, Indeed, Glassdoor, ZipRecruiter via JobSpy
- **Smart deduplication**: Fingerprint-based matching to avoid duplicate listings
- **Job normalization**: Extracts salary, commission info, and job type from raw data
- **Rate limiting**: Built-in delays and retry logic to avoid bans
- **Proxy support**: Optional residential proxy rotation for production

## Quick Start

### Local Development

1. **Install Python dependencies**:
   ```bash
   cd scraper
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Configure environment**:
   ```bash
   cp .env.example .env
   # Edit .env with your DATABASE_URL
   ```

3. **Run the service**:
   ```bash
   uvicorn main:app --reload --port 8080
   ```

4. **Test the API**:
   ```bash
   # Health check
   curl http://localhost:8080/health

   # Trigger a scrape (requires API key)
   curl -X POST http://localhost:8080/collect/jobspy \
     -H "Content-Type: application/json" \
     -H "X-API-Key: dev-api-key" \
     -d '{"sources": ["indeed"], "hours_old": 24, "results_per_search": 10}'
   ```

### Docker Deployment

```bash
# Build image
docker build -t closerjobs-scraper .

# Run container
docker run -p 8080:8080 \
  -e DATABASE_URL="postgresql://..." \
  -e API_KEY="your-secret-key" \
  closerjobs-scraper
```

### Deploy to Railway

1. Create a new project on [Railway](https://railway.app)
2. Connect your GitHub repo
3. Set the root directory to `/scraper`
4. Add environment variables:
   - `DATABASE_URL`
   - `API_KEY`
   - `PROXY_URL` (optional)

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/collect/jobspy` | POST | Trigger synchronous job collection |
| `/collect/jobspy/async` | POST | Trigger async job collection |
| `/runs/{run_id}` | GET | Get scraping run status |
| `/stats` | GET | Get scraping statistics |

### Request Body for `/collect/jobspy`

```json
{
  "sources": ["linkedin", "indeed", "glassdoor", "zip_recruiter"],
  "hours_old": 24,
  "results_per_search": 50
}
```

## Search Terms

The scraper searches for these terms to find relevant roles:
- remote sales closer
- high ticket closer
- appointment setter remote
- remote closer
- sales closer work from home
- commission sales closer
- And more...

## Rate Limits

To avoid being blocked, the scraper:
- Adds 2-second delays between searches
- Uses exponential backoff on failures
- Supports proxy rotation (recommended for production)

## Production Tips

1. **Use residential proxies**: Services like Bright Data or Smartproxy
2. **Run during off-peak hours**: Less likely to hit rate limits
3. **Monitor error rates**: Set up alerting for failed scrapes
4. **Cache results**: Don't re-scrape the same jobs too frequently

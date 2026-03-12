# Scraper API (Hotline, Comfy, Brain)

Async service for scraping Hotline offers and Comfy/Brain product reviews with storage in MongoDB.

## Stack
- Python 3.11+
- FastAPI (async) + Pydantic v2
- Playwright (Chromium)
- MongoDB (motor)
- Docker / docker-compose

## Quick start (Docker)

1. Run:
```bash
docker compose up --build
```
2. Open Swagger:
```
http://localhost:8000/docs
```

## Environment variables
- `MONGO_URI` — MongoDB connection string (default in docker-compose)
- `PLAYWRIGHT_HEADLESS` — `true|false` (default `true`)
- `COMMENTS_DEBUG` — `1` to dump debug HTML/JSON to `/tmp/comments-debug` inside the container

## API

### 1) Hotline offers
```
GET /product/offers
```
Parameters:
- `url` (required) — product page URL
- `timeout_limit` (optional, sec)
- `price_sort` (optional: `asc|desc`)
- `count_limit` (optional)

Example:
```bash
curl -X GET "http://localhost:8000/product/offers?url=https://hotline.ua/bt-vyazalnye-mashiny/silver-reed-sk840srp60n&timeout_limit=5&count_limit=10&price_sort=asc"
```

### 2) Product comments (Comfy / Brain)
```
GET /product/comments
```
Parameters:
- `url` (required)
- `date_to` (optional, format `YYYY-MM-DD`) — parse comments up to this date (inclusive)

Examples:
```bash
curl -X GET "http://localhost:8000/product/comments?url=https://comfy.ua/ua/product/...&date_to=2024-02-08"

curl -X GET "http://localhost:8000/product/comments?url=https://brain.com.ua/product/..."
```

## MongoDB
Data is stored in:
- `offers` — Hotline offers
- `comments` — Comfy/Brain reviews

## Code structure
- `app/api.py` — routers (controllers)
- `app/services/*` — service layer
- `app/repositories/*` — database access
- `app/hotline.py` — Hotline scraping
- `app/comments.py` — Comfy/Brain scraping
- `app/models.py` — internal and API models
- `app/db.py` — MongoDB init

## Notes
- All network and DB operations are async.
- If rating/date is missing in the source, API returns `null`.

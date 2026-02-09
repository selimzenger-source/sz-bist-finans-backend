# BIST Finans Backend API

FastAPI backend for BIST Finans mobile app. Scrapes IPO data from halkarz.com.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run development server
uvicorn main:app --reload --port 8000
```

## API Endpoints

- `GET /api/v1/ipos/sections` - IPO sections (announced, in_subscription, recently_trading)
- `GET /api/v1/ipos` - All IPOs
- `GET /api/v1/ipos/{id}` - IPO detail
- `GET /api/v1/news/latest` - Latest news

## Deploy to Render

1. Push to GitHub
2. Connect repo to Render
3. Set start command: `uvicorn main:app --host 0.0.0.0 --port $PORT`

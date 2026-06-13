# Real-Time Stock Data Pipeline

Pulls live stock prices from Alpha Vantage every 60 seconds, processes them through a Redis queue, persists to PostgreSQL, and exposes a REST API with a live dashboard.

Built to show the kind of data pipeline work done at JPMorgan: automated ingestion, queueing, storage, and serving with real-time market data.

## Architecture

```
Alpha Vantage API -> Python Worker -> Redis Queue -> PostgreSQL -> REST API -> Dashboard
```

## Tech Stack
- Python, Flask, Gunicorn
- Redis (queue + cache)
- PostgreSQL (persistence)
- Alpha Vantage API
- Render (deployment)

## Getting Started

```bash
git clone https://github.com/hkhatib005/stock-pipeline
cd stock-pipeline
pip install -r requirements.txt
export REDIS_URL=redis://localhost:6379
export DATABASE_URL=postgresql://user:pass@host/db
export ALPHA_VANTAGE_KEY=your_key
python app.py
```

## Endpoints
| Method | Route | Description |
|--------|-------|-------------|
| GET | /api/prices | Latest price for all symbols |
| GET | /api/prices/<symbol> | Price history |
| GET | /api/health | Health check |

## License
MIT
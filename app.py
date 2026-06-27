import os
import json
import time
import threading
import requests
from datetime import datetime
from flask import Flask, jsonify, render_template

# Optional dependencies — gracefully skip if not installed
try:
    import redis
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    pass

app = Flask(__name__)

# Config — set these in environment for production
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
ALPHA_KEY = os.environ.get("ALPHA_VANTAGE_KEY", "demo")
SYMBOLS = ["AAPL", "GOOGL", "MSFT", "AMZN", "META"]

# Redis client — None if unavailable
try:
    r = redis.from_url(REDIS_URL, decode_responses=True)
except Exception:
    r = None


def get_db():
    if not DATABASE_URL:
        return None
    try:
        return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    except:
        return None


def fetch_price(symbol):
    """Fetch live price from Alpha Vantage; fall back to seeded random data."""
    try:
        url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey={ALPHA_KEY}"
        res = requests.get(url, timeout=5)
        data = res.json().get("Global Quote", {})
        return {
            "symbol": symbol,
            "price": float(data.get("05. price", 0)),
            "volume": int(data.get("06. volume", 0)),
            "fetched_at": datetime.utcnow().isoformat()
        }
    except:
        # API unavailable — return realistic mock prices for development
        import random
        base = {"AAPL": 189, "GOOGL": 175, "MSFT": 415, "AMZN": 185, "META": 490}
        return {
            "symbol": symbol,
            "price": round(base.get(symbol, 100) + random.uniform(-2, 2), 2),
            "volume": random.randint(1_000_000, 5_000_000),
            "fetched_at": datetime.utcnow().isoformat()
        }


def worker():
    """Background thread: polls prices every 60s, writes to Redis and Postgres."""
    while True:
        for sym in SYMBOLS:
            try:
                data = fetch_price(sym)

                if r:
                    r.lpush("stock_queue", json.dumps(data))
                    r.set(f"price:{sym}", json.dumps(data), ex=60)

                conn = get_db()
                if conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "INSERT INTO stock_prices (symbol, price, volume) VALUES (%s, %s, %s)",
                            (data["symbol"], data["price"], data["volume"])
                        )
                    conn.commit()
                    conn.close()

            except Exception as e:
                print(f"Error fetching {sym}: {e}")

        time.sleep(60)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/prices")
def prices():
    """Return latest price for all tracked symbols, using Redis cache when available."""
    results = []
    for sym in SYMBOLS:
        if r:
            cached = r.get(f"price:{sym}")
            if cached:
                results.append(json.loads(cached))
                continue
        results.append(fetch_price(sym))
    return jsonify(results)


@app.route("/api/prices/<symbol>")
def history(symbol):
    """Return last 50 price records for a given symbol from Postgres."""
    conn = get_db()
    if not conn:
        return jsonify({"error": "DB not configured"}), 503
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT symbol, price, volume, fetched_at FROM stock_prices WHERE symbol=%s ORDER BY fetched_at DESC LIMIT 50",
                (symbol.upper(),)
            )
            return jsonify([dict(row) for row in cur.fetchall()])
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/health")
def health():
    return jsonify({
        "status": "ok",
        "redis": r.ping() if r else False,
        "timestamp": datetime.utcnow().isoformat()
    })


if __name__ == "__main__":
    threading.Thread(target=worker, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)

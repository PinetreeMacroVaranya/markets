"""
Portfolio Intelligence - Data Fetcher
Runs daily via GitHub Actions.
Reads tickers.json, fetches price data + news, writes portfolio.json
"""

import json
import os
import sys
from datetime import datetime, date
import requests

try:
    import yfinance as yf
    import pandas as pd
    import numpy as np
except ImportError as e:
    print(f"ERROR: Missing library: {e}. Run: pip install yfinance pandas numpy")
    sys.exit(1)

try:
    import feedparser
except ImportError:
    print("ERROR: feedparser not installed. Run: pip install feedparser")
    sys.exit(1)

# -----------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------

NEWS_PER_TICKER = 10
HISTORY_PERIOD  = "15mo"   # enough for 250 trading days + buffer

# -----------------------------------------------------------------
# HELPERS
# -----------------------------------------------------------------

def log(msg):
    print(f"[{datetime.utcnow().strftime('%H:%M:%S')}] {msg}", flush=True)

def safe_float(v, decimals=2):
    try:
        f = float(v)
        if f != f:
            return None
        return round(f, decimals)
    except (TypeError, ValueError):
        return None

# -----------------------------------------------------------------
# LOAD TICKERS
# -----------------------------------------------------------------

def load_tickers():
    try:
        with open("tickers.json", "r") as f:
            tickers = json.load(f)
        return [t.strip().upper() for t in tickers if t.strip()]
    except (FileNotFoundError, json.JSONDecodeError) as e:
        log(f"ERROR: Could not read tickers.json: {e}")
        return []

# -----------------------------------------------------------------
# TECHNICAL INDICATORS
# -----------------------------------------------------------------

def compute_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

def compute_cmf(high, low, close, volume, period=21):
    """Chaikin Money Flow (21-day)"""
    hl_range = (high - low).replace(0, float('nan'))
    mfm = ((close - low) - (high - close)) / hl_range
    mfv = mfm * volume
    cmf = mfv.rolling(period).sum() / volume.rolling(period).sum()
    return cmf

def compute_zscore(close, period=90):
    """Z-Score: how many std devs current price is from 90-day mean"""
    mean = close.rolling(period).mean()
    std  = close.rolling(period).std()
    return (close - mean) / std.replace(0, float('nan'))

def get_ma_signal(price, ema50, ema200):
    """
    BUY     : price > ema50 AND > ema200 AND within 5% above ema50
    HOLD    : price > ema50 AND > ema200 AND more than 5% above ema50
    MONITOR : price < ema50 BUT > ema200
    SELL    : price < ema200
    """
    if price is None or ema50 is None or ema200 is None:
        return "UNKNOWN"
    if price < ema200:
        return "SELL"
    if price < ema50:
        return "MONITOR"
    pct_above_ema50 = (price - ema50) / ema50 * 100
    if pct_above_ema50 <= 5.0:
        return "BUY"
    return "HOLD"

def pct_return(series, days):
    """Return % change over N trading days"""
    if len(series) <= days:
        return None
    current = series.iloc[-1]
    past    = series.iloc[-(days + 1)]
    if past == 0 or pd.isna(past):
        return None
    return round((current - past) / past * 100, 2)

# -----------------------------------------------------------------
# FETCH STOCK DATA
# -----------------------------------------------------------------

def fetch_stock(ticker):
    log(f"  Fetching {ticker}...")
    try:
        t    = yf.Ticker(ticker)
        hist = t.history(period=HISTORY_PERIOD, interval="1d", auto_adjust=True)

        if hist.empty or len(hist) < 30:
            log(f"    {ticker}: insufficient data ({len(hist)} rows)")
            return {"status": "error", "note": f"Insufficient price history for {ticker}"}

        close  = hist["Close"].dropna()
        high   = hist["High"].dropna()
        low    = hist["Low"].dropna()
        volume = hist["Volume"].dropna()

        price  = safe_float(close.iloc[-1], 2)
        as_of  = close.index[-1].strftime("%Y-%m-%d")

        # Returns
        returns = {
            "1d":   pct_return(close, 1),
            "5d":   pct_return(close, 5),
            "30d":  pct_return(close, 30),
            "90d":  pct_return(close, 90),
            "150d": pct_return(close, 150),
            "250d": pct_return(close, 250),
        }

        # EMAs
        ema50_series  = compute_ema(close, 50)
        ema200_series = compute_ema(close, 200)
        ema50         = safe_float(ema50_series.iloc[-1], 2)
        ema200        = safe_float(ema200_series.iloc[-1], 2)

        # MA Signal
        signal = get_ma_signal(price, ema50, ema200)

        # Z-Score (90-day)
        zs     = compute_zscore(close, 90)
        zscore = safe_float(zs.iloc[-1], 2)

        # CMF (21-day)
        cmf_series = compute_cmf(high, low, close, volume, 21)
        cmf        = safe_float(cmf_series.iloc[-1], 3)

        # Previous close
        prev_close = safe_float(close.iloc[-2], 2) if len(close) >= 2 else None

        # Company name
        name = ticker
        try:
            info = t.info
            name = info.get("shortName") or info.get("longName") or ticker
        except Exception:
            pass

        log(f"    {ticker}: ${price} | {signal} | Z={zscore} | CMF={cmf}")

        return {
            "status":     "ok",
            "name":       name,
            "ticker":     ticker,
            "price":      price,
            "prev_close": prev_close,
            "change_pct": returns["1d"],
            "as_of":      as_of,
            "returns":    returns,
            "ema50":      ema50,
            "ema200":     ema200,
            "ma_signal":  signal,
            "zscore":     zscore,
            "cmf":        cmf,
            "note":       "",
            "news":       []
        }

    except Exception as e:
        log(f"    ERROR {ticker}: {e}")
        return {
            "status": "error",
            "ticker": ticker,
            "name":   ticker,
            "note":   str(e),
            "news":   []
        }

# -----------------------------------------------------------------
# FETCH NEWS
# -----------------------------------------------------------------

def fetch_news(ticker, max_articles=10):
    articles = []
    seen_titles = set()

    # Source 1: Yahoo Finance RSS
    try:
        url  = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"
        feed = feedparser.parse(url)
        for entry in feed.entries[:max_articles]:
            title = entry.get("title", "").strip()
            if title and title not in seen_titles:
                articles.append({
                    "title":     title,
                    "link":      entry.get("link", ""),
                    "published": entry.get("published", ""),
                    "source":    "Yahoo Finance"
                })
                seen_titles.add(title)
    except Exception as e:
        log(f"    Yahoo RSS failed for {ticker}: {e}")

    # Source 2: Google News RSS
    if len(articles) < max_articles:
        try:
            url  = f"https://news.google.com/rss/search?q={ticker}+stock&hl=en-US&gl=US&ceid=US:en"
            feed = feedparser.parse(url)
            for entry in feed.entries:
                if len(articles) >= max_articles:
                    break
                title = entry.get("title", "").strip()
                if title and title not in seen_titles:
                    articles.append({
                        "title":     title,
                        "link":      entry.get("link", ""),
                        "published": entry.get("published", ""),
                        "source":    "Google News"
                    })
                    seen_titles.add(title)
        except Exception as e:
            log(f"    Google News RSS failed for {ticker}: {e}")

    return articles[:max_articles]

# -----------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------

def main():
    log("=== Portfolio Data Fetch Starting ===")

    tickers = load_tickers()
    if not tickers:
        log("No tickers found in tickers.json — writing empty portfolio.json")
        output = {
            "generated_at":   datetime.utcnow().isoformat() + "Z",
            "generated_date": date.today().isoformat(),
            "tickers":        [],
            "stocks":         {}
        }
        with open("portfolio.json", "w") as f:
            json.dump(output, f, indent=2)
        return

    log(f"Tickers: {tickers}")

    stocks = {}
    for ticker in tickers:
        data       = fetch_stock(ticker)
        data["news"] = fetch_news(ticker, NEWS_PER_TICKER)
        stocks[ticker] = data

    ok  = sum(1 for v in stocks.values() if v["status"] == "ok")
    err = sum(1 for v in stocks.values() if v["status"] == "error")
    log(f"\nSummary: OK={ok} | Errors={err}")

    output = {
        "generated_at":   datetime.utcnow().isoformat() + "Z",
        "generated_date": date.today().isoformat(),
        "tickers":        tickers,
        "stocks":         stocks,
    }

    with open("portfolio.json", "w") as f:
        json.dump(output, f, indent=2)

    log(f"portfolio.json written — {len(json.dumps(output)) // 1024} KB")
    log("=== Done ===")

if __name__ == "__main__":
    main()

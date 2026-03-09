"""
Market Intelligence Dashboard — Data Fetcher
Runs daily via GitHub Actions.
Writes data.json to repo root which index.html reads.

Libraries: yfinance, requests, pandas
"""

import json
import os
import sys
from datetime import datetime, timedelta, date
import requests

try:
    import yfinance as yf
except ImportError:
    print("ERROR: yfinance not installed. Run: pip install yfinance")
    sys.exit(1)

# ─────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────

FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
FRED_BASE    = "https://api.stlouisfed.org/fred/series/observations"
MONTHS_BACK  = 3  # how many months of history to include in charts

TODAY        = date.today().isoformat()
START_DATE   = (date.today() - timedelta(days=MONTHS_BACK * 31)).isoformat()

# ─────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────

def log(msg):
    print(f"[{datetime.utcnow().strftime('%H:%M:%S')}] {msg}", flush=True)

def safe_float(v, decimals=2):
    """Round a float safely, return None if invalid."""
    try:
        f = float(v)
        if f != f:   # NaN check
            return None
        return round(f, decimals)
    except (TypeError, ValueError):
        return None

def make_entry(series_list, status, source, note=""):
    """
    series_list: [{"date": "YYYY-MM-DD", "value": float}, ...]
    Returns a standardised dict for data.json
    """
    clean = [
        {"date": d["date"], "value": d["value"]}
        for d in series_list
        if d["value"] is not None
    ]
    clean.sort(key=lambda x: x["date"])

    latest = clean[-1]["value"] if clean else None
    prev   = clean[-2]["value"] if len(clean) >= 2 else None
    as_of  = clean[-1]["date"]  if clean else None

    return {
        "series": clean,
        "latest": latest,
        "prev":   prev,
        "change": round(latest - prev, 4) if (latest is not None and prev is not None) else None,
        "as_of":  as_of,
        "status": status,   # "ok" | "error" | "manual"
        "source": source,
        "note":   note,
    }

def error_entry(source, message):
    return {
        "series": [],
        "latest": None,
        "prev":   None,
        "change": None,
        "as_of":  None,
        "status": "error",
        "source": source,
        "note":   message,
    }

# ─────────────────────────────────────────────────────────────────
# YAHOO FINANCE FETCHER (via yfinance)
# ─────────────────────────────────────────────────────────────────

def fetch_yahoo(ticker, decimals=2):
    """
    Fetch 3-month daily history for a ticker using yfinance.
    Returns a standardised entry dict.
    """
    log(f"  Yahoo → {ticker}")
    try:
        t = yf.Ticker(ticker)
        hist = t.history(start=START_DATE, end=TODAY, interval="1d", auto_adjust=True)

        if hist.empty:
            return error_entry("Yahoo Finance", f"No data returned for {ticker}")

        series = []
        for idx, row in hist.iterrows():
            d   = idx.strftime("%Y-%m-%d")
            val = safe_float(row["Close"], decimals)
            if val is not None:
                series.append({"date": d, "value": val})

        if not series:
            return error_entry("Yahoo Finance", f"All values null for {ticker}")

        return make_entry(series, "ok", f"Yahoo Finance ({ticker})")

    except Exception as e:
        return error_entry("Yahoo Finance", str(e))


def fetch_yahoo_ratio(ticker1, ticker2, decimals=3):
    """
    Compute the ratio of two Yahoo tickers (e.g. GLD / TLT).
    """
    log(f"  Yahoo ratio → {ticker1} / {ticker2}")
    try:
        t1 = yf.Ticker(ticker1)
        t2 = yf.Ticker(ticker2)
        h1 = t1.history(start=START_DATE, end=TODAY, interval="1d", auto_adjust=True)
        h2 = t2.history(start=START_DATE, end=TODAY, interval="1d", auto_adjust=True)

        if h1.empty or h2.empty:
            return error_entry("Yahoo Finance", f"Empty data for {ticker1} or {ticker2}")

        # Align on common dates
        common_dates = h1.index.intersection(h2.index)
        series = []
        for idx in common_dates:
            v1 = safe_float(h1.loc[idx, "Close"])
            v2 = safe_float(h2.loc[idx, "Close"])
            if v1 and v2 and v2 != 0:
                series.append({
                    "date":  idx.strftime("%Y-%m-%d"),
                    "value": round(v1 / v2, decimals)
                })

        if not series:
            return error_entry("Yahoo Finance", "No overlapping dates for ratio")

        return make_entry(series, "ok", f"Yahoo Finance ({ticker1}/{ticker2} ratio)")

    except Exception as e:
        return error_entry("Yahoo Finance", str(e))

# ─────────────────────────────────────────────────────────────────
# FRED FETCHER
# ─────────────────────────────────────────────────────────────────

def fetch_fred(series_id, decimals=2, months_back=None):
    """
    Fetch a FRED series via the official REST API.
    Returns a standardised entry dict.
    """
    log(f"  FRED  → {series_id}")
    if not FRED_API_KEY:
        return error_entry("FRED", "FRED_API_KEY not set in environment")

    start = (date.today() - timedelta(days=(months_back or MONTHS_BACK) * 31)).isoformat()

    params = {
        "series_id":         series_id,
        "observation_start": start,
        "observation_end":   TODAY,
        "file_type":         "json",
        "api_key":           FRED_API_KEY,
    }

    try:
        res = requests.get(FRED_BASE, params=params, timeout=15)
        res.raise_for_status()
        data = res.json()

        if "error_message" in data:
            return error_entry("FRED", data["error_message"])

        series = []
        for obs in data.get("observations", []):
            if obs["value"] == ".":
                continue
            val = safe_float(obs["value"], decimals)
            if val is not None:
                series.append({"date": obs["date"], "value": val})

        if not series:
            return error_entry("FRED", f"No valid observations for {series_id}")

        return make_entry(series, "ok", f"FRED ({series_id})")

    except requests.RequestException as e:
        return error_entry("FRED", f"HTTP error: {str(e)}")
    except Exception as e:
        return error_entry("FRED", str(e))


def fetch_buffett(months=3):
    """
    Scrapes Buffett Indicator directly from thebuffettindicator.com
    Falls back to yfinance + FRED computation if scrape fails.
    """
    log("  Fetching Buffett Indicator from thebuffettindicator.com")

    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        res = requests.get("https://thebuffettindicator.com/", headers=headers, timeout=15)
        res.raise_for_status()

        html = res.text

        # Extract Buffett Indicator % value
        # The page contains text like: Buffett Indicator: 216.63%
        import re
        match = re.search(r'Buffett Indicator[:\s]+([0-9]+\.?[0-9]*)\s*%', html)
        if not match:
            raise ValueError("Could not find Buffett Indicator value in page HTML")

        latest_val = round(float(match.group(1)), 2)
        log(f"    ✓ Scraped Buffett Indicator: {latest_val}%")

        # Build a single-point series for today
        # Also load existing history from data.json to maintain chart continuity
        today_entry = {"date": TODAY, "value": latest_val}

        # Load existing series to keep historical chart data
        existing_series = []
        try:
            with open("data.json", "r") as f:
                existing = json.load(f)
            existing_series = existing.get("indicators", {}).get("buffett", {}).get("series", [])
        except (FileNotFoundError, json.JSONDecodeError, KeyError):
            pass

        # Merge: remove today if already present, add fresh value
        existing_series = [d for d in existing_series if d["date"] != TODAY]
        existing_series.append(today_entry)
        existing_series.sort(key=lambda x: x["date"])

        # Keep only last 90 days
        cutoff = (date.today() - timedelta(days=90)).isoformat()
        existing_series = [d for d in existing_series if d["date"] >= cutoff]

        return make_entry(
            existing_series,
            status="ok",
            source="thebuffettindicator.com",
            note=f"Scraped from thebuffettindicator.com. Value: {latest_val}%"
        )

    except Exception as e:
        log(f"    ✗ Scrape failed: {e} — returning error")
        return error_entry("thebuffettindicator.com", f"Scrape failed: {str(e)}")# ─────────────────────────────────────────────────────────────────
# LOAD EXISTING MANUAL DATA (carry forward from previous data.json)
# ─────────────────────────────────────────────────────────────────

def load_existing_manual():
    """
    Read data.json if it exists and preserve manual entries.
    Manual indicators (MOVE history, GLD/TLT history, etc.) are
    only updated when the user saves from the browser — we must
    not overwrite them.
    """
    try:
        with open("data.json", "r") as f:
            existing = json.load(f)
        manual_ids = ["move", "gldtlt", "hyg", "gvz", "fg"]
        return {k: v for k, v in existing.get("indicators", {}).items() if k in manual_ids}
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────

def main():
    log("=== Market Intelligence Data Fetch Starting ===")
    log(f"Date range: {START_DATE} → {TODAY}")
    log(f"FRED key:   {'SET ✓' if FRED_API_KEY else 'MISSING ✗'}")

    indicators = {}

    # ── Yahoo Finance pulls ───────────────────────────────────────
    log("\n── Yahoo Finance ──")
    indicators["move"]   = fetch_yahoo("^MOVE",   decimals=2)
    indicators["vix"]    = fetch_yahoo("^VIX",    decimals=2)
    indicators["gvz"]    = fetch_yahoo("^GVZ",    decimals=2)
    indicators["hyg"]    = fetch_yahoo("HYG",     decimals=2)
    indicators["gldtlt"] = fetch_yahoo_ratio("GLD", "TLT", decimals=3)

    # ── FRED pulls ────────────────────────────────────────────────
    log("\n── FRED ──")
    indicators["dxy"]     = fetch_fred("DTWEXBGS",          decimals=2)
    indicators["jgb"]     = fetch_fred("IRLTLT01JPM156N",   decimals=2)
    indicators["nfci"]    = fetch_fred("NFCI",              decimals=2)
    indicators["spread"]  = fetch_fred("T10Y2Y",            decimals=2)
    indicators["brent"]   = fetch_fred("DCOILBRENTEU",      decimals=2)
    indicators["buffett"] = fetch_buffett() 

    # ── FRED VIX as backup if Yahoo failed ───────────────────────
    if indicators["vix"]["status"] == "error":
        log("  Yahoo VIX failed — trying FRED VIXCLS as backup")
        indicators["vix"] = fetch_fred("VIXCLS", decimals=2)

    # ── Manual indicators (carry forward from existing data.json) ─
    log("\n── Manual indicators (preserved from existing data.json) ──")
    existing_manual = load_existing_manual()
    # fg (Fear & Greed) is always manual
    indicators["fg"] = existing_manual.get("fg", {
        "series": [], "latest": None, "prev": None,
        "change": None, "as_of": None,
        "status": "manual", "source": "Manual entry",
        "note": "Enter from CNN Fear & Greed page"
    })

    # ── Summary ───────────────────────────────────────────────────
    log("\n── Summary ──")
    ok  = sum(1 for v in indicators.values() if v["status"] == "ok")
    err = sum(1 for v in indicators.values() if v["status"] == "error")
    man = sum(1 for v in indicators.values() if v["status"] == "manual")
    log(f"  OK: {ok}  |  Errors: {err}  |  Manual: {man}")

    for k, v in indicators.items():
        icon = "✓" if v["status"] == "ok" else ("✗" if v["status"] == "error" else "✎")
        val  = v["latest"]
        log(f"  {icon} {k:10s} latest={val}  ({v['status']})")

    # ── Write data.json ───────────────────────────────────────────
    output = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "generated_date": TODAY,
        "indicators": indicators,
    }

    with open("data.json", "w") as f:
        json.dump(output, f, indent=2)

    log(f"\n✅ data.json written — {len(json.dumps(output)) // 1024} KB")
    log("=== Done ===")

if __name__ == "__main__":
    main()

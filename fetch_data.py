"""
Market Intelligence Dashboard - Data Fetcher
Runs daily via GitHub Actions.
Writes data.json to repo root which index.html reads.
"""

import json
import os
import sys
from datetime import datetime, timedelta, date
import requests

try:
    import yfinance as yf
except ImportError:
    print("ERROR: yfinance not installed.")
    sys.exit(1)

# -----------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------

FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
TE_API_KEY = os.environ.get("TE_API_KEY", "")
FRED_BASE    = "https://api.stlouisfed.org/fred/series/observations"
MONTHS_BACK  = 3

TODAY      = (date.today() + timedelta(days=1)).isoformat()
START_DATE = (date.today() - timedelta(days=MONTHS_BACK * 31)).isoformat()

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

def make_entry(series_list, status, source, note=""):
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
        "status": status,
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

def manual_entry(source, note):
    return {
        "series": [],
        "latest": None,
        "prev":   None,
        "change": None,
        "as_of":  None,
        "status": "manual",
        "source": source,
        "note":   note,
    }

# -----------------------------------------------------------------
# YAHOO FINANCE
# -----------------------------------------------------------------

def fetch_yahoo(ticker, decimals=2):
    log(f"  Yahoo -> {ticker}")
    try:
        t    = yf.Ticker(ticker)
        hist = t.history(start=START_DATE, end=TODAY, interval="1d", auto_adjust=True)
        if hist.empty:
            return error_entry("Yahoo Finance", f"No data returned for {ticker}")
        series = []
        for idx, row in hist.iterrows():
            val = safe_float(row["Close"], decimals)
            if val is not None:
                series.append({"date": idx.strftime("%Y-%m-%d"), "value": val})
        if not series:
            return error_entry("Yahoo Finance", f"All values null for {ticker}")
        return make_entry(series, "ok", f"Yahoo Finance ({ticker})")
    except Exception as e:
        return error_entry("Yahoo Finance", str(e))

def fetch_yahoo_ratio(ticker1, ticker2, decimals=3):
    log(f"  Yahoo ratio -> {ticker1} / {ticker2}")
    try:
        h1 = yf.Ticker(ticker1).history(start=START_DATE, end=TODAY, interval="1d", auto_adjust=True)
        h2 = yf.Ticker(ticker2).history(start=START_DATE, end=TODAY, interval="1d", auto_adjust=True)
        if h1.empty or h2.empty:
            return error_entry("Yahoo Finance", f"Empty data for {ticker1} or {ticker2}")
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

# -----------------------------------------------------------------
# FRED
# -----------------------------------------------------------------

def fetch_fred(series_id, decimals=2, months_back=None):
    log(f"  FRED  -> {series_id}")
    if not FRED_API_KEY:
        return error_entry("FRED", "FRED_API_KEY not set in environment")
    start = (date.today() - timedelta(days=(months_back or MONTHS_BACK) * 31)).isoformat()
    params = {
        "series_id":         series_id,
        "observation_start": start,
        "observation_end":   date.today().isoformat(),
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
# -----------------------------------------------------------------
# TRADING ECONOMICS
# -----------------------------------------------------------------
def fetch_te_jgb():
    """
    Fetch Japan 10Y Government Bond yield from Trading Economics API.
    Returns a standardised entry dict.
    """
    log("  Trading Economics -> Japan 10Y Bond Yield")
    if not TE_API_KEY:
        return error_entry("Trading Economics", "TE_API_KEY not set in environment")
    try:
        url = f"https://api.tradingeconomics.com/markets/bond?c={TE_API_KEY}&country=japan"
        res = requests.get(url, timeout=15)
        res.raise_for_status()
        data = res.json()

        # Find the 10Y bond entry
        entry = None
        for item in data:
            name = item.get("Name","").lower()
            if "10" in name and "year" in name.lower() or item.get("Ticker","") == "GJGB10":
                entry = item
                break

        if not entry and data:
            entry = data[0]  # fallback to first result

        if not entry:
            return error_entry("Trading Economics", "No Japan bond data returned")

        value = safe_float(entry.get("Last") or entry.get("Price"), 2)
        dt    = entry.get("Date") or entry.get("LastUpdate") or date.today().isoformat()
        # Normalise date format
        try:
            dt = dt[:10]
        except Exception:
            dt = date.today().isoformat()

        if value is None:
            return error_entry("Trading Economics", "Null value returned for JGB yield")

        log(f"    JGB 10Y -> {value}% as of {dt}")

        # Build a single point — no historical series from this endpoint
        # Load existing series to maintain chart continuity
        existing_series = []
        try:
            with open("data.json", "r") as f:
                existing = json.load(f)
            existing_series = existing.get("indicators", {}).get("jgb", {}).get("series", [])
        except (FileNotFoundError, json.JSONDecodeError):
            pass

        # Remove today if already present, add fresh value
        existing_series = [d for d in existing_series if d["date"] != dt]
        existing_series.append({"date": dt, "value": value})
        existing_series.sort(key=lambda x: x["date"])

        # Keep last 90 days
        cutoff = (date.today() - timedelta(days=90)).isoformat()
        existing_series = [d for d in existing_series if d["date"] >= cutoff]

        return make_entry(
            existing_series,
            status="ok",
            source="Trading Economics",
            note=f"Japan 10Y Government Bond Yield. Daily via Trading Economics API."
        )

    except Exception as e:
        log(f"    TE JGB failed: {e}")
        return error_entry("Trading Economics", str(e))

# -----------------------------------------------------------------
# LOAD EXISTING MANUAL DATA
# -----------------------------------------------------------------

def load_existing_manual():
    try:
        with open("data.json", "r") as f:
            existing = json.load(f)
        manual_ids = ["fg", "buffett"]
        return {k: v for k, v in existing.get("indicators", {}).items() if k in manual_ids}
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

# -----------------------------------------------------------------
# MONTHLY HISTORY UPDATE
# -----------------------------------------------------------------

from datetime import date, timedelta
import json

def is_day_after_last_trading_day():
    """Returns True if yesterday was the last trading day of its month."""
    today = date.today()
    yesterday = today - timedelta(days=1)
    
    # If yesterday was a weekend, it wasn't the last trading day
    if yesterday.weekday() >= 5:
        return False

    # Look ahead from 'yesterday' to find the next valid weekday (Mon-Fri)
    # If that next weekday is in a different month, yesterday was the last trading day.
    for i in range(1, 4):
        next_day = yesterday + timedelta(days=i)
        if next_day.weekday() < 5:
            return next_day.month != yesterday.month
            
    return False

def update_monthly_history(indicators):
    """Updates the 6-month history using data from the morning after month-end."""
    if not is_day_after_last_trading_day():
        log("  Not the morning after month-end - skipping monthly history update")
        return

    log("  Month-end follow-up detected - updating monthly_history.json")
    
    try:
        with open("monthly_history.json", "r") as f:
            history = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        history = {"months": []}

    # We use 'yesterday' for the labels/dates because the data represents the month just closed
    today = date.today()
    target_date = today - timedelta(days=1)
    
    # Formatting: e.g., "31-Mar-26"
    label = f"{target_date.day}-{target_date.strftime('%b-%y')}"
    
    values = {}
    indicator_list = [
        "move", "dxy", "jgb", "nfci", "gldtlt", "spread", 
        "vix", "brent", "buffett", "hyg", "gvz", "fg"
    ]
    
    for ind_id in indicator_list:
        values[ind_id] = indicators.get(ind_id, {}).get("latest")

    new_entry = {
        "label":  label,
        "date":   target_date.isoformat(), # Stored as YYYY-MM-DD of the last day
        "values": values,
    }

    months = history.get("months", [])
    
    # Remove any existing entry for the target month to prevent duplicates
    month_prefix = target_date.strftime("%Y-%m")
    months = [m for m in months if not m["date"].startswith(month_prefix)]
    
    months.append(new_entry)
    months.sort(key=lambda x: x["date"])
    
    # Keep only the last 6 months
    months = months[-6:]
    history["months"] = months
    
    with open("monthly_history.json", "w") as f:
        json.dump(history, f, indent=2)
        
    log(f"  monthly_history.json updated - {len(months)} months stored (Entry: {label})")

# -----------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------

def main():
    log("=== Market Intelligence Data Fetch Starting ===")
    log(f"Date range: {START_DATE} -> {TODAY}")
    log(f"FRED key:   {'SET' if FRED_API_KEY else 'MISSING'}")

    indicators = {}

    # Yahoo Finance
    log("\n-- Yahoo Finance --")
    indicators["move"]   = fetch_yahoo("^MOVE",  decimals=2)
    indicators["vix"]    = fetch_yahoo("^VIX",   decimals=2)
    indicators["gvz"]    = fetch_yahoo("^GVZ",   decimals=2)
    indicators["hyg"]    = fetch_yahoo("HYG",    decimals=2)
    indicators["gldtlt"] = fetch_yahoo_ratio("GLD", "TLT", decimals=3)
   

    # FRED
    log("\n-- FRED --")
    indicators["dxy"] = fetch_fred("DTWEXBGS",           decimals=2)
   indicators["jgb"] = fetch_fred("IRLTLT01JPM156N", decimals=2, months_back=6)
    indicators["nfci"]   = fetch_fred("NFCI",            decimals=2)
    indicators["spread"] = fetch_fred("T10Y2Y",          decimals=2)
    indicators["brent"]  = fetch_fred("DCOILBRENTEU",    decimals=2)

    # VIX fallback
    if indicators["vix"]["status"] == "error":
        log("  Yahoo VIX failed - trying FRED VIXCLS as backup")
        indicators["vix"] = fetch_fred("VIXCLS", decimals=2)

    # Manual indicators - carried forward from existing data.json
    log("\n-- Manual indicators --")
    existing_manual = load_existing_manual()

    indicators["fg"] = existing_manual.get("fg", manual_entry(
        "Manual entry",
        "Enter from CNN Fear & Greed page"
    ))

    indicators["buffett"] = existing_manual.get("buffett", manual_entry(
        "Manual entry",
        "Enter from thebuffettindicator.com"
    ))

    # Monthly history
    log("\n-- Monthly history check --")
    update_monthly_history(indicators)

    # Summary
    log("\n-- Summary --")
    ok  = sum(1 for v in indicators.values() if v["status"] == "ok")
    err = sum(1 for v in indicators.values() if v["status"] == "error")
    man = sum(1 for v in indicators.values() if v["status"] == "manual")
    log(f"  OK: {ok}  |  Errors: {err}  |  Manual: {man}")
    for k, v in indicators.items():
        icon = "OK" if v["status"] == "ok" else ("ERR" if v["status"] == "error" else "MAN")
        log(f"  [{icon}] {k:10s} latest={v['latest']}  as_of={v.get('as_of')}")

    # Write data.json
    output = {
        "generated_at":   datetime.utcnow().isoformat() + "Z",
        "generated_date": date.today().isoformat(),
        "indicators":     indicators,
    }
    with open("data.json", "w") as f:
        json.dump(output, f, indent=2)
    log(f"\n data.json written - {len(json.dumps(output)) // 1024} KB")
    log("=== Done ===")

if __name__ == "__main__":
    main()

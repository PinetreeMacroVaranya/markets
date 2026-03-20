"""
ETF Screener - Data Fetcher
Runs daily via GitHub Actions.
Fetches top/bottom ETFs by return and newly listed ETFs.
Writes etf_data.json to repo root.
"""

import json
import os
import sys
from datetime import datetime, date, timedelta
import requests
import time

try:
    import yfinance as yf
    import pandas as pd
    import numpy as np
except ImportError as e:
    print(f"ERROR: Missing library: {e}")
    sys.exit(1)

# -----------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------

TOP_N = 10  # top and bottom N ETFs per period

# 500 largest US ETFs by AUM - curated list
ETF_UNIVERSE = [
    "SPY","IVV","VOO","VTI","QQQ","VEA","IEFA","AGG","VUG","VTV",
    "BND","VWO","GLD","IJH","IWM","VIG","IEMG","IJR","VGT","VO",
    "IWF","VYD","VXUS","BSV","MUB","VB","ITOT","IWD","IEF","LQD",
    "TLT","SHY","VNQ","XLF","XLK","XLE","XLV","XLI","XLC","XLY",
    "XLP","XLU","XLB","XLRE","GDX","SLV","USO","UNG","DBO","DJP",
    "IAU","SGOL","SIVR","PALL","PPLT","DBB","DBC","DBO","GSG","PDBC",
    "HYG","JNK","EMB","VCIT","VCSH","VGSH","VGIT","VGLT","VMBS","MBB",
    "TIP","SCHP","STIP","VTIP","SPTI","SPTL","SPTS","SPAB","SPSB","SPSM",
    "SCHB","SCHX","SCHD","SCHF","SCHE","SCHH","SCHG","SCHV","SCHA","SCHM",
    "ARKK","ARKQ","ARKW","ARKG","ARKF","ARKX","ARKB","ARKY","ARKZ","ARKD",
    "SPDW","SPEM","SPMB","SPMD","SPLG","SPYG","SPYV","SPYD","SPMO","SPGP",
    "DIA","MDY","RSP","OEF","SDY","DVY","VHT","VFH","VIS","VAW",
    "VCR","VDC","VPU","VRE","VOX","VBK","VBR","VMC","VGK","VPL",
    "EFA","EEM","EWJ","EWZ","EWC","EWG","EWU","EWA","EWH","EWS",
    "EWL","EWT","EWY","EWP","EWQ","EWI","EWD","EWN","EWO","EWK",
    "FXI","MCHI","KWEB","CQQQ","ASHR","GXC","CNXT","KBA","KURE","CXSE",
    "EPI","INDA","SMIN","INDY","ADRE","AAXJ","GMF","GEM","IEMG","EEMA",
    "VWO","SPEM","DEM","EDIV","EMHY","LEMB","FEMB","PCY","VWOB","EMAG",
    "IYW","IGV","SOXX","SMH","QTEC","SKYY","CLOU","WCLD","BUG","CIBR",
    "HACK","IHAK","ROBO","BOTZ","IRBO","THNQ","DTEC","LRNZ","EDUT","EMQQ",
    "ICLN","QCLN","ACES","SMOG","CNRG","ERTH","VEGN","KRBN","NETZ","CLMA",
    "IBB","XBI","LABU","ARKG","PTH","BBH","FBT","SBIO","GNOM","HELX",
    "GNR","XME","PICK","SLX","REMX","LIT","COPX","URA","URNM","NLR",
    "REIT","IYR","RWR","SCHH","REM","MORT","HOMZ","ROOF","INDS","FFR",
    "TBF","TMF","TMV","TBT","TTT","SPTL","VGLT","EDV","ZROZ","GOVZ",
    "PSQ","SH","RWM","DOG","SDS","QID","TWM","MYY","MZZ","SPXU",
    "TQQQ","UPRO","UDOW","URTY","TNA","SPXL","TECL","FNGU","SOXL","LABU",
    "JEPI","JEPQ","DIVO","QYLD","RYLD","XYLD","NUSI","GPIX","GPIQ","XDTE",
    "AMLP","AMJ","ENFR","MLPA","MLPX","TPYP","ZMLP","MLPB","AMZA","AMJB",
    "PFF","PFFD","FPE","IPFF","SPFF","PFXF","HYD","HYMB","MUNI","CMF",
    "BKLN","SRLN","FLBL","FLRN","FLTR","FLOT","USFR","TFLO","ICSH","NEAR",
    "SHV","BIL","SGOV","CLTL","TBLL","TBIL","GBIL","VBIL","ZZZD","OPER",
    "MINT","JPST","GSY","PULS","ULST","RAVI","SCHO","FTSM","ISTB","IGSB",
    "ANGL","FALN","HYS","HYDB","SHYG","USHY","HYLB","HYHG","HYXF","HYXU",
    "BNDX","IAGG","BWX","WIP","IGOV","ISHG","EBND","PFUIX","IGBH","HYEM",
    "DXJ","HEDJ","HEFA","DBEF","DBEZ","HEWJ","HEWG","HEWU","HEEM","HEZU",
    "SPLV","USMV","EFAV","EEMV","ACWV","LGLV","FDLO","FIDU","FSTA","FENY",
    "FHLC","FMAT","FNCL","FREL","FTEC","FUTY","FCOM","FDIS","FIVG","FDRV",
    "ONEQ","FENY","FIDU","FSTA","FHLC","FMAT","FNCL","FREL","FUTY","FCOM",
    "QUAL","SIZE","VLUE","MTUM","USMF","LRGF","INTL","INTF","ESGU","ESGE",
    "USSG","ESGV","ESGE","SUSL","SUSA","CRBN","LOWC","ETHO","NUMV","NULV",
    "COWZ","CALF","DEEP","DSTL","DSTX","STXV","STXG","STXE","STXK","STXM",
    "MOAT","GOAT","WIDE","MFUS","MFEM","MFMO","MFGP","MFUS","MFDX","MFDE",
    "VRP","PFIG","PGF","PSK","IPFF","FPE","SPFF","PFFD","PFFR","PFXF"
]

# Remove duplicates while preserving order
seen = set()
ETF_UNIVERSE_CLEAN = []
for t in ETF_UNIVERSE:
    if t not in seen:
        seen.add(t)
        ETF_UNIVERSE_CLEAN.append(t)

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

def pct_change(current, past):
    if current is None or past is None or past == 0:
        return None
    return round((current - past) / past * 100, 2)

# -----------------------------------------------------------------
# FETCH ETF BATCH DATA
# -----------------------------------------------------------------

def fetch_etf_returns(tickers):
    """
    Fetch 95 days of history for all ETFs in batches.
    Returns dict: ticker -> {price, ret_1d, ret_1w, ret_1m, ret_3m, series, name, aum}
    """
    log(f"Fetching price history for {len(tickers)} ETFs...")
    results = {}

    # Fetch in batches of 50 to avoid rate limiting
    batch_size = 50
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i+batch_size]
        log(f"  Batch {i//batch_size + 1}: {batch[0]} ... {batch[-1]}")
        try:
            data = yf.download(
                batch,
                period="6mo",
                interval="1d",
                auto_adjust=True,
                progress=False,
                group_by="ticker",
                threads=True
            )

            for ticker in batch:
                try:
                    if len(batch) == 1:
                        close = data["Close"].dropna()
                    else:
                        if ticker not in data.columns.get_level_values(0):
                            continue
                        close = data[ticker]["Close"].dropna()

                    if len(close) < 5:
                        continue

                    price = safe_float(close.iloc[-1])
                    if price is None:
                        continue

                    # Build series for sparkline (last 30 days)
                    series = [
                        {"date": idx.strftime("%Y-%m-%d"), "value": safe_float(v)}
                        for idx, v in close.tail(30).items()
                        if safe_float(v) is not None
                    ]

                    results[ticker] = {
                        "ticker":  ticker,
                        "price":   price,
                        "ret_1d":  pct_change(close.iloc[-1], close.iloc[-2]) if len(close) >= 2 else None,
                        "ret_1w":  pct_change(close.iloc[-1], close.iloc[-6]) if len(close) >= 6 else None,
                        "ret_1m":  pct_change(close.iloc[-1], close.iloc[-22]) if len(close) >= 22 else None,
                        "ret_3m":  pct_change(close.iloc[-1], close.iloc[-66]) if len(close) >= 66 else None,
                        "series":  series,
                        "as_of":   close.index[-1].strftime("%Y-%m-%d"),
                        "name":    ticker,
                        "aum":     None,
                        "expense": None,
                        "category": None,
                    }
                except Exception as e:
                    pass

        except Exception as e:
            log(f"  Batch error: {e}")

        time.sleep(1)  # be nice to Yahoo Finance

    log(f"  Got data for {len(results)} ETFs")
    return results

def enrich_etf_info(results, sample_size=100):
    """
    Enrich top/bottom candidates with name, AUM, expense ratio, category.
    Only fetches info for tickers we actually need (saves time).
    """
    log("Enriching ETF metadata...")
    for ticker in list(results.keys())[:sample_size]:
        try:
            info = yf.Ticker(ticker).info
            results[ticker]["name"]     = info.get("longName") or info.get("shortName") or ticker
            results[ticker]["aum"]      = info.get("totalAssets")
            results[ticker]["expense"]  = info.get("annualReportExpenseRatio") or info.get("expenseRatio")
            results[ticker]["category"] = info.get("category") or info.get("etfType") or ""
            time.sleep(0.2)
        except Exception:
            pass

# -----------------------------------------------------------------
# RANK ETFs
# -----------------------------------------------------------------

def rank_etfs(results, period_key, n=10):
    """Return top N and bottom N ETFs for a given period."""
    valid = [
        v for v in results.values()
        if v.get(period_key) is not None
    ]
    valid.sort(key=lambda x: x[period_key], reverse=True)
    top    = valid[:n]
    bottom = valid[-n:][::-1]  # worst first
    return top, bottom

# -----------------------------------------------------------------
# FETCH NEWLY LISTED ETFs
# -----------------------------------------------------------------

def fetch_new_etfs():
    """
    Fetch ETFs listed in the last 30 days with AUM >= $50M.
    Uses a curated approach checking yfinance for recent IPO date.
    """
    log("Fetching newly listed ETFs...")
    new_etfs = []

    # Check a broader list for recent listings
    # We use iShares, Vanguard, State Street, Invesco new launches
    # pulled from ETF.com RSS / SEC EDGAR
    cutoff = date.today() - timedelta(days=30)

    # Try SEC EDGAR full-text search for N-1A (ETF registration) filings
    try:
        url = "https://efts.sec.gov/LATEST/search-index?q=%22exchange+traded+fund%22&dateRange=custom&startdt={}&enddt={}&forms=N-1A".format(
            cutoff.isoformat(), date.today().isoformat()
        )
        res = requests.get(url, timeout=15, headers={"User-Agent": "market-dashboard research@example.com"})
        if res.ok:
            data = res.json()
            hits = data.get("hits", {}).get("hits", [])
            log(f"  SEC EDGAR: {len(hits)} recent N-1A filings")
    except Exception as e:
        log(f"  SEC EDGAR failed: {e}")

    # Fallback: check a known list of recently launched ETFs via yfinance
    # These are tickers from major issuers launched recently
    candidates = [
        "BALI","SPGP","RSST","GGLL","NVDL","TSLL","MSFO","APLY","AMZO",
        "GOGL","METL","NFLY","MSTU","MSFU","NVDU","NVDD","TSLU","TSLQ",
        "CONY","MSFO","AMZY","GOOGY","PLTY","TSMY","YMAX","YMAG","YBIT",
        "LFGY","SNOY","NVDY","AMDY","GOGY","MSFY","PYPY","COINY","DISO",
        "FIVY","SIXO","SVOL","QQQY","SPYY","IWMY","DIVY","JEPY","FEPI",
        "AIPI","GPIX","GPIQ","XDTE","RDTE","WDTE","MDTE","QDTE","ODTE",
        "BMAX","BSVO","BUFD","BUFQ","BUFW","BUFZ","BUFE","BUFN","BUFO","BUFP"
    ]

    for ticker in candidates:
        try:
            t    = yf.Ticker(ticker)
            info = t.info
            aum  = info.get("totalAssets", 0) or 0

            if aum < 50_000_000:
                continue

            # Check if recently listed
            hist = t.history(period="3mo", interval="1d", auto_adjust=True)
            if hist.empty:
                continue

            first_date = hist.index[0].date()
            if first_date < cutoff:
                continue

            price   = safe_float(hist["Close"].iloc[-1])
            launch_ret = pct_change(hist["Close"].iloc[-1], hist["Close"].iloc[0])

            new_etfs.append({
                "ticker":     ticker,
                "name":       info.get("longName") or info.get("shortName") or ticker,
                "launch_date": first_date.isoformat(),
                "aum_m":      round(aum / 1_000_000, 1),
                "category":   info.get("category") or info.get("etfType") or "—",
                "issuer":     info.get("fundFamily") or "—",
                "ret_since_launch": launch_ret,
                "expense":    info.get("annualReportExpenseRatio") or info.get("expenseRatio"),
                "price":      price,
            })
            log(f"  New ETF: {ticker} launched {first_date} AUM=${round(aum/1e6,1)}M")
            time.sleep(0.3)

        except Exception:
            pass

    new_etfs.sort(key=lambda x: x["launch_date"], reverse=True)
    log(f"  Found {len(new_etfs)} newly listed ETFs with AUM >= $50M")
    return new_etfs

# -----------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------

def main():
    log("=== ETF Screener Data Fetch Starting ===")
    log(f"Universe: {len(ETF_UNIVERSE_CLEAN)} ETFs")

    # 1. Fetch price data for all ETFs
    results = fetch_etf_returns(ETF_UNIVERSE_CLEAN)

    # 2. Rank by each period
    top_1d,  bot_1d  = rank_etfs(results, "ret_1d",  TOP_N)
    top_1w,  bot_1w  = rank_etfs(results, "ret_1w",  TOP_N)
    top_1m,  bot_1m  = rank_etfs(results, "ret_1m",  TOP_N)
    top_3m,  bot_3m  = rank_etfs(results, "ret_3m",  TOP_N)

    # 3. Enrich top/bottom candidates with metadata
    candidates = set(
        [e["ticker"] for e in top_1d + bot_1d +
         top_1w + bot_1w + top_1m + bot_1m + top_3m + bot_3m]
    )
    enrich_targets = {k: results[k] for k in candidates if k in results}
    enrich_etf_info(enrich_targets, sample_size=len(enrich_targets))
    # Merge enriched data back
    for k, v in enrich_targets.items():
        results[k].update(v)

    # 4. Rebuild rankings with enriched data
    top_1d,  bot_1d  = rank_etfs(results, "ret_1d",  TOP_N)
    top_1w,  bot_1w  = rank_etfs(results, "ret_1w",  TOP_N)
    top_1m,  bot_1m  = rank_etfs(results, "ret_1m",  TOP_N)
    top_3m,  bot_3m  = rank_etfs(results, "ret_3m",  TOP_N)

    # 5. Fetch newly listed ETFs
    new_etfs = fetch_new_etfs()

    # 6. Write output
    output = {
        "generated_at":   datetime.utcnow().isoformat() + "Z",
        "generated_date": date.today().isoformat(),
        "universe_count": len(results),
        "rankings": {
            "1d":  {"top": top_1d,  "bottom": bot_1d},
            "1w":  {"top": top_1w,  "bottom": bot_1w},
            "1m":  {"top": top_1m,  "bottom": bot_1m},
            "3m":  {"top": top_3m,  "bottom": bot_3m},
        },
        "new_etfs": new_etfs,
    }

    with open("etf_data.json", "w") as f:
        json.dump(output, f, indent=2)

    log(f"\netf_data.json written — {len(json.dumps(output)) // 1024} KB")
    log(f"Rankings: 1D top={len(top_1d)} bot={len(bot_1d)} | 1W top={len(top_1w)} | 1M top={len(top_1m)} | 3M top={len(top_3m)}")
    log("=== Done ===")

if __name__ == "__main__":
    main()

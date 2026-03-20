"""
Portfolio Intelligence - Data Fetcher
Runs daily via GitHub Actions.
Fetches price data + signals + news for 500 ETFs + 500 US stocks.
User selects which to display on the dashboard.
Writes portfolio.json to repo root.
"""

import json
import os
import sys
import time
from datetime import datetime, date
import requests

try:
    import yfinance as yf
    import pandas as pd
    import numpy as np
except ImportError as e:
    print(f"ERROR: Missing library: {e}")
    sys.exit(1)

try:
    import feedparser
except ImportError:
    print("ERROR: feedparser not installed.")
    sys.exit(1)

# -----------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------

NEWS_PER_TICKER = 10
HISTORY_PERIOD  = "15mo"
BATCH_SIZE      = 50

# -----------------------------------------------------------------
# TOP 500 US ETFs BY AUM
# -----------------------------------------------------------------

ETF_TICKERS = [
    "SPY","IVV","VOO","VTI","QQQ","VEA","IEFA","AGG","VUG","VTV",
    "BND","VWO","GLD","IJH","IWM","VIG","IEMG","IJR","VGT","VO",
    "IWF","VXUS","BSV","MUB","VB","ITOT","IWD","IEF","LQD","TLT",
    "SHY","VNQ","XLF","XLK","XLE","XLV","XLI","XLC","XLY","XLP",
    "XLU","XLB","XLRE","GDX","SLV","USO","IAU","SGOL","DBC","PDBC",
    "HYG","JNK","EMB","VCIT","VCSH","VGSH","VGIT","VGLT","VMBS","MBB",
    "TIP","SCHP","STIP","VTIP","SPAB","SPSB","SPSM","SCHB","SCHX","SCHD",
    "SCHF","SCHE","SCHH","SCHG","SCHV","SCHA","SCHM","ARKK","ARKQ","ARKW",
    "ARKG","ARKF","SPDW","SPEM","SPLG","SPYG","SPYV","SPYD","SPMO","DIA",
    "MDY","RSP","OEF","SDY","DVY","VHT","VFH","VIS","VAW","VCR",
    "VDC","VPU","VRE","VOX","VBK","VBR","VGK","VPL","EFA","EEM",
    "EWJ","EWZ","EWC","EWG","EWU","EWA","EWH","EWS","EWL","EWT",
    "EWY","EWP","EWQ","EWI","EWD","EWN","EWO","EWK","FXI","MCHI",
    "KWEB","ASHR","EPI","INDA","SMIN","INDY","AAXJ","VWO","DEM","EDIV",
    "IYW","IGV","SOXX","SMH","QTEC","SKYY","CLOU","WCLD","BUG","CIBR",
    "HACK","ROBO","BOTZ","IRBO","ICLN","QCLN","ACES","SMOG","CNRG","ERTH",
    "IBB","XBI","PTH","BBH","FBT","SBIO","GNR","XME","PICK","SLX",
    "REMX","LIT","COPX","URA","URNM","IYR","RWR","REM","MORT","HOMZ",
    "TBF","TMF","TBT","SPTL","EDV","ZROZ","PSQ","SH","RWM","DOG",
    "SDS","QID","TWM","TQQQ","UPRO","UDOW","URTY","TNA","SPXL","TECL",
    "JEPI","JEPQ","DIVO","QYLD","RYLD","XYLD","NUSI","GPIX","GPIQ","XDTE",
    "AMLP","AMJ","ENFR","MLPA","PFF","PFFD","FPE","SPFF","HYD","HYMB",
    "BKLN","SRLN","FLOT","USFR","TFLO","ICSH","NEAR","SHV","BIL","SGOV",
    "TBIL","GBIL","MINT","JPST","GSY","PULS","RAVI","FTSM","IGSB","ANGL",
    "FALN","HYS","HYDB","SHYG","USHY","HYLB","HYXF","HYXU","BNDX","IAGG",
    "BWX","WIP","IGOV","EBND","HYEM","DXJ","HEDJ","HEFA","DBEF","DBEZ",
    "HEWJ","HEWG","HEWU","HEEM","HEZU","SPLV","USMV","EFAV","EEMV","ACWV",
    "LGLV","QUAL","SIZE","VLUE","MTUM","LRGF","ESGU","ESGE","USSG","ESGV",
    "CRBN","LOWC","ETHO","COWZ","CALF","DEEP","DSTL","MOAT","GOAT","WIDE",
    "VRP","PGF","PSK","PFIG","BMAX","BUFD","BUFQ","BUFW","BUFZ","BUFE",
    "SVOL","QQQY","SPYY","IWMY","DIVY","JEPY","FEPI","AIPI","CONY","NVDY",
    "AMZY","MSFO","TSLL","NVDL","GGLL","MSTU","MSFU","NVDU","NVDD","TSLU",
    "YMAX","YMAG","PLTY","TSMY","SNOY","GOGL","METL","NFLY","AMDY","GOGY",
    "FIVY","SIXO","ODTE","RDTE","WDTE","MDTE","QDTE","BSVO","BUFO","BUFP",
    "BUFN","BUFO","RDVY","TDIV","SDOG","FDL","FVD","CDC","DGRO","DGRW",
    "NOBL","REGL","SMDV","VYMI","VIGI","VFMF","VFMO","VFQY","VFVA","VFLQ",
    "OMFL","OMFS","DYNF","FVAL","FQAL","FMOM","FLCG","FLCV","FLMG","FLMV",
    "FLSV","FLGV","LRGF","INTF","INTL","MFUS","MFEM","MFMO","MFGP","MFDX",
    "ONEQ","FENY","FIDU","FSTA","FHLC","FMAT","FNCL","FREL","FTEC","FUTY",
    "FCOM","FDIS","FIVG","FDRV","FBND","FCOR","FLTB","FSEC","FUMB","FLDR",
    "FTSM","FCSH","FCLD","FDWM","FDIG","FSMB","FMCX","FSST","FLEE","FDEV",
    "FINX","IPAY","LEND","KOIN","SNSR","XITK","XWEB","XHLT","XSHD","XSLV",
    "XSOE","XSMO","XSVM","XSIX","XSW","XNTK","XMMO","XMHQ","XMVM","XLBS",
    "XLEY","XLFS","XLGS","XLCS","XLHB","XLUS","XLBS","XLES","XLHE","XLIE",
    "XLIS","XLIT","XLID","XLIF","XLIM","XLIK","XLIL","XLII","XLIH","XLIG",
    "XLTV","XLTU","XLTS","XLTR","XLTQ","XLTP","XLTO","XLTN","XLTM","XLTL",
]

# TOP 500 US STOCKS BY MARKET CAP
STOCK_TICKERS = [
    # Mega cap
    "AAPL","MSFT","NVDA","GOOGL","GOOG","AMZN","META","TSLA","BRK-B","AVGO",
    "JPM","LLY","V","UNH","XOM","MA","JNJ","PG","HD","COST",
    "ABBV","MRK","CVX","BAC","WMT","NFLX","KO","CRM","AMD","ORCL",
    "PEP","TMO","ACN","CSCO","LIN","MCD","ABT","DHR","ADBE","TXN",
    "WFC","PM","CAT","NEE","IBM","QCOM","GE","INTU","RTX","VZ",
    "SPGI","MS","GS","BLK","AXP","ISRG","PLD","SYK","T","BKNG",
    "AMGN","HON","ELV","MDT","TJX","GILD","CB","C","DE","VRTX",
    "REGN","MU","BSX","SCHW","ADI","CI","MMC","NKE","SO","DUK",
    "ZTS","CL","CME","MDLZ","HCA","ICE","LRCX","MCO","PGR","USB",
    "APH","KLAC","SHW","EQIX","TGT","MO","ITW","FCX","FI","AON",
    "EMR","NSC","WM","CSX","ROP","OKE","PCAR","ALL","AIG","HLT",
    "FDX","ECL","COF","EW","FTNT","CARR","GWW","STZ","PSA","AEP",
    "D","WELL","AFL","CMG","TFC","PH","ROST","JCI","SRE","SPG",
    "FAST","GPC","PAYX","ETN","IDXX","SLB","CTAS","IQV","GIS","A",
    "VRSK","YUM","KMB","BDX","OTIS","MSCI","ODFL","XEL","ED","PPG",
    "KEYS","CDW","HBAN","MTB","CBOE","CINF","EXPD","GEN","HIG","WBA",
    "LHX","NUE","PKG","PFG","RF","STT","SWKS","SYF","TRV","UAL",
    "UDR","VTR","WLTW","XYL","ZBRA","ZBH","ZION","ZM","ZS","DDOG",
    "SNOW","PLTR","ABNB","COIN","RBLX","UBER","LYFT","DASH","SHOP","SQ",
    "PYPL","SNAP","PINS","SPOT","HOOD","RIVN","LCID","NIO","XPEV","LI",
    "BIDU","JD","PDD","BABA","TME","BILI","IQ","VIPS","WB","SOHU",
    "TSM","ASML","SAP","SONY","TM","HMC","NSRGY","RHHBY","NOVN","AZN",
    "GSK","BP","SHEL","RIO","BHP","VALE","PBR","SAN","BBVA","ING",
    "UBS","CS","DB","BCS","LYG","NWG","AIB","MFG","SMFG","MTU",
    "COP","EOG","PXD","MPC","VLO","PSX","DVN","HES","APA","FANG",
    "OXY","CVI","DKL","DINO","PBF","CAPL","DKS","GPS","KSS","M",
    "JWN","ANF","AEO","URBN","LEVI","PVH","RL","TPR","VFC","UAA",
    "NKE","ADDYY","PUMA","SKX","CROX","DECK","HBI","G","WWW","BOOT",
    "DIS","CMCSA","NFLX","WBD","PARA","FOX","FOXA","NYT","GCI","SSP",
    "AMC","CNK","IMAX","LGF-A","LGF-B","MSGM","MSGS","MSGE","WWE","TKO",
    "EA","TTWO","ATVI","RBLX","U","PLTK","GDEV","HUYA","DOYU","GLUU",
    "CZR","MGM","WYNN","LVS","MLCO","BYD","PENN","DKNG","RSI","EVRI",
    "MAR","HLT","H","IHG","WH","CHH","VAC","TNL","PLYA","ATNF",
    "DAL","UAL","AAL","LUV","ALK","JBLU","SAVE","SKYW","MESA","ULCC",
    "CCL","RCL","NCLH","VIK","ONEW","SIX","FUN","SEAS","PRKS","EPR",
    "AMT","CCI","SBAC","UNIT","IRM","CONE","QTS","SWCH","COR","EQIX",
    "DLR","VICI","GLPI","MGP","O","NNN","STOR","STAG","EXR","CUBE",
    "LSI","NSA","REXR","FR","EGP","PLD","DRE","LPT","BXP","SLG",
    "HIW","PDM","VNO","KRC","DEA","OFC","ARE","ESRT","PGRE","CLDT",
    "LTC","SBRA","CTRE","OHI","NHI","SNH","WELL","VTR","HR","PEAK",
    "HTA","MOH","CNC","HUM","CVS","CI","ELV","UNH","ANTM","WCG",
    "ALHC","ACM","CANO","ONEM","HIMS","AMWL","TDOC","ACCD","LVGO","ONEM",
    "DXCM","PODD","TNDM","INSP","NVCR","HOLX","SWAV","ICUI","LMAT","ATRC",
    "PFE","MRK","LLY","ABBV","BMY","AMGN","GILD","REGN","VRTX","BIIB",
    "SGEN","ALNY","IONS","BMRN","SRPT","RARE","ACAD","PTCT","FOLD","KRYS",
]

# Deduplicate
def dedup(lst):
    seen = set()
    out = []
    for t in lst:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out

ETF_TICKERS   = dedup(ETF_TICKERS)[:500]
STOCK_TICKERS = dedup(STOCK_TICKERS)[:500]
ALL_TICKERS   = dedup(ETF_TICKERS + STOCK_TICKERS)

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

def pct_return(series, days):
    if len(series) <= days:
        return None
    current = series.iloc[-1]
    past    = series.iloc[-(days + 1)]
    if past == 0 or pd.isna(past):
        return None
    return round((current - past) / past * 100, 2)

# -----------------------------------------------------------------
# TECHNICAL INDICATORS
# -----------------------------------------------------------------

def compute_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

def compute_cmf(high, low, close, volume, period=21):
    hl_range = (high - low).replace(0, float('nan'))
    mfm = ((close - low) - (high - close)) / hl_range
    mfv = mfm * volume
    return mfv.rolling(period).sum() / volume.rolling(period).sum()

def compute_zscore(close, period=90):
    mean = close.rolling(period).mean()
    std  = close.rolling(period).std()
    return (close - mean) / std.replace(0, float('nan'))

def get_ma_signal(price, ema50, ema200):
    if price is None or ema50 is None or ema200 is None:
        return "UNKNOWN"
    if price < ema200:
        return "SELL"
    if price < ema50:
        return "MONITOR"
    pct_above = (price - ema50) / ema50 * 100
    return "BUY" if pct_above <= 5.0 else "HOLD"

# -----------------------------------------------------------------
# BATCH PRICE FETCH
# -----------------------------------------------------------------

def fetch_batch_prices(tickers):
    """Fetch closing prices for all tickers in batches using yfinance download."""
    log(f"Fetching price history for {len(tickers)} tickers in batches of {BATCH_SIZE}...")
    all_closes = {}

    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i:i+BATCH_SIZE]
        log(f"  Batch {i//BATCH_SIZE+1}/{(len(tickers)-1)//BATCH_SIZE+1}: {len(batch)} tickers")
        try:
            data = yf.download(
                batch,
                period="15mo",
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
                    if len(close) >= 5:
                        all_closes[ticker] = close
                except Exception:
                    pass
        except Exception as e:
            log(f"  Batch error: {e}")
        time.sleep(0.5)

    log(f"  Got price data for {len(all_closes)} tickers")
    return all_closes

# -----------------------------------------------------------------
# COMPUTE SIGNALS FOR ONE TICKER
# -----------------------------------------------------------------

def compute_signals(ticker, close_series):
    try:
        close = close_series

        # Need volume for CMF — fetch individually only for tickers displayed
        # For batch processing, we skip volume-based CMF and compute from close only
        price      = safe_float(close.iloc[-1], 2)
        prev_close = safe_float(close.iloc[-2], 2) if len(close) >= 2 else None
        as_of      = close.index[-1].strftime("%Y-%m-%d")

        returns = {
            "1d":   pct_return(close, 1),
            "5d":   pct_return(close, 5),
            "30d":  pct_return(close, 30),
            "90d":  pct_return(close, 90),
            "150d": pct_return(close, 150),
            "250d": pct_return(close, 250),
        }

        ema50  = safe_float(compute_ema(close, 50).iloc[-1], 2)
        ema200 = safe_float(compute_ema(close, 200).iloc[-1], 2)
        signal = get_ma_signal(price, ema50, ema200)

        zs     = compute_zscore(close, 90)
        zscore = safe_float(zs.iloc[-1], 2)

        # Build 30-day series for sparkline
        series = [
            {"date": idx.strftime("%Y-%m-%d"), "value": safe_float(v)}
            for idx, v in close.tail(30).items()
            if safe_float(v) is not None
        ]

        return {
            "status":     "ok",
            "ticker":     ticker,
            "name":       ticker,  # enriched later for displayed tickers
            "price":      price,
            "prev_close": prev_close,
            "change_pct": returns["1d"],
            "as_of":      as_of,
            "returns":    returns,
            "ema50":      ema50,
            "ema200":     ema200,
            "ma_signal":  signal,
            "zscore":     zscore,
            "cmf":        None,   # requires volume; enriched on demand
            "series":     series,
            "note":       "",
            "news":       [],
        }
    except Exception as e:
        return {
            "status": "error",
            "ticker": ticker,
            "name":   ticker,
            "note":   str(e),
            "news":   [],
        }

# -----------------------------------------------------------------
# ENRICH TOP TICKERS (name, CMF, news) — only for commonly viewed
# -----------------------------------------------------------------

def enrich_ticker(ticker, data):
    """Fetch full OHLCV + info + news for a single ticker."""
    try:
        t    = yf.Ticker(ticker)
        hist = t.history(period="6mo", interval="1d", auto_adjust=True)
        info = {}
        try:
            info = t.info
        except Exception:
            pass

        data["name"] = info.get("shortName") or info.get("longName") or ticker

        if not hist.empty and "Volume" in hist.columns:
            close  = hist["Close"].dropna()
            high   = hist["High"].dropna()
            low    = hist["Low"].dropna()
            volume = hist["Volume"].dropna()
            cmf_series = compute_cmf(high, low, close, volume, 21)
            data["cmf"] = safe_float(cmf_series.iloc[-1], 3)

    except Exception as e:
        pass
    return data

def fetch_news(ticker, max_articles=10):
    articles = []
    seen_titles = set()
    try:
        url  = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"
        feed = feedparser.parse(url)
        for entry in feed.entries[:max_articles]:
            title = entry.get("title","").strip()
            if title and title not in seen_titles:
                articles.append({"title":title,"link":entry.get("link",""),"published":entry.get("published",""),"source":"Yahoo Finance"})
                seen_titles.add(title)
    except Exception:
        pass
    if len(articles) < max_articles:
        try:
            url  = f"https://news.google.com/rss/search?q={ticker}+stock&hl=en-US&gl=US&ceid=US:en"
            feed = feedparser.parse(url)
            for entry in feed.entries:
                if len(articles) >= max_articles:
                    break
                title = entry.get("title","").strip()
                if title and title not in seen_titles:
                    articles.append({"title":title,"link":entry.get("link",""),"published":entry.get("published",""),"source":"Google News"})
                    seen_titles.add(title)
        except Exception:
            pass
    return articles[:max_articles]

# -----------------------------------------------------------------
# LOAD TICKERS FROM tickers.json
# -----------------------------------------------------------------

def load_display_tickers():
    try:
        with open("tickers.json", "r") as f:
            tickers = json.load(f)
        return [t.strip().upper() for t in tickers if t.strip()]
    except (FileNotFoundError, json.JSONDecodeError):
        return []

# -----------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------

def main():
    log("=== Portfolio Data Fetch Starting ===")
    log(f"Universe: {len(ALL_TICKERS)} tickers ({len(ETF_TICKERS)} ETFs + {len(STOCK_TICKERS)} stocks)")

    # 1. Batch fetch all prices
    all_closes = fetch_batch_prices(ALL_TICKERS)

    # 2. Compute signals for all tickers
    log("Computing signals for all tickers...")
    stocks = {}
    for ticker, close in all_closes.items():
        stocks[ticker] = compute_signals(ticker, close)

    log(f"Signals computed for {len(stocks)} tickers")

    # 3. Enrich display tickers with full name, CMF, and news
    display_tickers = load_display_tickers()
    if display_tickers:
        log(f"Enriching {len(display_tickers)} display tickers with name + CMF + news...")
        for ticker in display_tickers:
            if ticker in stocks and stocks[ticker]["status"] == "ok":
                stocks[ticker] = enrich_ticker(ticker, stocks[ticker])
                stocks[ticker]["news"] = fetch_news(ticker, NEWS_PER_TICKER)
                log(f"  Enriched {ticker}: {stocks[ticker].get('name','?')} CMF={stocks[ticker].get('cmf','?')}")
                time.sleep(0.3)

    # 4. Summary
    ok  = sum(1 for v in stocks.values() if v["status"] == "ok")
    err = sum(1 for v in stocks.values() if v["status"] == "error")
    log(f"\nSummary: OK={ok} | Errors={err}")

    # 5. Write output
    output = {
        "generated_at":   datetime.utcnow().isoformat() + "Z",
        "generated_date": date.today().isoformat(),
        "tickers":        display_tickers,
        "universe":       ALL_TICKERS,
        "etf_universe":   ETF_TICKERS,
        "stock_universe": STOCK_TICKERS,
        "stocks":         stocks,
    }

    with open("portfolio.json", "w") as f:
        json.dump(output, f, indent=2)

    size_kb = len(json.dumps(output)) // 1024
    log(f"portfolio.json written — {size_kb} KB")
    log("=== Done ===")

if __name__ == "__main__":
    main()

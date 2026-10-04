#!/usr/bin/env python3
import json
import math
import os
import re
import threading
import time
from datetime import datetime, timezone
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from email.utils import parsedate_to_datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn


CACHE = {}
CACHE_TTL = 8  # seconds


def fetch_candles(symbol: str, interval: str, limit: int = 160):
    """
    Fetch live OHLCV candles from Binance Vision (for Crypto/USDT pairs)
    or Yahoo Finance (for Stocks, Forex, Gold, Indices).
    interval: '1m', '5m', or '15m'
    """
    symbol = symbol.strip().upper()
    # Gold → PAXGUSDT (the same gold proxy the scanner grid already uses)
    _KLINE_ALIAS = {"XAUUSD": "PAXGUSDT", "GOLD": "PAXGUSDT", "XAU": "PAXGUSDT"}
    symbol = _KLINE_ALIAS.get(symbol, symbol)
    cache_key = f"{symbol}_{interval}_{limit}"
    now = time.time()
    if cache_key in CACHE and (now - CACHE[cache_key]["ts"] < CACHE_TTL):
        return CACHE[cache_key]["data"]

    candles = []
    is_crypto = symbol.endswith("USDT") or symbol.endswith("USDC") or symbol.endswith("BUSD") or symbol in ("BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "PAXG")
    if symbol in ("BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "PAXG"):
        symbol = symbol + "USDT"

    if is_crypto:
        urls = [
            f"https://data-api.binance.vision/api/v3/klines?symbol={urllib.parse.quote(symbol)}&interval={interval}&limit={limit}",
            f"https://api.binance.us/api/v3/klines?symbol={urllib.parse.quote(symbol)}&interval={interval}&limit={limit}",
        ]
        for url in urls:
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=6) as resp:
                    raw = json.loads(resp.read().decode("utf-8"))
                    if isinstance(raw, list) and len(raw) > 20:
                        for row in raw:
                            candles.append({
                                "time": int(row[0] // 1000),
                                "open": float(row[1]),
                                "high": float(row[2]),
                                "low": float(row[3]),
                                "close": float(row[4]),
                                "volume": float(row[5]),
                            })
                        break
            except Exception:
                continue

    if not candles:
        # Fallback or primary for Stocks, Forex, Gold (Yahoo Finance)
        yf_range = "1d" if interval == "1m" else ("1mo" if interval in ("1h", "60m") else "5d")
        yf_alias = {"EURUSD": "EURUSD=X", "GBPUSD": "GBPUSD=X", "AUDUSD": "AUDUSD=X", "NZDUSD": "NZDUSD=X",
                    "USDCAD": "USDCAD=X", "USDCHF": "USDCHF=X", "USDJPY": "USDJPY=X",
                    "XAUUSD": "XAUUSD=X", "XAU": "XAUUSD=X", "GOLD": "XAUUSD=X", "SILVER": "XAGUSD=X"}
        yf_sym = yf_alias.get(symbol, symbol)
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(yf_sym)}?interval={interval}&range={yf_range}"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=6) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                result = data["chart"]["result"][0]
                timestamps = result.get("timestamp", [])
                quote = result["indicators"]["quote"][0]
                opens = quote.get("open", [])
                highs = quote.get("high", [])
                lows = quote.get("low", [])
                closes = quote.get("close", [])
                vols = quote.get("volume", [])
                for i in range(len(timestamps)):
                    if closes[i] is None or opens[i] is None or highs[i] is None or lows[i] is None:
                        continue
                    candles.append({
                        "time": int(timestamps[i]),
                        "open": float(opens[i]),
                        "high": float(highs[i]),
                        "low": float(lows[i]),
                        "close": float(closes[i]),
                        "volume": float(vols[i] or 1.0),
                    })
                if len(candles) > limit:
                    candles = candles[-limit:]
        except Exception as e:
            raise RuntimeError(f"Could not fetch live market data for {symbol}: {e}")

    if not candles:
        raise RuntimeError(f"No candle data returned for {symbol}")

    CACHE[cache_key] = {"ts": now, "data": candles}
    return candles


# ============================================================================
# LIVE TICKER (seconds-fresh market price, best source per symbol)
# ============================================================================
TICKER_CACHE = {}
TICKER_TTL = 5  # seconds
_FX_BINANCE = {"EURUSD": "EURUSDT", "GBPUSD": "GBPUSDT", "AUDUSD": "AUDUSDT"}
_CRYPTO_ALTS = ("BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "PAXG", "SUI", "PEPE")


def _binance_ticker_price(bin_sym):
    url = f"https://data-api.binance.vision/api/v3/ticker/price?symbol={urllib.parse.quote(bin_sym)}"
    req = urllib.request.Request(url, headers={"User-Agent": BROWSER_UA})
    with urllib.request.urlopen(req, timeout=6) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    return float(data["price"])


def _yahoo_ticker_price(yf_symbol):
    url = (f"https://query1.finance.yahoo.com/v8/finance/chart/"
           f"{urllib.parse.quote(yf_symbol)}?interval=1m&range=1d")
    req = urllib.request.Request(url, headers={"User-Agent": BROWSER_UA})
    with urllib.request.urlopen(req, timeout=7) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    meta = data["chart"]["result"][0]["meta"]
    price = float(meta.get("regularMarketPrice") or 0)
    if price <= 0:
        raise ValueError("no price in Yahoo meta")
    return price


def fetch_ticker(symbol):
    """Return {symbol, price, source, ts} — seconds-fresh real market price."""
    s = str(symbol).strip().upper()
    now = time.time()
    ck = "tick_" + s
    hit = TICKER_CACHE.get(ck)
    if hit and (now - hit["ts"] < TICKER_TTL):
        return hit["data"]

    price, source = None, ""

    # Gold spot (true XAUUSD) — gold-api.com, updated every few seconds
    if s in ("XAUUSD", "GOLD", "XAU"):
        try:
            req = urllib.request.Request("https://api.gold-api.com/price/XAU",
                                         headers={"User-Agent": BROWSER_UA})
            with urllib.request.urlopen(req, timeout=6) as resp:
                d = json.loads(resp.read().decode("utf-8"))
            p = float(d.get("price") or 0)
            if p > 0:
                price, source = p, "XAU spot · gold-api (seconds old)"
        except Exception:
            price = None
        if price is None:  # fallback: PAXG (tokenized gold) on Binance Vision
            price = _binance_ticker_price("PAXGUSDT")
            source = "PAXGUSDT (Binance) — gold proxy"

    # FX via Binance USDT pairs, corrected for the USD (USDT peg)
    if price is None and s in _FX_BINANCE:
        raw = _binance_ticker_price(_FX_BINANCE[s])
        peg = _binance_ticker_price("USDCUSDT")  # USDT value in USD
        price = raw / peg if peg > 0 else raw
        source = "FX via Binance · real-time (USD-corrected)"

    # Pure crypto / USDT pairs
    if price is None:
        bin_sym = s
        if s in _CRYPTO_ALTS:
            bin_sym = s + "USDT"
        if bin_sym.endswith(("USDT", "USDC", "BUSD")):
            try:
                price = _binance_ticker_price(bin_sym)
                source = "Binance spot · real-time"
            except Exception:
                price = None

    # Stocks / indices / futures / other forex → Yahoo last trade
    if price is None:
        price = _yahoo_ticker_price(s)
        source = "Yahoo Finance · last trade"

    data = {"symbol": s, "price": price, "source": source, "ts": int(now * 1000)}
    TICKER_CACHE[ck] = {"ts": now, "data": data}
    return data


# ============================================================================
# WORLD NEWS WIRE — live global headlines + keyword sentiment/impact scoring
# ============================================================================
NEWS_CACHE = {"key": None, "ts": 0.0, "data": []}

_W_BULL = ["rate cut", "cuts rates", "cut rates", "beat expectation", "beats forecast",
           "upgrade", "upgraded", "surge", "soar", "rally", "record high", "stimulus",
           "dovish", "approves", "approval", "etf approval", "strong earnings",
           "raises guidance", "jump", "gain", "bullish", "all-time high", "breakthrough",
           "wins contract", "growth", "rebound", "injects", "cuts borrowing"]
_W_BEAR = ["rate hike", "raise rates", "raises rates", "miss expectation", "misses forecast",
           "downgrade", "downgraded", "crash", "plunge", "sink", "recession", "war",
           "missile", "sanctions", "hack", "breach", "ban", "lawsuit", "probe",
           "investigation", "hawkish", "weak jobs", "default", "bankruptcy", "fraud",
           "bearish", "sell-off", "selloff", "falls", "drops", "tumble", "halts",
           "suspends", "warns", "cuts forecast", "misses estimates"]
_W_IMPACT = ["federal reserve", "fomc", " central bank", "cpi", "inflation data",
             "jobs report", "nonfarm", "payrolls", "rate decision", "earnings",
             "etf", "sec ", "geopolitic", "war", "tariff", "recession", "powell",
             "binance", "hack", "exploit", "interest rate", "gdp", "unemployment",
             "stimulus", "sanctions", "earnings beat", "earnings miss", "ipo"]


def score_headline(title):
    t = (" " + title.lower() + " ")
    bull = sum(1 for k in _W_BULL if k in t)
    bear = sum(1 for k in _W_BEAR if k in t)
    sentiment = max(-3, min(3, bull - bear))
    impact = "HIGH" if any(k in t for k in _W_IMPACT) else ("MED" if sentiment != 0 else "LOW")
    return sentiment, impact


def fetch_news(queries):
    """queries: list of (category, query_text). Returns deduped, scored items."""
    key = "|".join(f"{c}:{q}" for c, q in queries)
    now = time.time()
    if NEWS_CACHE["key"] == key and (now - NEWS_CACHE["ts"] < 75):
        return NEWS_CACHE["data"]

    def one(item):
        cat, qtext = item
        url = ("https://news.google.com/rss/search?q=" + urllib.parse.quote(qtext) +
               "&hl=en-US&gl=US&ceid=US:en")
        req = urllib.request.Request(url, headers={"User-Agent": BROWSER_UA})
        with urllib.request.urlopen(req, timeout=9) as resp:
            xml = resp.read()
        root = ET.fromstring(xml)
        out = []
        for i, it in enumerate(root.iter("item")):
            if i >= 12:
                break
            title = (it.findtext("title") or "").strip()
            if not title:
                continue
            link = (it.findtext("link") or "").strip()
            pub = it.findtext("pubDate") or ""
            try:
                ts = int(parsedate_to_datetime(pub).timestamp() * 1000)
            except Exception:
                ts = int(now * 1000)
            src_el = it.find("source")
            source = (src_el.text or "").strip() if src_el is not None else ""
            sent, impact = score_headline(title)
            out.append({"title": title, "url": link, "source": source, "ts": ts,
                        "topic": cat, "sentiment": sent, "impact": impact})
        return out

    items, seen, errs = [], set(), []
    queries = queries[:8]
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = [ex.submit(one, q) for q in queries]
        for fut, q in zip(futures, queries):
            try:
                for row in fut.result():
                    kk = row["title"].lower()[:90]
                    if kk in seen:
                        continue
                    seen.add(kk)
                    items.append(row)
            except Exception as e:
                errs.append(f"{q[0]}: {e}")
    items.sort(key=lambda r: r["ts"], reverse=True)
    items = items[:60]
    NEWS_CACHE.update({"key": key, "ts": now, "data": items})
    return items


# ============================================================================
# HEALTH · ECONOMIC CALENDAR · MARKET SCAN · 24/7 BACKGROUND ALERTS
# ============================================================================
UPTIME_TS = time.time()
_CAL_CACHE = {"ts": 0.0, "data": None}
_SCAN_CACHE = {"key": None, "ts": 0.0, "data": None}
_ALERTS_CFG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "alerts_config.json")
ALERTS_CFG = {}
ALERT_STATE = {}  # "SYM|tf" -> last signal time pushed
try:
    with open(_ALERTS_CFG_PATH, "r", encoding="utf-8") as _f:
        ALERTS_CFG = json.loads(_f.read() or "{}")
except Exception:
    ALERTS_CFG = {}
if "ntfyBase" not in ALERTS_CFG:
    # fixed default topic — same codes forever (site displays insider-sniper-v4-1m/5m/15m)
    ALERTS_CFG["ntfyBase"] = "insider-sniper-v4"


def _fetch_calendar():
    now = time.time()
    if _CAL_CACHE["data"] is not None and (now - _CAL_CACHE["ts"] < 1800):
        return _CAL_CACHE["data"]
    url = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
    req = urllib.request.Request(url, headers={"User-Agent": BROWSER_UA})
    with urllib.request.urlopen(req, timeout=10) as resp:
        raw = json.loads(resp.read().decode("utf-8"))
    out = []
    lo, hi = now - 6 * 3600, now + 72 * 3600
    for e in (raw if isinstance(raw, list) else []):
        try:
            dt = datetime.fromisoformat(str(e.get("date", "")))
            ts = int(dt.timestamp())
        except Exception:
            continue
        if ts < lo or ts > hi:
            continue
        out.append({
            "title": str(e.get("title", ""))[:90],
            "country": str(e.get("country", ""))[:8],
            "ts": ts,
            "impact": str(e.get("impact", "Low")).capitalize(),
            "forecast": str(e.get("forecast", "") or ""),
            "previous": str(e.get("previous", "") or ""),
        })
    out.sort(key=lambda r: r["ts"])
    _CAL_CACHE.update({"ts": now, "data": out[:40]})
    return _CAL_CACHE["data"]


_SYM_MAP = {
    "XAUUSD": "PAXGUSDT", "GOLD": "PAXGUSDT", "XAU": "PAXGUSDT",
    "EURUSD": "EURUSDT", "GBPUSD": "GBPUSDT", "AUDUSD": "AUDUSDT",
    "BTC": "BTCUSDT", "ETH": "ETHUSDT", "SOL": "SOLUSDT",
}


def _candle_sym(sym):
    """Map display symbols to fetchable sources (same mapping the site uses)."""
    s = str(sym).strip().upper()
    return _SYM_MAP.get(s, s)


def _scan_one(sym):
    row = {"sym": sym, "price": None, "wr": "--", "tf": {}}
    try:
        t = fetch_ticker(sym)
        row["price"] = t["price"]
        row["source"] = t["source"]
    except Exception:
        pass
    csym = _candle_sym(sym)
    for tf in ("1m", "5m", "15m"):
        try:
            cands = fetch_candles(csym, tf, 150)
            cat = run_hybrid_pro_engine(cands, tf)
            at = cat.get("active_trade") or {}
            row["tf"][tf] = {
                "action": at.get("action"),
                "dir": at.get("dir"),
                "entry": at.get("entry"),
                "sl": at.get("sl"),
                "tp1": at.get("tp1"),
                "trend": cat.get("trend_dir"),
                "whale": bool(at.get("whale_confirmed")),
                "score": at.get("score"),
            }
            if tf == "5m":
                row["wr"] = cat.get("win_rate", "--")
        except Exception:
            continue
    return row


def _run_scan(symbols):
    key = ",".join(symbols)
    now = time.time()
    if _SCAN_CACHE["key"] == key and (now - _SCAN_CACHE["ts"] < 45):
        return _SCAN_CACHE["data"]
    rows = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        for row in ex.map(_scan_one, symbols):
            rows.append(row)
    _SCAN_CACHE.update({"key": key, "ts": now, "data": rows})
    return rows


def _push_ntfy(base, tf, title, body):
    topic = f"{base}-{tf}"
    req = urllib.request.Request(
        f"https://ntfy.sh/{urllib.parse.quote(topic)}",
        data=body.encode("utf-8"),
        headers={"Title": title[:120].encode("ascii", "ignore").decode("ascii"),
                 "Priority": "high",
                 "Tags": "chart_with_upwards_trend" if "BUY" in title else "chart_with_downwards_trend",
                 "User-Agent": BROWSER_UA},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=6) as r:
        return r.status


def _push_telegram(token, chat_id, text):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = json.dumps({"chat_id": chat_id, "text": text[:3900]}).encode("utf-8")
    req = urllib.request.Request(url, data=payload,
                                 headers={"Content-Type": "application/json", "User-Agent": BROWSER_UA})
    with urllib.request.urlopen(req, timeout=7) as r:
        return r.status


def _alerts_loop():
    """24/7 background scanner — pushes even when the browser is closed."""
    while True:
        try:
            ntfy_base = str(ALERTS_CFG.get("ntfyBase", "") or "").strip().lstrip("@")
            tg_token = str(ALERTS_CFG.get("telegramToken", "") or "").strip()
            tg_chat = str(ALERTS_CFG.get("telegramChatId", "") or "").strip()
            if ntfy_base or (tg_token and tg_chat):
                syms = ALERTS_CFG.get("symbols") or ["XAUUSD", "BTCUSDT", "ETHUSDT", "EURUSD"]
                for sym in syms[:8]:
                    for tf in ("1m", "5m", "15m"):
                        try:
                            cands = fetch_candles(_candle_sym(sym), tf, 130)
                            cat = run_hybrid_pro_engine(cands, tf)
                            sigs = cat.get("signals") or []
                            if not sigs:
                                continue
                            last = sigs[-1]
                            if last.get("resolved"):
                                continue
                            key = f"{sym}|{tf}"
                            seen = ALERT_STATE.get(key, 0)
                            if last.get("time", 0) <= seen:
                                continue
                            if last.get("time", 0) < time.time() - 300:
                                ALERT_STATE[key] = last.get("time", 0)
                                continue
                            ALERT_STATE[key] = last.get("time", 0)
                            act = last.get("action", "?")
                            wr = cat.get("win_rate", "--")
                            title = f"SNIPER V4 {sym} {tf} {act}"
                            body = (f"{act} {sym} @ {last.get('entry')}\n"
                                    f"SL: {last.get('sl')} | TP1: {last.get('tp1')} | TP3: {last.get('tp3')}\n"
                                    f"Whale: {'YES' if last.get('whale_confirmed') else 'no'} · score {last.get('score')}\n"
                                    f"Real WR on this chart: {'collecting' if wr == '--' else str(wr) + '%'} (not guaranteed)\n"
                                    f"Open the site to manage the trade.")
                            if ntfy_base:
                                try:
                                    _push_ntfy(ntfy_base, tf, title, body)
                                except Exception:
                                    pass
                            if tg_token and tg_chat:
                                try:
                                    _push_telegram(tg_token, tg_chat, title + "\n" + body)
                                except Exception:
                                    pass
                        except Exception:
                            continue
            time.sleep(60)
        except Exception:
            time.sleep(60)


# ============================================================================
# TECHNICAL INDICATOR & WHALE/INSIDER ENGINE (MATCHING PINE SCRIPT v5)
# ============================================================================
def calc_ema(values, length):
    if not values:
        return []
    k = 2.0 / (length + 1.0)
    out = [values[0]]
    for i in range(1, len(values)):
        out.append(values[i] * k + out[-1] * (1.0 - k))
    return out


def calc_sma(values, length):
    out = []
    s = 0.0
    for i, v in enumerate(values):
        s += v
        if i >= length:
            s -= values[i - length]
            out.append(s / length)
        else:
            out.append(s / (i + 1))
    return out


def calc_wma(values, length):
    out = []
    weights = list(range(1, length + 1))
    w_sum = sum(weights)
    for i in range(len(values)):
        if i + 1 < length:
            sub = values[: i + 1]
            w = list(range(1, len(sub) + 1))
            out.append(sum(x * y for x, y in zip(sub, w)) / sum(w))
        else:
            sub = values[i - length + 1 : i + 1]
            out.append(sum(x * y for x, y in zip(sub, weights)) / w_sum)
    return out


def calc_hma(values, length=34):
    half = max(int(length / 2), 1)
    sqrt_l = max(int(math.sqrt(length)), 1)
    wma_half = calc_wma(values, half)
    wma_full = calc_wma(values, length)
    diff = [2.0 * a - b for a, b in zip(wma_half, wma_full)]
    return calc_wma(diff, sqrt_l)


def calc_atr(candles, length=14):
    trs = []
    for i, c in enumerate(candles):
        if i == 0:
            trs.append(max(c["high"] - c["low"], 1e-9))
        else:
            pc = candles[i - 1]["close"]
            tr = max(c["high"] - c["low"], abs(c["high"] - pc), abs(c["low"] - pc))
            trs.append(max(tr, 1e-9))
    # RMA smoothing
    alpha = 1.0 / length
    out = [trs[0]]
    for i in range(1, len(trs)):
        out.append(alpha * trs[i] + (1.0 - alpha) * out[-1])
    return out


def calc_rsi(closes, length=14):
    if len(closes) < 2:
        return [50.0] * len(closes)
    gains = [0.0]
    losses = [0.0]
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0.0))
        losses.append(max(-diff, 0.0))
    alpha = 1.0 / length
    avg_g = gains[0]
    avg_l = losses[0]
    rsis = [50.0]
    for i in range(1, len(closes)):
        avg_g = alpha * gains[i] + (1.0 - alpha) * avg_g
        avg_l = alpha * losses[i] + (1.0 - alpha) * avg_l
        if avg_l < 1e-12:
            rsis.append(100.0)
        else:
            rs = avg_g / avg_l
            rsis.append(100.0 - (100.0 / (1.0 + rs)))
    return rsis


def calc_supertrend(candles, factor=2.5, atr_len=10):
    atr = calc_atr(candles, atr_len)
    upper_band = [0.0] * len(candles)
    lower_band = [0.0] * len(candles)
    st_dir = [1] * len(candles)  # 1 = Bullish, -1 = Bearish
    st_val = [candles[0]["close"]] * len(candles)

    for i, c in enumerate(candles):
        hl2 = (c["high"] + c["low"]) / 2.0
        basic_upper = hl2 + factor * atr[i]
        basic_lower = hl2 - factor * atr[i]
        if i == 0:
            upper_band[i] = basic_upper
            lower_band[i] = basic_lower
            st_dir[i] = 1
            st_val[i] = lower_band[i]
        else:
            prev_close = candles[i - 1]["close"]
            upper_band[i] = basic_upper if (basic_upper < upper_band[i - 1] or prev_close > upper_band[i - 1]) else upper_band[i - 1]
            lower_band[i] = basic_lower if (basic_lower > lower_band[i - 1] or prev_close < lower_band[i - 1]) else lower_band[i - 1]

            if st_dir[i - 1] == 1:
                if c["close"] < lower_band[i]:
                    st_dir[i] = -1
                    st_val[i] = upper_band[i]
                else:
                    st_dir[i] = 1
                    st_val[i] = lower_band[i]
            else:
                if c["close"] > upper_band[i]:
                    st_dir[i] = 1
                    st_val[i] = lower_band[i]
                else:
                    st_dir[i] = -1
                    st_val[i] = upper_band[i]
    return st_val, st_dir


def run_hybrid_pro_engine(candles, interval_label="5m"):
    n = len(candles)
    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    opens = [c["open"] for c in candles]
    vols = [max(c["volume"], 1e-9) for c in candles]

    ema21 = calc_ema(closes, 21)
    ema55 = calc_ema(closes, 55)
    hma34 = calc_hma(closes, 34)
    atr14 = calc_atr(candles, 14)
    rsi14 = calc_rsi(closes, 14)
    rsi_ma = calc_sma(rsi14, 9)
    st_val, st_dir = calc_supertrend(candles, factor=2.4, atr_len=10)

    # Volume-Weighted MACD
    pv = [c * v for c, v in zip(closes, vols)]
    vw_fast = [a / max(b, 1e-9) for a, b in zip(calc_ema(pv, 12), calc_ema(vols, 12))]
    vw_slow = [a / max(b, 1e-9) for a, b in zip(calc_ema(pv, 26), calc_ema(vols, 26))]
    vw_macd = [a - b for a, b in zip(vw_fast, vw_slow)]
    vw_sig = calc_ema(vw_macd, 9)
    vw_hist = [a - b for a, b in zip(vw_macd, vw_sig)]

    # Institutional Money Flow (CMF + OBV)
    mf_vols = []
    obv = [0.0]
    for i in range(n):
        rng = max(highs[i] - lows[i], 1e-9)
        mult = ((closes[i] - lows[i]) - (highs[i] - closes[i])) / rng
        mf_vols.append(mult * vols[i])
        if i > 0:
            if closes[i] > closes[i - 1]:
                obv.append(obv[-1] + vols[i])
            elif closes[i] < closes[i - 1]:
                obv.append(obv[-1] - vols[i])
            else:
                obv.append(obv[-1])

    cmf20 = [a / max(b, 1e-9) for a, b in zip(calc_sma(mf_vols, 20), calc_sma(vols, 20))]
    obv_fast = calc_ema(obv, 10)
    obv_slow = calc_ema(obv, 25)
    avg_vol20 = calc_sma(vols, 20)

    signals = []
    whale_events = []
    whale_zones = []
    trade_dir = 0
    active_trade = None
    total_trades = 0
    winning_trades = 0

    bull_scores = [0] * n
    bear_scores = [0] * n
    rvols = [1.0] * n

    for i in range(5, n):
        rvol = vols[i] / max(avg_vol20[i], 1e-9)
        rvols[i] = rvol
        rng = max(highs[i] - lows[i], 1e-9)
        body = abs(closes[i] - opens[i])
        lower_wick = min(opens[i], closes[i]) - lows[i]
        upper_wick = highs[i] - max(opens[i], closes[i])

        inst_bull = cmf20[i] > 0.02 or obv_fast[i] > obv_slow[i]
        inst_bear = cmf20[i] < -0.02 or obv_fast[i] < obv_slow[i]

        b_score = (
            (1 if st_dir[i] == 1 else 0)
            + (1 if closes[i] > ema55[i] else 0)
            + (1 if closes[i] > hma34[i] else 0)
            + (1 if (vw_hist[i] > 0 or vw_hist[i] > vw_hist[i - 1]) else 0)
            + (1 if (rsi14[i] > 48 and rsi14[i] >= rsi_ma[i]) else 0)
            + (1 if inst_bull else 0)
            + (1 if closes[i] > ema21[i] else 0)
        )
        s_score = (
            (1 if st_dir[i] == -1 else 0)
            + (1 if closes[i] < ema55[i] else 0)
            + (1 if closes[i] < hma34[i] else 0)
            + (1 if (vw_hist[i] < 0 or vw_hist[i] < vw_hist[i - 1]) else 0)
            + (1 if (rsi14[i] < 52 and rsi14[i] <= rsi_ma[i]) else 0)
            + (1 if inst_bear else 0)
            + (1 if closes[i] < ema21[i] else 0)
        )
        bull_scores[i] = b_score
        bear_scores[i] = s_score

        # Whale / Big Player & Stealth Insider Detection
        whale_bull_candle = (closes[i] > opens[i] and body > rng * 0.5 and closes[i] > highs[i] - rng * 0.35) or (lower_wick > rng * 0.42 and closes[i] > lows[i] + rng * 0.5)
        whale_bear_candle = (closes[i] < opens[i] and body > rng * 0.5 and closes[i] < lows[i] + rng * 0.35) or (upper_wick > rng * 0.42 and closes[i] < highs[i] - rng * 0.5)

        w_buy = rvol >= 1.85 and whale_bull_candle and (cmf20[i] >= 0 or obv[i] > obv[i - 1])
        w_sell = rvol >= 1.85 and whale_bear_candle and (cmf20[i] <= 0 or obv[i] < obv[i - 1])

        price_comp = abs(closes[i] - closes[i - 5]) / max(atr14[i] * 5.0, 1e-9) < 0.48
        stealth_buy = price_comp and cmf20[i] > 0.10 and obv_fast[i] > obv_slow[i] and rvol > 1.1
        stealth_sell = price_comp and cmf20[i] < -0.10 and obv_fast[i] < obv_slow[i] and rvol > 1.1

        if w_buy or stealth_buy:
            kind = "🐋 WHALE BUY" if w_buy else "🕵️ INSIDER ACCUM"
            whale_events.append({
                "index": i,
                "time": candles[i]["time"],
                "type": "BUY",
                "label": kind,
                "price": closes[i],
                "rvol": round(rvol, 2),
                "zone_low": lows[i],
                "zone_high": max(opens[i], closes[i]),
            })
            if w_buy:
                whale_zones.append({
                    "index": i,
                    "type": "BUY",
                    "top": max(opens[i], closes[i]),
                    "bottom": lows[i],
                    "price": closes[i],
                    "rvol": round(rvol, 2),
                })
        elif w_sell or stealth_sell:
            kind = "🐋 WHALE SELL" if w_sell else "🕵️ INSIDER DUMP"
            whale_events.append({
                "index": i,
                "time": candles[i]["time"],
                "type": "SELL",
                "label": kind,
                "price": closes[i],
                "rvol": round(rvol, 2),
                "zone_low": min(opens[i], closes[i]),
                "zone_high": highs[i],
            })
            if w_sell:
                whale_zones.append({
                    "index": i,
                    "type": "SELL",
                    "top": highs[i],
                    "bottom": min(opens[i], closes[i]),
                    "price": closes[i],
                    "rvol": round(rvol, 2),
                })

        cross_up_hma = closes[i] > hma34[i] and closes[i - 1] <= hma34[i - 1]
        cross_dn_hma = closes[i] < hma34[i] and closes[i - 1] >= hma34[i - 1]

        raw_buy = b_score >= 5 and st_dir[i] == 1 and (st_dir[i - 1] != 1 or cross_up_hma or (w_buy and closes[i] > ema55[i]))
        raw_sell = s_score >= 5 and st_dir[i] == -1 and (st_dir[i - 1] != -1 or cross_dn_hma or (w_sell and closes[i] < ema55[i]))

        # Check active trade TP1 hit for winrate
        if active_trade and not active_trade["resolved"]:
            if active_trade["dir"] == 1:
                if highs[i] >= active_trade["tp1"]:
                    active_trade["tp1_hit"] = True
                    active_trade["resolved"] = True
                    winning_trades += 1
                elif lows[i] <= active_trade["sl"]:
                    active_trade["sl_hit"] = True
                    active_trade["resolved"] = True
            else:
                if lows[i] <= active_trade["tp1"]:
                    active_trade["tp1_hit"] = True
                    active_trade["resolved"] = True
                    winning_trades += 1
                elif highs[i] >= active_trade["sl"]:
                    active_trade["sl_hit"] = True
                    active_trade["resolved"] = True

        if raw_buy and trade_dir <= 0:
            if active_trade and not active_trade["resolved"]:
                active_trade["resolved"] = True
                if (active_trade["dir"] == 1 and closes[i] > active_trade["entry"]) or (active_trade["dir"] == -1 and closes[i] < active_trade["entry"]):
                    winning_trades += 1
            trade_dir = 1
            swing_low = min(lows[max(0, i - 6) : i + 1])
            sl = min(closes[i] - atr14[i] * 1.6, swing_low - atr14[i] * 0.25)
            risk = max(closes[i] - sl, atr14[i] * 0.4)
            tp1 = closes[i] + risk * 1.0
            tp2 = closes[i] + risk * 2.2
            tp3 = closes[i] + risk * 3.8
            total_trades += 1
            active_trade = {
                "index": i,
                "time": candles[i]["time"],
                "dir": 1,
                "action": "BUY",
                "entry": closes[i],
                "sl": sl,
                "tp1": tp1,
                "tp2": tp2,
                "tp3": tp3,
                "score": b_score,
                "whale_confirmed": bool(w_buy or stealth_buy),
                "rvol": round(rvol, 2),
                "tp1_hit": False,
                "sl_hit": False,
                "resolved": False,
            }
            signals.append(active_trade)

        elif raw_sell and trade_dir >= 0:
            if active_trade and not active_trade["resolved"]:
                active_trade["resolved"] = True
                if (active_trade["dir"] == 1 and closes[i] > active_trade["entry"]) or (active_trade["dir"] == -1 and closes[i] < active_trade["entry"]):
                    winning_trades += 1
            trade_dir = -1
            swing_high = max(highs[max(0, i - 6) : i + 1])
            sl = max(closes[i] + atr14[i] * 1.6, swing_high + atr14[i] * 0.25)
            risk = max(sl - closes[i], atr14[i] * 0.4)
            tp1 = closes[i] - risk * 1.0
            tp2 = closes[i] - risk * 2.2
            tp3 = closes[i] - risk * 3.8
            total_trades += 1
            active_trade = {
                "index": i,
                "time": candles[i]["time"],
                "dir": -1,
                "action": "SELL",
                "entry": closes[i],
                "sl": sl,
                "tp1": tp1,
                "tp2": tp2,
                "tp3": tp3,
                "score": s_score,
                "whale_confirmed": bool(w_sell or stealth_sell),
                "rvol": round(rvol, 2),
                "tp1_hit": False,
                "sl_hit": False,
                "resolved": False,
            }
            signals.append(active_trade)

    last_close = closes[-1]
    last_atr = atr14[-1]
    cur_dir = trade_dir if trade_dir != 0 else (1 if closes[-1] >= ema21[-1] else -1)

    if active_trade is None:
        risk = max(last_atr * 1.5, last_close * 0.003)
        active_trade = {
            "index": n - 1,
            "time": candles[-1]["time"],
            "dir": cur_dir,
            "action": "BUY" if cur_dir == 1 else "SELL",
            "entry": last_close,
            "sl": last_close - risk if cur_dir == 1 else last_close + risk,
            "tp1": last_close + risk * 1.2 if cur_dir == 1 else last_close - risk * 1.2,
            "tp2": last_close + risk * 2.5 if cur_dir == 1 else last_close - risk * 2.5,
            "tp3": last_close + risk * 4.0 if cur_dir == 1 else last_close - risk * 4.0,
            "score": bull_scores[-1] if cur_dir == 1 else bear_scores[-1],
            "whale_confirmed": False,
            "rvol": round(rvols[-1], 2),
            "tp1_hit": False,
            "sl_hit": False,
            "resolved": False,
        }

    resolved_count = sum(1 for s in signals if s["resolved"])
    win_rate = round((winning_trades * 100.0 / resolved_count), 1) if resolved_count > 0 else "--"

    # Determine current Insider / Whale flow status
    recent_whale = whale_events[-1] if whale_events else None
    if recent_whale and (n - 1 - recent_whale["index"]) <= 4:
        whale_status = f"{recent_whale['label']} ({recent_whale['rvol']}x Vol)"
    elif cmf20[-1] > 0.05:
        whale_status = f"🏦 Institutional Accumulation (CMF +{cmf20[-1]:.2f})"
    elif cmf20[-1] < -0.05:
        whale_status = f"🏦 Institutional Distribution (CMF {cmf20[-1]:.2f})"
    else:
        whale_status = "⚖️ Balanced Order Flow"

    bars_since_signal = (n - 1) - active_trade["index"]

    return {
        "interval": interval_label,
        "market_price": last_close,
        "trend_dir": cur_dir,
        "action": "BUY NOW" if (cur_dir == 1 and bars_since_signal <= 2) else ("SELL NOW" if (cur_dir == -1 and bars_since_signal <= 2) else ("BUY / BULLISH" if cur_dir == 1 else "SELL / BEARISH")),
        "is_fresh_signal": bars_since_signal <= 2,
        "bars_since_signal": bars_since_signal,
        "active_trade": active_trade,
        "bull_score": bull_scores[-1],
        "bear_score": bear_scores[-1],
        "rsi": round(rsi14[-1], 1),
        "cmf": round(cmf20[-1], 3),
        "rvol": round(rvols[-1], 2),
        "whale_status": whale_status,
        "recent_whale": recent_whale,
        "whale_zones": whale_zones[-4:],
        "win_rate": win_rate,
        "total_trades": total_trades,
        "winning_trades": winning_trades,
        "signals": signals[-12:],
        "candles": candles[-90:],
        "ema21": ema21[-90:],
        "ema55": ema55[-90:],
    }



# ============================================================================
# EXCHANGE GATEWAY — verifies the user's OWN API keys & places real spot
# orders (Binance / Binance.US / OKX). Keys arrive per-request from the
# browser and are NEVER stored on the server.
# ============================================================================
import base64
import hashlib
import hmac
import urllib.error
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN

BINANCE_HOSTS = ["https://api.binance.com", "https://api1.binance.com", "https://api2.binance.com", "https://api3.binance.com"]
BINANCE_US_HOSTS = ["https://api.binance.us"]
OKX_BASE = "https://www.okx.com"
MEXC_BASE = "https://api.mexc.com"


BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")


def _http_json(url, method="GET", headers=None, data=None, timeout=12):
    body = None
    hdrs = dict(headers or {})
    hdrs.setdefault("User-Agent", BROWSER_UA)
    if data is not None:
        body = data if isinstance(data, bytes) else json.dumps(data).encode("utf-8")
        hdrs.setdefault("Content-Type", "application/json")
    req = urllib.request.Request(url, data=body, method=method, headers=hdrs)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode("utf-8"))
        except Exception:
            return e.code, {"error": "HTTP " + str(e.code)}
    except Exception as e:
        return 0, {"error": str(e)}


def _floor_step(qty, step):
    try:
        q = Decimal(str(qty))
        s = Decimal(str(step))
        if s <= 0:
            return str(q)
        return str((q / s).to_integral_value(rounding=ROUND_DOWN) * s)
    except Exception:
        return str(qty)


def _geo_msg(status, data):
    if status == 451:
        return ("Binance API is geo-blocked from this server (HTTP 451). "
                "Run server.py on your own computer (e.g. in the Philippines) "
                "or use OKX / Binance.US instead.")
    if status in (401, 403):
        return "Exchange rejected the keys: " + str(data.get("msg") or data.get("error") or data)
    return str(data.get("msg") or data.get("error") or ("HTTP " + str(status)))


# ------------------------------ BINANCE ------------------------------------
def _binance_hosts(exchange):
    return BINANCE_US_HOSTS if exchange == "binanceus" else BINANCE_HOSTS


def _binance_call(c, path, params, method):
    params = dict(params or {})
    params["timestamp"] = int(time.time() * 1000)
    params["recvWindow"] = 6000
    qs = urllib.parse.urlencode(params)
    sig = hmac.new(c["apiSecret"].encode("utf-8"), qs.encode("utf-8"), hashlib.sha256).hexdigest()
    headers = {"X-MBX-APIKEY": c["apiKey"]}
    last = (0, {"error": "no host reachable"}, "")
    for host in _binance_hosts(c.get("exchange")):
        status, data = _http_json(host + path + "?" + qs + "&signature=" + sig, method=method, headers=headers)
        last = (status, data, host)
        if status != 451:
            return status, data, host
    return last


def _binance_public(c, path):
    for host in _binance_hosts(c.get("exchange")):
        status, data = _http_json(host + path)
        if status == 200:
            return status, data
        if status not in (0, 451):
            return status, data
    return 0, {"error": "Binance public API unreachable (geo-blocked) from this server."}


def _binance_verify(c):
    status, data, host = _binance_call(c, "/api/v3/account", {}, "GET")
    if status != 200:
        return {"ok": False, "error": _geo_msg(status, data)}
    bals = [{"asset": b.get("asset"), "free": float(b.get("free", 0)), "locked": float(b.get("locked", 0))}
            for b in data.get("balances", []) if float(b.get("free", 0)) > 0 or float(b.get("locked", 0)) > 0]
    return {"ok": True, "host": host, "balances": bals[:24]}


def _binance_order(c, order):
    sym = str(order.get("symbol", "")).upper()
    side = str(order.get("side", "")).upper()
    if not sym or side not in ("BUY", "SELL"):
        return {"ok": False, "error": "Bad symbol/side"}
    hosts = _binance_hosts(c.get("exchange"))
    params = {"symbol": sym, "side": side, "type": "MARKET",
              "newClientOrderId": "sniper" + str(int(time.time() * 1000))}
    qty = str(order.get("qty") or "").strip()
    notional = float(order.get("notionalUsd") or 0)
    if qty:
        params["quantity"] = qty
    elif side == "BUY" and notional > 0:
        params["quoteOrderQty"] = f"{notional:.2f}"
    elif side == "SELL" and notional > 0:
        st, px = _binance_public(c, "/api/v3/ticker/price?symbol=" + sym)
        if st != 200:
            return {"ok": False, "error": _geo_msg(st, px)}
        st, info = _binance_public(c, "/api/v3/exchangeInfo?symbol=" + sym)
        if st != 200 or not info.get("symbols"):
            return {"ok": False, "error": "Could not load lot size for " + sym}
        step, min_qty = "0.01", 0.0
        for f in info["symbols"][0].get("filters", []):
            if f.get("filterType") == "LOT_SIZE":
                step = f.get("stepSize", step)
                min_qty = float(f.get("minQty", 0))
        price = float(px.get("price", 0) or 0)
        if price <= 0:
            return {"ok": False, "error": "No price for " + sym}
        calc = _floor_step(notional / price, step)
        if float(calc) < min_qty:
            return {"ok": False, "error": f"Trade size too small: {calc} {sym} < min {min_qty}"}
        params["quantity"] = calc
    else:
        return {"ok": False, "error": "Order needs qty or notionalUsd"}
    status, data = _binance_call(c, "/api/v3/order", params, "POST")
    if status != 200:
        return {"ok": False, "error": _geo_msg(status, data)}
    eq = float(data.get("cummulativeQuoteQty") or 0)
    eq_qty = float(data.get("executedQty") or 0)
    avg = data.get("avgPrice") or ""
    if (not avg or float(avg or 0) == 0) and eq_qty > 0:
        avg = f"{eq / eq_qty:.8f}"
    return {"ok": True, "orderId": str(data.get("orderId")), "qty": data.get("executedQty"),
            "avgPrice": avg, "quoteSpent": data.get("cummulativeQuoteQty")}


# --------------------------------- OKX -------------------------------------
def _okx_call(c, method, path, body_obj=None):
    body_str = json.dumps(body_obj) if body_obj is not None else ""
    now = datetime.now(timezone.utc)
    ts = now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    prehash = ts + method + path + body_str
    sign = base64.b64encode(hmac.new(c["apiSecret"].encode("utf-8"), prehash.encode("utf-8"), hashlib.sha256).digest()).decode()
    headers = {
        "OK-ACCESS-KEY": c["apiKey"],
        "OK-ACCESS-SIGN": sign,
        "OK-ACCESS-TIMESTAMP": ts,
        "OK-ACCESS-PASSPHRASE": c.get("passphrase", ""),
        "Content-Type": "application/json",
    }
    url = OKX_BASE + path
    return _http_json(url, method=method, headers=headers, data=body_obj if method == "POST" else None)


def _okx_ok(data, status):
    if status != 200:
        if status == 401:
            return False, "OKX rejected the keys (401) — check Key/Secret/Passphrase."
        if status == 403:
            return False, "OKX blocked this server (Cloudflare 403/1010). Try running server.py on your own computer."
        if status == 0:
            return False, "OKX unreachable: " + str(data.get("error", ""))
        return False, "OKX HTTP " + str(status) + ": " + str(data.get("error") or data.get("msg") or data)
    if not isinstance(data, dict) or str(data.get("code", "")) != "0":
        if isinstance(data, dict):
            msgs = "; ".join(str(e.get("msg")) for e in data.get("data", []) if isinstance(e, dict) and e.get("msg"))
            return False, (msgs or str(data.get("msg") or data.get("error") or "OKX error"))
        return False, "Unexpected OKX response"
    return True, ""


def _okx_verify(c):
    status, data = _okx_call(c, "GET", "/api/v5/account/balance?ccy=USDT")
    ok, err = _okx_ok(data, status)
    if not ok:
        return {"ok": False, "error": err}
    bals = []
    try:
        for d in data["data"][0].get("details", []):
            avail = float(d.get("availBal") or 0)
            if avail > 0:
                bals.append({"asset": d.get("ccy"), "free": avail, "locked": 0.0})
    except Exception:
        pass
    return {"ok": True, "host": OKX_BASE, "balances": bals[:24]}


def _okx_inst(sym):
    s = sym.upper()
    if s.endswith("USDT"):
        return s[:-4] + "-USDT"
    if s.endswith("USD") and len(s) > 3:
        return s[:-3] + "-USD"
    return s


def _okx_order(c, order):
    inst = _okx_inst(str(order.get("symbol", "")))
    side = str(order.get("side", "")).lower()
    if not inst or side not in ("buy", "sell"):
        return {"ok": False, "error": "Bad symbol/side"}
    qty = str(order.get("qty") or "").strip()
    notional = float(order.get("notionalUsd") or 0)
    sz = qty
    if not sz and side == "buy" and notional > 0:
        sz = f"{notional:.2f}"
    elif not sz and side == "sell" and notional > 0:
        st, tick = _http_json(OKX_BASE + "/api/v5/market/ticker?instId=" + inst)
        st2, instr = _http_json(OKX_BASE + "/api/v5/public/instruments?instType=SPOT&instId=" + inst)
        if st != 200 or not tick.get("data"):
            return {"ok": False, "error": "Could not fetch OKX price for " + inst}
        lot, min_sz = "0.01", 0.0
        if instr.get("data"):
            lot = instr["data"][0].get("lotSz", lot)
            min_sz = float(instr["data"][0].get("minSz", 0) or 0)
        price = float(tick["data"][0].get("last") or 0)
        if price <= 0:
            return {"ok": False, "error": "No price for " + inst}
        sz = _floor_step(notional / price, lot)
        if float(sz) < min_sz:
            return {"ok": False, "error": f"Trade size too small: {sz} < min {min_sz}"}
    if not sz:
        return {"ok": False, "error": "Order needs qty or notionalUsd"}
    body = {"instId": inst, "tdMode": "cash", "side": side, "ordType": "market", "sz": str(sz)}
    status, data = _okx_call(c, "POST", "/api/v5/trade/order", body)
    ok, err = _okx_ok(data, status)
    if not ok:
        return {"ok": False, "error": err}
    rec = (data.get("data") or [{}])[0]
    return {"ok": True, "orderId": str(rec.get("ordId") or ""), "qty": sz, "avgPrice": rec.get("avgPx") or "", "quoteSpent": ""}


# --------------------------------- MEXC ------------------------------------
# Spot v3 (Binance-style): HMAC-SHA256 over query string, X-MEXC-APIKEY header.
# Docs: https://www.mexc.com/api-docs/spot-v3/introduction
def _mexc_sign(c, params):
    params = dict(params or {})
    params["timestamp"] = int(time.time() * 1000)
    params["recvWindow"] = 6000
    qs = urllib.parse.urlencode(params)
    sig = hmac.new(c["apiSecret"].encode("utf-8"), qs.encode("utf-8"), hashlib.sha256).hexdigest()
    return qs + "&signature=" + sig


def _mexc_call(c, path, params, method):
    qs = _mexc_sign(c, params)
    headers = {"X-MEXC-APIKEY": c["apiKey"]}
    url = MEXC_BASE + path + "?" + qs
    return _http_json(url, method=method, headers=headers)


def _mexc_public(path):
    return _http_json(MEXC_BASE + path)


def _mexc_err(status, data):
    if status == 0:
        return "MEXC unreachable: " + str((data or {}).get("error", ""))
    if isinstance(data, dict):
        return str(data.get("message") or data.get("msg") or data.get("error") or ("HTTP " + str(status)))
    return "HTTP " + str(status)


COINEX_BASE = "https://api.coinex.com"
BLOFIN_BASE = "https://openapi.blofin.com"


def _coinex_sign(secret, method, path, body, ts):
    msg = method + path + (body or "") + str(ts)
    return hmac.new(secret.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256).hexdigest().lower()


def _coinex_call(c, method, path, body_obj=None, signed=True):
    body = ""
    if body_obj is not None:
        body = json.dumps(body_obj, separators=(",", ":"))
    ts = int(time.time() * 1000)
    headers = {"Content-Type": "application/json"}
    if signed:
        headers["X-COINEX-KEY"] = c["apiKey"]
        headers["X-COINEX-SIGN"] = _coinex_sign(c["apiSecret"], method, path, body, ts)
        headers["X-COINEX-TIMESTAMP"] = str(ts)
    data = body.encode("utf-8") if body_obj is not None else None
    return _http_json(COINEX_BASE + path, method=method, headers=headers, data=data)


def _coinex_err(status, data):
    if status == 0:
        return "CoinEx unreachable: " + str((data or {}).get("error", ""))
    if isinstance(data, dict):
        return str(data.get("message") or data.get("msg") or data.get("error") or ("HTTP " + str(status)))
    return "HTTP " + str(status)


def _coinex_verify(c):
    status, data = _coinex_call(c, "GET", "/v2/assets/spot/balance")
    if status != 200 or not isinstance(data, dict) or str(data.get("code")) not in ("0", "None"):
        if not (isinstance(data, dict) and data.get("code") == 0):
            return {"ok": False, "error": _coinex_err(status, data)}
    bals = []
    for b in (data.get("data") or []):
        if not isinstance(b, dict):
            continue
        try:
            free = float(b.get("free", 0) or 0)
            locked = float(b.get("frozen", b.get("locked", 0)) or 0)
        except Exception:
            free = locked = 0.0
        if free > 0 or locked > 0:
            bals.append({"asset": b.get("asset") or b.get("currency"), "free": free, "locked": locked})
    return {"ok": True, "host": COINEX_BASE, "balances": bals[:24]}


def _coinex_order(c, order):
    sym = str(order.get("symbol", "")).upper()
    side = str(order.get("side", "")).upper()
    if not sym or side not in ("BUY", "SELL"):
        return {"ok": False, "error": "Bad symbol/side"}
    qty = str(order.get("qty") or "").strip()
    try:
        notional = float(order.get("notionalUsd") or 0)
    except Exception:
        notional = 0.0
    amount = ""
    if qty:
        amount = qty
    elif notional > 0:
        if side == "BUY":
            amount = f"{notional:.2f}"  # CoinEx spot market BUY: amount in quote USDT
        else:
            st, tick = _coinex_call(c, "GET", "/v2/market/ticker?market=" + sym, signed=False)
            price = 0.0
            try:
                rows = tick.get("data") or []
                price = float(rows[0].get("last") or 0)
            except Exception:
                price = 0.0
            if st != 200 or price <= 0:
                return {"ok": False, "error": "Could not fetch CoinEx price for " + sym}
            amount = _floor_step(notional / price, "0.00000001")
    if not str(amount):
        return {"ok": False, "error": "Order needs qty or notionalUsd"}
    body = {"market": sym, "market_type": "SPOT", "side": side.lower(), "type": "market", "amount": str(amount)}
    status, data = _coinex_call(c, "POST", "/v2/spot/order", body)
    if status != 200 or not isinstance(data, dict) or str(data.get("code")) != "0":
        if not (isinstance(data, dict) and data.get("code") == 0):
            return {"ok": False, "error": _coinex_err(status, data)}
    d = data.get("data") or {}
    if isinstance(d, list) and d:
        d = d[0]
    return {"ok": True, "orderId": str((d or {}).get("order_id") or (d or {}).get("orderId") or ""), "host": COINEX_BASE}


def _blofin_sign(c, method, path, body, ts, nonce):
    msg = path + method + str(ts) + nonce + (body or "")
    hexsig = hmac.new(c["apiSecret"].encode("utf-8"), msg.encode("utf-8"), hashlib.sha256).hexdigest()
    return base64.b64encode(hexsig.encode("utf-8")).decode()


def _blofin_call(c, method, path, body_obj=None, signed=True):
    body = ""
    if body_obj is not None:
        body = json.dumps(body_obj, separators=(",", ":"))
    ts = int(time.time() * 1000)
    nonce = str(ts) + os.urandom(3).hex()
    headers = {"Content-Type": "application/json"}
    if signed:
        headers["ACCESS-KEY"] = c["apiKey"]
        headers["ACCESS-SIGN"] = _blofin_sign(c, method, path, body, ts, nonce)
        headers["ACCESS-TIMESTAMP"] = str(ts)
        headers["ACCESS-NONCE"] = nonce
        headers["ACCESS-PASSPHRASE"] = c.get("passphrase", "")
    data = body.encode("utf-8") if body_obj is not None else None
    return _http_json(BLOFIN_BASE + path, method=method, headers=headers, data=data)


def _blofin_err(status, data):
    if status == 0:
        return "BloFin unreachable: " + str((data or {}).get("error", ""))
    if isinstance(data, dict):
        return str(data.get("msg") or data.get("message") or data.get("error") or ("HTTP " + str(status)))
    return "HTTP " + str(status)


def _blofin_ok(status, data):
    if status != 200 or not isinstance(data, dict):
        return False
    return str(data.get("code", "0")) == "0"


def _blofin_verify(c):
    status, data = _blofin_call(c, "GET", "/api/v1/account/balance")
    if not _blofin_ok(status, data):
        return {"ok": False, "error": _blofin_err(status, data)}
    bals = []
    for b in (data.get("data") or []):
        if not isinstance(b, dict):
            continue
        try:
            free = float(b.get("available", 0) or 0)
            locked = float(b.get("frozen", b.get("hold", 0)) or 0)
        except Exception:
            free = locked = 0.0
        if free > 0 or locked > 0:
            bals.append({"asset": b.get("currency"), "free": free, "locked": locked})
    return {"ok": True, "host": BLOFIN_BASE, "balances": bals[:24]}


def _blofin_order(c, order):
    sym = str(order.get("symbol", "")).upper()
    side = str(order.get("side", "")).upper()
    if not sym or side not in ("BUY", "SELL"):
        return {"ok": False, "error": "Bad symbol/side"}
    if not sym.endswith("USDT"):
        return {"ok": False, "error": "BloFin trades USDT-margined contracts — use a *USDT symbol (got " + sym + ")."}
    inst = sym[:-4] + "-USDT"
    qty = str(order.get("qty") or "").strip()
    try:
        notional = float(order.get("notionalUsd") or 0)
    except Exception:
        notional = 0.0
    st, tick = _blofin_call(c, "GET", "/api/v1/market/ticker?instId=" + inst, signed=False)
    price = 0.0
    try:
        rows = tick.get("data") or []
        price = float(rows[0].get("last") or rows[0].get("lastPrice") or 0)
    except Exception:
        price = 0.0
    if st != 200 or price <= 0:
        return {"ok": False, "error": "Could not fetch BloFin price for " + inst + " (" + _blofin_err(st, tick) + ")"}
    meta = None
    for q in ("?instType=SWAP", ""):
        st2, instr = _blofin_call(c, "GET", "/api/v1/market/instruments" + q, signed=False)
        try:
            rows = instr.get("data") or []
        except Exception:
            rows = []
        for mrow in rows:
            if isinstance(mrow, dict) and mrow.get("instId") == inst:
                meta = mrow
                break
        if meta:
            break
    if not meta:
        return {"ok": False, "error": "BloFin instrument info unavailable for " + inst}
    try:
        ct_val = float(meta.get("ctVal") or 0)
    except Exception:
        ct_val = 0.0
    if ct_val <= 0:
        return {"ok": False, "error": "BloFin ctVal missing for " + inst}
    lot = str(meta.get("lotSz") or "1")
    try:
        min_sz = float(meta.get("minSz") or 0)
    except Exception:
        min_sz = 0.0
    ct_ccy = str(meta.get("ctValCcy") or "")
    quote_ct = ct_ccy in ("USDT", "USD", "USDC")
    if qty:
        try:
            base_amt = float(qty)
        except Exception:
            return {"ok": False, "error": "Bad qty"}
        contracts = (base_amt * price / ct_val) if quote_ct else (base_amt / ct_val)
    elif notional > 0:
        notional_per_ct = ct_val if quote_ct else ct_val * price
        contracts = notional / notional_per_ct
    else:
        return {"ok": False, "error": "Order needs qty or notionalUsd"}
    size = _floor_step(contracts, lot)
    try:
        if float(size) < max(min_sz, float(lot)):
            return {"ok": False, "error": "Trade size too small for BloFin: " + str(size) + " contracts (min " + str(min_sz) + ")"}
    except Exception:
        pass
    body = {"instId": inst, "side": side.lower(), "orderType": "market", "size": str(size),
            "marginMode": "cross", "leverage": "1", "positionSide": "net"}
    status, data = _blofin_call(c, "POST", "/api/v1/trade/order", body)
    if not _blofin_ok(status, data):
        return {"ok": False, "error": _blofin_err(status, data)}
    d = data.get("data") or {}
    if isinstance(d, list) and d:
        d = d[0]
    return {"ok": True, "orderId": str((d or {}).get("orderId") or ""), "host": BLOFIN_BASE}


def _mexc_verify(c):
    status, data = _mexc_call(c, "/api/v3/account", {}, "GET")
    if status != 200 or not isinstance(data, dict) or "balances" not in data:
        return {"ok": False, "error": _mexc_err(status, data)}
    bals = []
    for b in data.get("balances", []):
        try:
            free = float(b.get("free", 0) or 0)
            locked = float(b.get("locked", 0) or 0)
        except Exception:
            free = locked = 0.0
        if free > 0 or locked > 0:
            bals.append({"asset": b.get("asset"), "free": free, "locked": locked})
    return {"ok": True, "host": MEXC_BASE, "balances": bals[:24]}


def _mexc_order(c, order):
    sym = str(order.get("symbol", "")).upper()
    side = str(order.get("side", "")).upper()
    if not sym or side not in ("BUY", "SELL"):
        return {"ok": False, "error": "Bad symbol/side"}
    params = {"symbol": sym, "side": side, "type": "MARKET",
              "newClientOrderId": "sniper" + str(int(time.time() * 1000))}
    qty = str(order.get("qty") or "").strip()
    notional = 0.0
    try:
        notional = float(order.get("notionalUsd") or 0)
    except Exception:
        notional = 0.0
    if qty:
        params["quantity"] = qty
    elif side == "BUY" and notional > 0:
        params["quoteOrderQty"] = f"{notional:.2f}"
    elif side == "SELL" and notional > 0:
        st, px = _mexc_public("/api/v3/ticker/price?symbol=" + sym)
        price = 0.0
        try:
            price = float((px or {}).get("price", 0) or 0)
        except Exception:
            price = 0.0
        if st != 200 or price <= 0:
            return {"ok": False, "error": "Could not fetch MEXC price for " + sym + " (" + _mexc_err(st, px) + ")"}
        step, min_qty = "0.01", 0.0
        st2, info = _mexc_public("/api/v3/exchangeInfo?symbol=" + sym)
        try:
            syms = (info or {}).get("symbols") or []
            if syms:
                for f in (syms[0].get("filters") or []):
                    if f.get("filterType") == "LOT_SIZE":
                        step = f.get("stepSize", step)
                        min_qty = float(f.get("minQty", 0) or 0)
        except Exception:
            pass
        calc = _floor_step(notional / price, step)
        try:
            if float(calc) < min_qty:
                return {"ok": False, "error": f"Trade size too small: {calc} {sym} < min {min_qty}"}
        except Exception:
            pass
        params["quantity"] = calc
    else:
        return {"ok": False, "error": "Order needs qty or notionalUsd"}
    status, data = _mexc_call(c, "/api/v3/order", params, "POST")
    if status != 200 or not isinstance(data, dict) or "orderId" not in data:
        return {"ok": False, "error": _mexc_err(status, data)}
    avg = ""
    try:
        eq = float(data.get("cummulativeQuoteQty") or 0)
        eq_qty = float(data.get("executedQty") or 0)
        avg = str(data.get("avgPrice") or "")
        if (not avg or float(avg or 0) == 0) and eq_qty > 0:
            avg = f"{eq / eq_qty:.8f}"
    except Exception:
        avg = ""
    return {"ok": True, "orderId": str(data.get("orderId")),
            "qty": data.get("executedQty"), "avgPrice": avg,
            "quoteSpent": data.get("cummulativeQuoteQty")}


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


class RequestHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def _send_json(self, payload, status=200):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        qs = urllib.parse.parse_qs(parsed.query)

        if path == "/" or path == "/index.html":
            index_path = os.path.join(os.path.dirname(__file__), "index.html")
            with open(index_path, "rb") as f:
                content = f.read()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)
            return

        if path == "/api/klines":
            symbol = qs.get("symbol", ["BTCUSDT"])[0].upper()
            interval = qs.get("interval", ["5m"])[0]
            try:
                try:
                    _lim = max(20, min(1000, int(qs.get("limit", ["250"])[0])))
                except Exception:
                    _lim = 250
                raw_candles = fetch_candles(symbol, interval, _lim)
                enriched = []
                for c in raw_candles:
                    rng = max(c["high"] - c["low"], 1e-9)
                    buy_pct = ((c["close"] - c["low"]) / rng) * 100.0
                    vol = max(c["volume"], 1.0)
                    tb = vol * (buy_pct / 100.0)
                    ts = vol - tb
                    enriched.append({
                        "time": c["time"],
                        "open": c["open"],
                        "high": c["high"],
                        "low": c["low"],
                        "close": c["close"],
                        "volume": vol,
                        "usd_vol": vol * c["close"],
                        "avg_trade_usd": max((vol * c["close"]) / 500.0, 100.0),
                        "taker_buy": tb,
                        "taker_sell": ts,
                        "delta": tb - ts,
                        "buy_pct": buy_pct,
                    })
                self._send_json({"ok": True, "candles": enriched})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=400)
            return

        if path == "/api/analyze":
            symbol = qs.get("symbol", ["BTCUSDT"])[0].upper()
            try:
                c1 = fetch_candles(symbol, "1m", 150)
                c5 = fetch_candles(symbol, "5m", 150)
                c15 = fetch_candles(symbol, "15m", 150)

                cat1 = run_hybrid_pro_engine(c1, "1m")
                cat2 = run_hybrid_pro_engine(c5, "5m")
                cat3 = run_hybrid_pro_engine(c15, "15m")

                triple_bull = cat1["trend_dir"] == 1 and cat2["trend_dir"] == 1 and cat3["trend_dir"] == 1
                triple_bear = cat1["trend_dir"] == -1 and cat2["trend_dir"] == -1 and cat3["trend_dir"] == -1

                self._send_json({
                    "ok": True,
                    "symbol": symbol,
                    "timestamp": int(time.time()),
                    "market_price": cat1["market_price"],
                    "triple_aligned": "STRONG BUY (ALL 3 ALIGNED: 1m + 5m + 15m)" if triple_bull else ("STRONG SELL (ALL 3 ALIGNED: 1m + 5m + 15m)" if triple_bear else "MIXED / FOLLOW INDIVIDUAL CATEGORY"),
                    "categories": {
                        "1m": cat1,
                        "5m": cat2,
                        "15m": cat3,
                    }
                })
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=400)
            return

        if path == "/api/pine":
            pine_path = "/home/user/Institutional_Insider_Hybrid_Pro.pine"
            with open(pine_path, "r", encoding="utf-8") as f:
                code = f.read()
            self._send_json({"ok": True, "code": code})
            return

        if path == "/api/ticker":
            symbol = qs.get("symbol", ["BTCUSDT"])[0]
            try:
                self._send_json({"ok": True, **fetch_ticker(symbol)})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=400)
            return

        if path == "/api/news":
            raw_q = qs.get("q", [""])[0]
            queries = []
            for chunk in raw_q.split("|"):
                chunk = chunk.strip()
                if ":" in chunk:
                    cat, qtext = chunk.split(":", 1)
                    cat, qtext = cat.strip()[:12], qtext.strip()[:80]
                    if cat and qtext:
                        queries.append((cat, qtext))
            if not queries:
                queries = [("macro", "federal reserve interest rate"),
                           ("stocks", "stock market today"),
                           ("gold", "gold price"),
                           ("crypto", "bitcoin")]
            try:
                items = fetch_news(queries)
                self._send_json({"ok": True, "ts": int(time.time()), "items": items})
            except Exception as e:
                self._send_json({"ok": True, "ts": int(time.time()), "items": [], "error": str(e)})
            return

        if path == "/api/health":
            self._send_json({
                "ok": True,
                "uptime_sec": int(time.time() - UPTIME_TS),
                "server_time": int(time.time() * 1000),
                "alerts247": bool(ALERTS_CFG.get("ntfyBase") or ALERTS_CFG.get("telegramToken")),
            })
            return

        if path == "/api/calendar":
            try:
                self._send_json({"ok": True, "ts": int(time.time()), "events": _fetch_calendar()})
            except Exception as e:
                self._send_json({"ok": True, "ts": int(time.time()), "events": _CAL_CACHE["data"] or [], "error": str(e)})
            return

        if path == "/api/scan":
            raw_syms = qs.get("symbols", [""])[0]
            syms = [s.strip().upper() for s in raw_syms.split(",") if s.strip()][:10] or ["XAUUSD", "BTCUSDT", "ETHUSDT", "EURUSD", "NVDA"]
            try:
                self._send_json({"ok": True, "ts": int(time.time()), "rows": _run_scan(syms)})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=400)
            return

        if path == "/manifest.json":
            p = os.path.join(os.path.dirname(__file__), "manifest.json")
            if os.path.exists(p):
                with open(p, "rb") as f:
                    content = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "application/manifest+json; charset=utf-8")
                self.send_header("Content-Length", str(len(content)))
                self.end_headers()
                self.wfile.write(content)
                return

        if path in ("/icon-192.png", "/icon-512.png"):
            p = os.path.join(os.path.dirname(__file__), os.path.basename(path))
            if os.path.exists(p):
                with open(p, "rb") as f:
                    content = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "image/png")
                self.send_header("Content-Length", str(len(content)))
                self.end_headers()
                self.wfile.write(content)
                return

        if path == "/sw.js":
            p = os.path.join(os.path.dirname(__file__), "sw.js")
            if os.path.exists(p):
                with open(p, "rb") as f:
                    content = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "text/javascript; charset=utf-8")
                self.send_header("Content-Length", str(len(content)))
                self.end_headers()
                self.wfile.write(content)
                return

        self._send_json({"ok": False, "error": "Not found"}, status=404)

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)

        if parsed.path == "/api/exchange":
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length).decode("utf-8") if length > 0 else "{}"
            try:
                data = json.loads(raw)
            except Exception:
                self._send_json({"ok": False, "error": "Bad JSON"})
                return
            action = data.get("action", "verify")
            exch = str(data.get("exchange", "")).lower()
            c = {
                "apiKey": str(data.get("apiKey", "")).strip(),
                "apiSecret": str(data.get("apiSecret", "")).strip(),
                "passphrase": str(data.get("passphrase", "")).strip(),
                "exchange": exch,
            }
            if not c["apiKey"] or not c["apiSecret"]:
                self._send_json({"ok": False, "error": "API key / secret are required."})
                return
            if exch in ("binance", "binanceus"):
                if action == "verify":
                    res = _binance_verify(c)
                else:
                    res = _binance_order(c, data.get("order", {}))
            elif exch == "okx":
                if action == "verify":
                    res = _okx_verify(c)
                else:
                    res = _okx_order(c, data.get("order", {}))
            elif exch == "mexc":
                if action == "verify":
                    res = _mexc_verify(c)
                else:
                    res = _mexc_order(c, data.get("order", {}))
            elif exch == "coinex":
                if action == "verify":
                    res = _coinex_verify(c)
                else:
                    res = _coinex_order(c, data.get("order", {}))
            elif exch == "blofin":
                if action == "verify":
                    res = _blofin_verify(c)
                else:
                    res = _blofin_order(c, data.get("order", {}))
            else:
                res = {"ok": False, "error": "Server-side orders support Binance / Binance.US / OKX / MEXC / CoinEx / BloFin. MetaTrader uses the EA bridge."}
            self._send_json(res)
            return

        if parsed.path == "/api/webhook":
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length).decode("utf-8") if length > 0 else "{}"
            data = json.loads(raw)
            webhook_url = data.get("webhook_url", "").strip()
            tg_token = data.get("tg_token", "").strip()
            tg_chat = data.get("tg_chat_id", "").strip()
            message = data.get("message", "Test Alert")

            results = []
            if webhook_url:
                try:
                    payload = json.dumps({"content": message, "text": message}).encode("utf-8")
                    req = urllib.request.Request(webhook_url, data=payload, headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"})
                    with urllib.request.urlopen(req, timeout=5) as r:
                        results.append(f"Webhook sent ({r.status})")
                except Exception as e:
                    results.append(f"Webhook error: {e}")

            if tg_token and tg_chat:
                try:
                    tg_url = f"https://api.telegram.org/bot{tg_token}/sendMessage"
                    payload = json.dumps({"chat_id": tg_chat, "text": message}).encode("utf-8")
                    req = urllib.request.Request(tg_url, data=payload, headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"})
                    with urllib.request.urlopen(req, timeout=5) as r:
                        results.append(f"Telegram sent ({r.status})")
                except Exception as e:
                    results.append(f"Telegram error: {e}")

            self._send_json({"ok": True, "results": results})
            return

        if parsed.path == "/api/alerts/config":
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length).decode("utf-8") if length > 0 else "{}"
            try:
                data = json.loads(raw)
            except Exception:
                data = {}
            global ALERTS_CFG
            if "ntfyBase" in data:
                ALERTS_CFG["ntfyBase"] = str(data.get("ntfyBase") or "").strip().lstrip("@")[:80]
            if "telegramToken" in data:
                ALERTS_CFG["telegramToken"] = str(data.get("telegramToken") or "").strip()[:80]
            if "telegramChatId" in data:
                ALERTS_CFG["telegramChatId"] = str(data.get("telegramChatId") or "").strip()[:40]
            if "symbols" in data and isinstance(data["symbols"], list):
                ALERTS_CFG["symbols"] = [str(s).upper()[:16] for s in data["symbols"][:8]]
            try:
                with open(_ALERTS_CFG_PATH, "w", encoding="utf-8") as f:
                    f.write(json.dumps(ALERTS_CFG))
            except Exception:
                pass
            self._send_json({
                "ok": True,
                "enabled": bool(ALERTS_CFG.get("ntfyBase") or ALERTS_CFG.get("telegramToken")),
                "ntfy": bool(ALERTS_CFG.get("ntfyBase")),
                "telegram": bool(ALERTS_CFG.get("telegramToken") and ALERTS_CFG.get("telegramChatId")),
                "symbols": ALERTS_CFG.get("symbols") or [],
            })
            return

        self._send_json({"ok": False, "error": "Not found"}, status=404)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    server = ThreadedHTTPServer(("0.0.0.0", port), RequestHandler)
    t = threading.Thread(target=_alerts_loop, daemon=True)
    t.start()
    print(f"Free Live Notification Server listening on http://0.0.0.0:{port} (24/7 alerts thread: on)", flush=True)
    server.serve_forever()

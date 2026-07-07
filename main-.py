"""
Deriv Synthetic Indices SMC+ICT Scanner — OB + FVG Confluence Model
Pairs: V10, V25, V75, V75(1s)
Runs 24/7 on Railway.app

Entry Logic (all must align):
  H4  — Trend bias (EMA 21/50) + ADX strength
  H1  — Unmitigated fresh Order Block (last 20 bars)
  M15 — Fair Value Gap overlapping inside the H1 OB zone + price enters the
        confluence zone + rejection wick + RSI + momentum (entry confirmation
        now happens on the same M15 candle as the FVG check, no separate M5 step)

Trade Plan:
  Entry  = M15 close inside OB+FVG zone
  SL     = Below/above OB wick + ATR buffer
  TP1    = 1:1 (close 50%, move SL to BE)
  TP2    = Dynamic 1:RR based on signal score
  Cooldown = 4 hours per level
"""

import asyncio
import json
import logging
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple

import pandas as pd
import websockets

# =============================================================================
#  LOGGING
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

WAT = timezone(timedelta(hours=1))


# =============================================================================
#  CONFIG — environment variables + constants
# =============================================================================
TG_TOKEN   = os.environ.get("TG_TOKEN",   "")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")

# Deriv synthetic index symbol mapping: { display_name: deriv_symbol }
# Standard volatility indices use "R_" prefix; the 1s-tick variants use
# the "1HZ..V" naming convention.
SYMBOL_MAP = {
    "V10":     "R_10",
    "V25":     "R_25",
    "V75":     "R_75",
    "V75(1s)": "1HZ75V",
}

WS_URI         = "wss://ws.derivws.com/websockets/v3?app_id=1089"
SCAN_INTERVAL  = 300       # scan every 5 minutes
COOLDOWN_SECS  = 14400     # 4-hour cooldown per signal key
COOLDOWN_FILE  = "cooldown.json"

# Timeframes
H4_TF  = 14400; H4_COUNT  = 100
H1_TF  = 3600;  H1_COUNT  = 100
M15_TF = 900;   M15_COUNT = 150   # now doubles as FVG timeframe AND entry timeframe

# Order Block settings
OB_LOOKBACK  = 30
OB_MIN_BODY  = 0.4
OB_MAX_AGE   = 20
OB_PROXIMITY = 1.0

# FVG settings
FVG_MIN_PCT  = 0.02

# Signal filters
ADX_MIN  = 18.0
RSI_OB   = 75.0
RSI_OS   = 25.0
BODY_MIN = 0.35
MIN_WICK = 0.25
ATR_BUF  = 0.5

# Risk/Reward
RR_MIN = 1.5
RR_MAX = 3.0

# Scoring / rating thresholds (score out of 9)
SCORE_MAX      = 9
RATING_PRIME   = 7
RATING_STRONG  = 5
RATING_GOOD    = 3

# Decimal precision per pair for display/rounding.
# Deriv volatility indices are typically quoted to 2 decimal places.
DECIMALS = {
    "V10":     2,
    "V25":     2,
    "V75":     2,
    "V75(1s)": 2,
}


# =============================================================================
#  SYMBOL VERIFICATION — confirms Deriv symbol strings exist before scanning
# =============================================================================
async def verify_symbols() -> dict:
    """
    Queries Deriv's active_symbols endpoint and checks that every symbol
    in SYMBOL_MAP actually exists. Returns the subset of SYMBOL_MAP that
    is confirmed valid. Logs warnings (with closest matches) for any
    symbol that isn't found.
    """
    valid_map = {}
    try:
        async with websockets.connect(WS_URI, ping_timeout=15, open_timeout=20) as ws:
            await ws.send(json.dumps({
                "active_symbols": "brief",
                "product_type": "basic",
            }))
            raw  = await asyncio.wait_for(ws.recv(), timeout=20)
            resp = json.loads(raw)

            if "error" in resp:
                log.error("active_symbols error: %s", resp["error"].get("message"))
                log.warning("Skipping verification — using SYMBOL_MAP as-is.")
                return dict(SYMBOL_MAP)

            all_symbols = {s["symbol"]: s.get("display_name", "")
                           for s in resp.get("active_symbols", [])}

            for display_name, deriv_symbol in SYMBOL_MAP.items():
                if deriv_symbol in all_symbols:
                    valid_map[display_name] = deriv_symbol
                    log.info("Verified %-8s -> %s (%s)",
                             display_name, deriv_symbol, all_symbols[deriv_symbol])
                else:
                    candidates = [
                        sym for sym, name in all_symbols.items()
                        if display_name.upper() in name.upper()
                        or display_name.replace("(1s)", "").upper() in sym.upper()
                    ]
                    log.warning(
                        "Symbol NOT FOUND: %s (tried '%s'). Possible matches: %s",
                        display_name, deriv_symbol,
                        ", ".join(candidates[:5]) if candidates else "none found"
                    )

            if not valid_map:
                log.error("No symbols verified! Falling back to SYMBOL_MAP as-is — scans may fail.")
                return dict(SYMBOL_MAP)

            return valid_map

    except Exception as e:
        log.error("Symbol verification failed: %s. Using SYMBOL_MAP as-is.", e)
        return dict(SYMBOL_MAP)


# =============================================================================
#  COOLDOWN — file-backed persistence
# =============================================================================
def _load_cooldown() -> dict:
    try:
        if os.path.exists(COOLDOWN_FILE):
            with open(COOLDOWN_FILE, "r") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return data
    except (json.JSONDecodeError, OSError) as e:
        log.warning("Cooldown file unreadable (%s). Starting fresh.", e)
    return {}


def _save_cooldown(state: dict) -> None:
    try:
        tmp = COOLDOWN_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(state, f)
        os.replace(tmp, COOLDOWN_FILE)
    except OSError as e:
        log.error("Failed to save cooldown file: %s", e)


def is_duplicate(key: str) -> bool:
    state = _load_cooldown()
    ts = state.get(key)
    if ts and (time.time() - ts) < COOLDOWN_SECS:
        remaining = int(COOLDOWN_SECS - (time.time() - ts)) // 60
        log.info("Cooldown active for %s — %d min remaining.", key, remaining)
        return True
    return False


def mark_sent(key: str) -> None:
    state = _load_cooldown()
    state[key] = time.time()
    cutoff = time.time() - 86400
    state = {k: v for k, v in state.items() if v > cutoff}
    _save_cooldown(state)
    log.info("Cooldown set for key: %s", key)


def build_cooldown_key(display_symbol: str, signal: str, zone_mid: float, decimals: int) -> str:
    """Rounds the zone midpoint to the pair's display precision so that
    re-entries into essentially the same zone don't bypass cooldown due to
    floating point noise."""
    return f"{display_symbol}_{signal}_{round(zone_mid, decimals)}"


# =============================================================================
#  TELEGRAM
# =============================================================================
def send_telegram(message: str) -> None:
    if not TG_TOKEN or not TG_CHAT_ID:
        log.warning("Telegram not configured — TG_TOKEN or TG_CHAT_ID missing.")
        return
    try:
        url  = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id":    TG_CHAT_ID,
            "text":       message,
            "parse_mode": "HTML",
        }).encode()
        req  = urllib.request.Request(url, data=data)
        urllib.request.urlopen(req, timeout=10)
        log.info("Telegram alert sent.")
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        log.error("Telegram HTTP error %d: %s", e.code, body)
    except Exception as e:
        log.error("Telegram send failed: %s", e)


def build_alert(
    display_symbol, signal, entry, sl, tp1, tp2, risk, rr,
    score, rating, h4_bias, ob, fvg, zone, rsi_val, now, decimals,
) -> str:
    icon  = "🟢 <b>BUY (LONG)</b>"  if signal == "BUY" else "🔴 <b>SELL (SHORT)</b>"
    stars = (
        "🔥 PRIME"    if rating == "PRIME"  else
        "⭐⭐ STRONG" if rating == "STRONG" else
        "⭐ GOOD"     if rating == "GOOD"   else "✗ SKIP"
    )
    div = "—" * 20
    fmt = f"%.{decimals}f"
    return (
        f"{icon}  &#8212;  <b>OB + FVG Confluence</b>\n"
        f"<code>{div}</code>\n"
        f"<b>Pair:</b>    {display_symbol}\n"
        f"<b>Rating:</b>  {stars}  ({score}/9)\n"
        f"<b>H4 Bias:</b> {h4_bias}\n"
        f"<b>Time:</b>    {now}\n"
        f"<code>{div}</code>\n"
        f"<b>Entry:</b>   {fmt % entry}\n"
        f"<b>SL:</b>      {fmt % sl}\n"
        f"<b>TP1:</b>     {fmt % tp1}  <i>(close 50%, move SL to BE)</i>\n"
        f"<b>TP2:</b>     {fmt % tp2}  <i>(1:{rr} RR)</i>\n"
        f"<b>Risk/pt:</b> {fmt % risk}\n"
        f"<code>{div}</code>\n"
        f"<b>H1 OB Zone:</b>   {fmt % ob['lo']} – {fmt % ob['hi']}\n"
        f"<b>M15 FVG Zone:</b> {fmt % fvg['lo']} – {fmt % fvg['hi']}\n"
        f"<b>Entry Zone:</b>   {fmt % zone['lo']} – {fmt % zone['hi']}\n"
        f"<b>RSI(14):</b>      {round(rsi_val, 1)}\n"
        f"<code>{div}</code>\n"
        f"<i>H4 trend + H1 OB + M15 FVG + M15 entry all confirmed.</i>"
    )


# =============================================================================
#  WEBSOCKET — candle fetcher
# =============================================================================
async def fetch_candles(deriv_symbol: str, granularity: int, count: int) -> Optional[pd.DataFrame]:
    try:
        async with websockets.connect(WS_URI, ping_timeout=15, open_timeout=20) as ws:
            await ws.send(json.dumps({
                "ticks_history":   deriv_symbol,
                "adjust_start_time": 1,
                "count":           count,
                "end":             "latest",
                "style":           "candles",
                "granularity":     granularity,
            }))
            raw  = await asyncio.wait_for(ws.recv(), timeout=20)
            resp = json.loads(raw)

            if "error" in resp:
                log.error("Deriv API error for %s (%ds): %s",
                          deriv_symbol, granularity, resp["error"].get("message", "unknown"))
                return None

            candles = resp.get("candles")
            if not candles:
                log.error("No candle data returned for %s (%ds).", deriv_symbol, granularity)
                return None

            df = pd.DataFrame(candles)
            df.rename(columns={"open": "Open", "high": "High",
                                "low": "Low",  "close": "Close"}, inplace=True)
            for col in ["Open", "High", "Low", "Close"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df["Time"] = pd.to_datetime(df["epoch"], unit="s", utc=True)
            df.set_index("Time", inplace=True)
            df.drop(columns=["epoch"], inplace=True)

            if df.isnull().any().any():
                log.warning("%s (%ds): NaN values present after parse.", deriv_symbol, granularity)

            return df

    except asyncio.TimeoutError:
        log.error("%s (%ds): WebSocket recv timed out.", deriv_symbol, granularity)
    except websockets.exceptions.WebSocketException as e:
        log.error("%s (%ds): WebSocket error — %s", deriv_symbol, granularity, e)
    except Exception as e:
        log.error("%s (%ds): Unexpected fetch error — %s", deriv_symbol, granularity, e)
    return None


async def fetch_closed_candles(deriv_symbol: str, granularity: int, count: int) -> Optional[pd.DataFrame]:
    """Wrapper around fetch_candles that drops the final still-forming candle,
    per established pattern (closed-only candles, iloc[:-1])."""
    df = await fetch_candles(deriv_symbol, granularity, count + 1)
    if df is None or len(df) < 2:
        return df
    return df.iloc[:-1]


# =============================================================================
#  INDICATORS
# =============================================================================
def ema(series: pd.Series, n: int) -> pd.Series:
    return series.ewm(span=n, adjust=False).mean()


def atr_series(df: pd.DataFrame, n: int = 14) -> pd.Series:
    tr = pd.concat([
        df["High"] - df["Low"],
        (df["High"] - df["Close"].shift(1)).abs(),
        (df["Low"]  - df["Close"].shift(1)).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(span=n, adjust=False).mean()


def body_ratio(df: pd.DataFrame) -> pd.Series:
    rng = (df["High"] - df["Low"]).replace(0.0, float("nan"))
    return (df["Close"] - df["Open"]).abs() / rng


def calc_rsi(series: pd.Series, n: int = 14) -> pd.Series:
    delta = series.diff()
    gain  = delta.clip(lower=0).ewm(span=n, adjust=False).mean()
    loss  = (-delta.clip(upper=0)).ewm(span=n, adjust=False).mean()
    rs    = gain / loss.replace(0.0, float("nan"))
    return 100 - (100 / (1 + rs))


def calc_adx(df: pd.DataFrame, n: int = 14) -> pd.Series:
    up   = df["High"].diff()
    down = -df["Low"].diff()
    pdm  = pd.Series(0.0, index=df.index)
    ndm  = pd.Series(0.0, index=df.index)
    pdm[up > down]   = up[up > down].clip(lower=0)
    ndm[down > up]   = down[down > up].clip(lower=0)
    atr_ = atr_series(df, n)
    safe = atr_.replace(0.0, float("nan"))
    pdi  = 100 * pdm.ewm(span=n, adjust=False).mean() / safe
    ndi  = 100 * ndm.ewm(span=n, adjust=False).mean() / safe
    denom = (pdi + ndi).replace(0.0, float("nan"))
    dx   = 100 * (pdi - ndi).abs() / denom
    return dx.ewm(span=n, adjust=False).mean()


# =============================================================================
#  LAYER 1 — H4 TREND BIAS
# =============================================================================
def get_h4_bias(h4: pd.DataFrame) -> Tuple[str, float]:
    df = h4.copy()
    df["E21"] = ema(df["Close"], 21)
    df["E50"] = ema(df["Close"], 50)
    df["ADX"] = calc_adx(df, 14)

    cur  = df.iloc[-1]
    prev = df.iloc[-3]
    adx_val = float(cur["ADX"])

    bullish = (
        cur["E21"]   > cur["E50"]
        and cur["Close"] > cur["E21"]
        and cur["E21"]   > prev["E21"]
        and cur["E50"]   > prev["E50"]
    )
    bearish = (
        cur["E21"]   < cur["E50"]
        and cur["Close"] < cur["E21"]
        and cur["E21"]   < prev["E21"]
        and cur["E50"]   < prev["E50"]
    )

    if bullish:
        return "BULLISH", adx_val
    if bearish:
        return "BEARISH", adx_val
    return "NEUTRAL", adx_val


# =============================================================================
#  LAYER 2 — H1 ORDER BLOCK (fresh + unmitigated)
# =============================================================================
def find_ob(h1: pd.DataFrame, bias: str) -> Optional[dict]:
    df = h1.copy()
    df["BR"]      = body_ratio(df)
    df["IsBull"]  = df["Close"] > df["Open"]
    df["IsBear"]  = df["Close"] < df["Open"]
    df["BullMSS"] = df["Close"] > df["High"].shift(1).rolling(5).max()
    df["BearMSS"] = df["Close"] < df["Low"].shift(1).rolling(5).min()

    lookback = df.iloc[-OB_LOOKBACK:]

    if bias == "BULLISH":
        mss_candles = lookback[lookback["BullMSS"]].index.tolist()
        for mss_idx in reversed(mss_candles):
            pool = lookback.loc[:mss_idx].iloc[:-1]
            pool = pool[pool["IsBear"] & (pool["BR"] >= OB_MIN_BODY)]
            if pool.empty:
                continue
            ob_row = pool.iloc[-1]
            hi     = max(float(ob_row["Open"]), float(ob_row["Close"]))
            lo     = min(float(ob_row["Open"]), float(ob_row["Close"]))
            wick   = float(ob_row["Low"])
            post_ob = df.loc[ob_row.name:]
            if post_ob["Low"].min() < wick:
                continue
            age = len(post_ob)
            if age > OB_MAX_AGE:
                continue
            return {
                "hi":   round(hi,   6),
                "lo":   round(lo,   6),
                "wick": round(wick, 6),
                "age":  age,
                "time": ob_row.name,
            }

    elif bias == "BEARISH":
        mss_candles = lookback[lookback["BearMSS"]].index.tolist()
        for mss_idx in reversed(mss_candles):
            pool = lookback.loc[:mss_idx].iloc[:-1]
            pool = pool[pool["IsBull"] & (pool["BR"] >= OB_MIN_BODY)]
            if pool.empty:
                continue
            ob_row = pool.iloc[-1]
            hi     = max(float(ob_row["Open"]), float(ob_row["Close"]))
            lo     = min(float(ob_row["Open"]), float(ob_row["Close"]))
            wick   = float(ob_row["High"])
            post_ob = df.loc[ob_row.name:]
            if post_ob["High"].max() > wick:
                continue
            age = len(post_ob)
            if age > OB_MAX_AGE:
                continue
            return {
                "hi":   round(hi,   6),
                "lo":   round(lo,   6),
                "wick": round(wick, 6),
                "age":  age,
                "time": ob_row.name,
            }

    return None


# =============================================================================
#  LAYER 3 — M15 FAIR VALUE GAP inside OB zone
# =============================================================================
def find_fvg_in_ob(m15: pd.DataFrame, bias: str, ob: dict) -> Optional[dict]:
    df   = m15.copy().reset_index()
    min_size = FVG_MIN_PCT / 100

    if bias == "BULLISH":
        for i in range(len(df) - 1, 1, -1):
            gap_lo = float(df.iloc[i - 2]["High"])
            gap_hi = float(df.iloc[i]["Low"])
            gap_sz = gap_hi - gap_lo

            if gap_sz <= 0:
                continue
            ref_price = float(df.iloc[i]["Close"])
            if (gap_sz / ref_price) < min_size:
                continue
            overlap_lo = max(gap_lo, ob["lo"])
            overlap_hi = min(gap_hi, ob["hi"])
            if overlap_lo < overlap_hi:
                return {
                    "lo":  round(gap_lo, 6),
                    "hi":  round(gap_hi, 6),
                    "mid": round((gap_lo + gap_hi) / 2, 6),
                    "time": df.iloc[i]["Time"],
                }

    elif bias == "BEARISH":
        for i in range(len(df) - 1, 1, -1):
            gap_hi = float(df.iloc[i - 2]["Low"])
            gap_lo = float(df.iloc[i]["High"])
            gap_sz = gap_hi - gap_lo

            if gap_sz <= 0:
                continue
            ref_price = float(df.iloc[i]["Close"])
            if (gap_sz / ref_price) < min_size:
                continue
            overlap_lo = max(gap_lo, ob["lo"])
            overlap_hi = min(gap_hi, ob["hi"])
            if overlap_lo < overlap_hi:
                return {
                    "lo":  round(gap_lo, 6),
                    "hi":  round(gap_hi, 6),
                    "mid": round((gap_lo + gap_hi) / 2, 6),
                    "time": df.iloc[i]["Time"],
                }

    return None


# =============================================================================
#  LAYER 4 — M15 ENTRY inside confluence zone
#  (previously M5 — now uses the same M15 series as the FVG check, since
#   OB stays on H1 and entry confirmation has been moved to M15 per request)
# =============================================================================
def check_m15_entry(
    m15: pd.DataFrame,
    bias: str,
    zone_lo: float,
    zone_hi: float,
    adx_val: float,
) -> Optional[dict]:
    df = m15.copy()
    df["RSI"] = calc_rsi(df["Close"], 14)
    df["BR"]  = body_ratio(df)

    cur  = df.iloc[-1]
    prev = df.iloc[-2]

    close = float(cur["Close"])
    high  = float(cur["High"])
    low   = float(cur["Low"])
    open_ = float(cur["Open"])
    rsi_val = float(cur["RSI"]) if not pd.isna(cur["RSI"]) else 50.0
    br      = float(cur["BR"])  if not pd.isna(cur["BR"])  else 0.0

    if not (zone_lo <= close <= zone_hi):
        return None

    rng = high - low
    if rng <= 0:
        return None

    if bias == "BULLISH":
        lower_wick     = (min(open_, close) - low) / rng
        is_bull_candle = close > open_
        momentum       = close > float(prev["Close"])
        rejection      = lower_wick >= MIN_WICK
        rsi_ok         = rsi_val < RSI_OB
        body_ok        = br >= BODY_MIN

        if not is_bull_candle:
            return None

        signal = "BUY"

    elif bias == "BEARISH":
        upper_wick     = (high - max(open_, close)) / rng
        is_bear_candle = close < open_
        momentum       = close < float(prev["Close"])
        rejection      = upper_wick >= MIN_WICK
        rsi_ok         = rsi_val > RSI_OS
        body_ok        = br >= BODY_MIN

        if not is_bear_candle:
            return None

        signal = "SELL"

    else:
        return None

    # -------------------------------------------------------------------
    #  SCORING (0-9)
    #    ADX strength      -> up to 2
    #    Rejection wick     -> up to 2
    #    RSI not extreme    -> up to 2
    #    Momentum agrees    -> up to 1
    #    Body strength      -> up to 1
    #    Candle closed deep in zone (near mid) -> up to 1
    # -------------------------------------------------------------------
    score = 0

    if adx_val >= ADX_MIN + 10:
        score += 2
    elif adx_val >= ADX_MIN:
        score += 1

    if rejection:
        score += 2
    elif (bias == "BULLISH" and lower_wick >= MIN_WICK / 2) or \
         (bias == "BEARISH" and upper_wick >= MIN_WICK / 2):
        score += 1

    if rsi_ok:
        score += 2
    elif (bias == "BULLISH" and rsi_val < RSI_OB + 5) or \
         (bias == "BEARISH" and rsi_val > RSI_OS - 5):
        score += 1

    if momentum:
        score += 1

    if body_ok:
        score += 1

    zone_mid = (zone_lo + zone_hi) / 2
    zone_half = (zone_hi - zone_lo) / 2 if zone_hi != zone_lo else 0
    if zone_half > 0 and abs(close - zone_mid) <= zone_half * 0.6:
        score += 1

    score = min(score, SCORE_MAX)

    if score >= RATING_PRIME:
        rating = "PRIME"
    elif score >= RATING_STRONG:
        rating = "STRONG"
    elif score >= RATING_GOOD:
        rating = "GOOD"
    else:
        rating = "SKIP"

    return {
        "signal":  signal,
        "entry":   close,
        "score":   score,
        "rating":  rating,
        "rsi":     rsi_val,
    }


# =============================================================================
#  RISK / REWARD — SL, TP1, TP2 from OB wick + ATR buffer + score-based RR
# =============================================================================
def calc_trade_plan(
    m15: pd.DataFrame,
    signal: str,
    entry: float,
    ob: dict,
    score: int,
) -> dict:
    atr_val = float(atr_series(m15, 14).iloc[-1])
    if pd.isna(atr_val) or atr_val <= 0:
        atr_val = abs(entry) * 0.001  # tiny fallback buffer

    buffer_amt = atr_val * ATR_BUF
    rr = RR_MIN + (RR_MAX - RR_MIN) * (score / SCORE_MAX)

    if signal == "BUY":
        sl  = ob["wick"] - buffer_amt
        risk = entry - sl
        tp1 = entry + risk
        tp2 = entry + risk * rr
    else:
        sl  = ob["wick"] + buffer_amt
        risk = sl - entry
        tp1 = entry - risk
        tp2 = entry - risk * rr

    return {
        "sl":   round(sl, 6),
        "tp1":  round(tp1, 6),
        "tp2":  round(tp2, 6),
        "risk": round(risk, 6),
        "rr":   round(rr, 2),
    }


# =============================================================================
#  MAIN SCAN — one pass across all symbols
# =============================================================================
async def scan_symbol(display_symbol: str, deriv_symbol: str) -> None:
    decimals = DECIMALS.get(display_symbol, 2)

    h4  = await fetch_closed_candles(deriv_symbol, H4_TF,  H4_COUNT)
    if h4 is None or len(h4) < 60:
        log.warning("%s: insufficient H4 data, skipping.", display_symbol)
        return

    bias, adx_val = get_h4_bias(h4)
    if bias == "NEUTRAL":
        log.info("%s: H4 bias NEUTRAL — no trade.", display_symbol)
        return
    if adx_val < ADX_MIN:
        log.info("%s: ADX %.1f below minimum %.1f — no trade.", display_symbol, adx_val, ADX_MIN)
        return

    h1 = await fetch_closed_candles(deriv_symbol, H1_TF, H1_COUNT)
    if h1 is None or len(h1) < 40:
        log.warning("%s: insufficient H1 data, skipping.", display_symbol)
        return

    ob = find_ob(h1, bias)
    if ob is None:
        log.info("%s: no fresh unmitigated H1 OB found.", display_symbol)
        return

    m15 = await fetch_closed_candles(deriv_symbol, M15_TF, M15_COUNT)
    if m15 is None or len(m15) < 30:
        log.warning("%s: insufficient M15 data, skipping.", display_symbol)
        return

    fvg = find_fvg_in_ob(m15, bias, ob)
    if fvg is None:
        log.info("%s: no M15 FVG overlapping H1 OB.", display_symbol)
        return

    zone = {
        "lo": max(ob["lo"], fvg["lo"]),
        "hi": min(ob["hi"], fvg["hi"]),
    }
    if zone["lo"] >= zone["hi"]:
        log.info("%s: OB/FVG overlap invalid.", display_symbol)
        return

    entry_result = check_m15_entry(m15, bias, zone["lo"], zone["hi"], adx_val)
    if entry_result is None:
        log.info("%s: no valid M15 entry confirmation.", display_symbol)
        return

    if entry_result["rating"] == "SKIP":
        log.info("%s: score %d/9 — rated SKIP, not alerting.", display_symbol, entry_result["score"])
        return

    plan = calc_trade_plan(m15, entry_result["signal"], entry_result["entry"], ob, entry_result["score"])

    zone_mid = (zone["lo"] + zone["hi"]) / 2
    key = build_cooldown_key(display_symbol, entry_result["signal"], zone_mid, decimals)
    if is_duplicate(key):
        return

    now = datetime.now(WAT).strftime("%Y-%m-%d %H:%M WAT")

    msg = build_alert(
        display_symbol, entry_result["signal"], entry_result["entry"],
        plan["sl"], plan["tp1"], plan["tp2"], plan["risk"], plan["rr"],
        entry_result["score"], entry_result["rating"], bias,
        ob, fvg, zone, entry_result["rsi"], now, decimals,
    )
    send_telegram(msg)
    mark_sent(key)
    log.info("%s: %s signal sent — score %d/9 (%s).",
              display_symbol, entry_result["signal"], entry_result["score"], entry_result["rating"])


# =============================================================================
#  MAIN LOOP
# =============================================================================
async def main() -> None:
    log.info("Starting Deriv Synthetic Indices SMC+ICT Scanner...")
    symbols = await verify_symbols()
    if not symbols:
        log.error("No valid symbols to scan. Exiting.")
        return

    log.info("Scanning symbols: %s", ", ".join(symbols.keys()))

    while True:
        cycle_start = time.time()
        for display_symbol, deriv_symbol in symbols.items():
            try:
                await scan_symbol(display_symbol, deriv_symbol)
            except Exception as e:
                log.error("%s: unhandled error during scan — %s", display_symbol, e)
            await asyncio.sleep(1)  # small gap between symbol scans

        elapsed = time.time() - cycle_start
        sleep_for = max(5, SCAN_INTERVAL - elapsed)
        log.info("Cycle complete in %.1fs. Sleeping %.1fs.", elapsed, sleep_for)
        await asyncio.sleep(sleep_for)


if __name__ == "__main__":
    asyncio.run(main())

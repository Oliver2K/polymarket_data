"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  POLYMARKET BTC 5-MIN — DATA COLLECTOR v2.0 (MongoDB)                      ║
║                                                                            ║
║  Samlar in ALL tillgänglig marknadsdata och sparar till MongoDB Atlas.      ║
║  Buffrar alla ticks under ett 5-min intervall, batch-insertar vid slut.    ║
║                                                                            ║
║  Krav:  pip install requests pymongo                                       ║
║  Start: python data_mongodb.py                                             ║
║                                                                            ║
║  MongoDB-struktur:                                                         ║
║    DB: polymarket                                                          ║
║    Collection: btc_5min_intervals                                          ║
║    Document: {                                                             ║
║      interval_ts: 1772407200,                                              ║
║      interval_start: "2026-03-01 23:20:00",                                ║
║      interval_end: "2026-03-01 23:25:00",                                  ║
║      tick_count: 280,                                                      ║
║      ticks: [ {sec: 0, ...}, {sec: 1, ...}, ... ]                          ║
║    }                                                                       ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import requests
import json
import time
import sys
import getpass
from datetime import datetime, timezone
from urllib.parse import quote_plus

# ═══════════════════════════════════════════════════════════════════════════════
# KONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

INTERVAL        = 300          # Polymarket BTC 5-min interval (sekunder)
POLL_RATE_HZ    = 1.0          # Snapshots per sekund
DEPTH_LEVELS    = 5            # Antal orderbok-nivåer att spara
SLIP_SIZES      = [5, 10, 20, 100, 200]  # Orderstorlekar för slippage-simulering

# MongoDB
MONGO_USER      = "oliver"
MONGO_CLUSTER   = "polymarket.ptuu9u9.mongodb.net"
MONGO_DB        = "polymarket"
MONGO_COLLECTION = "btc_5min_intervals"

# API-endpoints
CLOB_HOST = "https://clob.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"

# Hur länge vi väntar vid start av nytt intervall
MARKET_DISCOVERY_PATIENCE = 30  # sekunder


# ═══════════════════════════════════════════════════════════════════════════════
# MONGODB-ANSLUTNING
# ═══════════════════════════════════════════════════════════════════════════════

def connect_mongodb(password: str):
    """Ansluter till MongoDB Atlas och returnerar collection-objekt."""
    try:
        from pymongo import MongoClient
        from pymongo.errors import ConnectionFailure
    except ImportError:
        print("  ❌ pymongo saknas. Installera med: pip install pymongo")
        sys.exit(1)

    encoded_pw = quote_plus(password)
    uri = (f"mongodb+srv://{MONGO_USER}:{encoded_pw}@{MONGO_CLUSTER}/"
           f"?retryWrites=true&w=majority&appName=polymarket")

    print("  🔌 Ansluter till MongoDB Atlas...")
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=10000)
        # Testa anslutningen
        client.admin.command("ping")
        print("  ✅ MongoDB-anslutning OK!")

        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]

        # Skapa index för snabb sökning på interval_ts (unikt)
        collection.create_index("interval_ts", unique=True)
        # Index för tidsbaserade queries
        collection.create_index("interval_start")

        doc_count = collection.count_documents({})
        print(f"  📊 Collection '{MONGO_COLLECTION}': {doc_count} intervall sparade")

        return client, collection

    except Exception as e:
        print(f"  ❌ Kunde inte ansluta till MongoDB: {e}")
        sys.exit(1)


# ═══════════════════════════════════════════════════════════════════════════════
# MARKNADSDISCOVERY
# ═══════════════════════════════════════════════════════════════════════════════

_market_cache = {}

def discover_market(interval_ts: int) -> dict | None:
    """Hämtar token-IDs för ett givet BTC 5-min intervall."""
    if _market_cache.get("ts") == interval_ts:
        return _market_cache.get("data")

    slug = f"btc-updown-5m-{interval_ts}"
    try:
        r = requests.get(f"{GAMMA_API}/markets?slug={slug}", timeout=5)
        if r.status_code != 200 or not r.json():
            return None
        market = r.json()[0] if isinstance(r.json(), list) else r.json()
        token_ids = json.loads(market.get("clobTokenIds", "[]"))
        outcomes = json.loads(market.get("outcomes", "[]"))
        if len(token_ids) < 2 or len(outcomes) < 2:
            return None

        token_up = None
        token_dn = None
        for i, outcome in enumerate(outcomes):
            if outcome.lower() == "up":
                token_up = token_ids[i]
            elif outcome.lower() == "down":
                token_dn = token_ids[i]

        if not token_up or not token_dn:
            return None

        data = {
            "slug": slug,
            "token_up": token_up,
            "token_dn": token_dn,
        }
        _market_cache["ts"] = interval_ts
        _market_cache["data"] = data
        return data
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# BTC SPOTPRIS
# ═══════════════════════════════════════════════════════════════════════════════

_btc_price_history = []

def fetch_btc_price() -> dict:
    """Hämtar BTC/USDT spotpris från Binance med rullande historik."""
    global _btc_price_history

    price = 0.0
    try:
        r = requests.get(
            "https://api.binance.com/api/v3/ticker/price",
            params={"symbol": "BTCUSDT"}, timeout=2)
        if r.status_code == 200:
            price = float(r.json().get("price", 0))
    except Exception:
        pass

    if price <= 0:
        try:
            r = requests.get(
                "https://api.coingecko.com/api/v3/simple/price",
                params={"ids": "bitcoin", "vs_currencies": "usd"}, timeout=3)
            if r.status_code == 200:
                price = float(r.json().get("bitcoin", {}).get("usd", 0))
        except Exception:
            pass

    if price <= 0:
        return {"price": 0, "change_5m_pct": 0, "change_1m_pct": 0,
                "change_15s_pct": 0, "volatility_5m": 0, "trend": "UNKNOWN"}

    now = time.time()
    _btc_price_history.append((now, price))
    _btc_price_history = [(t, p) for t, p in _btc_price_history if now - t < 360]

    change_5m = 0.0
    change_1m = 0.0
    change_15s = 0.0

    for t, p in _btc_price_history:
        age = now - t
        if 290 < age < 320 and p > 0:
            change_5m = (price - p) / p * 100
        if 55 < age < 70 and p > 0:
            change_1m = (price - p) / p * 100
        if 12 < age < 20 and p > 0:
            change_15s = (price - p) / p * 100

    vol_5m = 0.0
    if len(_btc_price_history) >= 10:
        changes = []
        sorted_hist = sorted(_btc_price_history, key=lambda x: x[0])
        for i in range(1, len(sorted_hist)):
            prev_p = sorted_hist[i-1][1]
            curr_p = sorted_hist[i][1]
            if prev_p > 0:
                changes.append(abs((curr_p - prev_p) / prev_p * 100))
        if changes:
            vol_5m = sum(changes) / len(changes)

    if change_5m > 0.05:
        trend = "UP"
    elif change_5m < -0.05:
        trend = "DOWN"
    else:
        trend = "FLAT"

    return {
        "price": round(price, 2),
        "change_5m_pct": round(change_5m, 4),
        "change_1m_pct": round(change_1m, 4),
        "change_15s_pct": round(change_15s, 4),
        "volatility_5m": round(vol_5m, 6),
        "trend": trend,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# ORDERBOK-HÄMTNING
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_orderbook(token_id: str) -> dict | None:
    """Hämtar full orderbok via GET /book?token_id=X"""
    try:
        r = requests.get(
            f"{CLOB_HOST}/book",
            params={"token_id": token_id},
            timeout=4
        )
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None


def parse_book_side(entries: list, levels: int = DEPTH_LEVELS) -> tuple:
    """Parsar en sida av orderboken."""
    parsed = []
    total_vol = 0.0
    value_sum = 0.0

    for entry in entries:
        p = float(entry.get("price", 0))
        s = float(entry.get("size", 0))
        total_vol += s
        if len(parsed) < levels:
            parsed.append((p, s))
            value_sum += p * s

    while len(parsed) < levels:
        parsed.append((0.0, 0.0))

    return parsed, total_vol, len(entries), value_sum


def estimate_slippage(asks: list, order_usd: float) -> float:
    """Simulerar en market-buy-order och returnerar genomsnittspris."""
    remaining_usd = order_usd
    total_shares = 0.0

    for entry in asks:
        p = float(entry.get("price", 0))
        s = float(entry.get("size", 0))
        if p <= 0 or s <= 0:
            continue
        level_usd = p * s
        if level_usd >= remaining_usd:
            shares_bought = remaining_usd / p
            total_shares += shares_bought
            remaining_usd = 0
            break
        else:
            total_shares += s
            remaining_usd -= level_usd

    if total_shares <= 0:
        return -1.0
    return order_usd / total_shares if remaining_usd <= 0 else -1.0


# ═══════════════════════════════════════════════════════════════════════════════
# SNAPSHOT-BYGGARE — alla kolumner som dict (MongoDB-vänligt)
# ═══════════════════════════════════════════════════════════════════════════════

def build_snapshot(interval_ts: int, market: dict) -> dict | None:
    """Bygger en komplett snapshot som dict (redo för MongoDB)."""
    t_start = time.time()
    now_ts = time.time()
    now_ms = int(now_ts * 1000)
    interval_start_ts = interval_ts
    sec_in = int(now_ts) - interval_start_ts

    book_up = fetch_orderbook(market["token_up"])
    book_dn = fetch_orderbook(market["token_dn"])
    fetch_ms = round((time.time() - t_start) * 1000, 1)

    if not book_up or not book_dn:
        return None

    quality = "OK"

    bids_up = sorted(book_up.get("bids", []), key=lambda x: float(x.get("price", 0)), reverse=True)
    bids_dn = sorted(book_dn.get("bids", []), key=lambda x: float(x.get("price", 0)), reverse=True)
    asks_up = sorted(book_up.get("asks", []), key=lambda x: float(x.get("price", 0)), reverse=False)
    asks_dn = sorted(book_dn.get("asks", []), key=lambda x: float(x.get("price", 0)), reverse=False)

    up_bid_levels, up_bid_total_vol, up_bid_n, up_bid_val = parse_book_side(bids_up)
    up_ask_levels, up_ask_total_vol, up_ask_n, up_ask_val = parse_book_side(asks_up)
    dn_bid_levels, dn_bid_total_vol, dn_bid_n, dn_bid_val = parse_book_side(bids_dn)
    dn_ask_levels, dn_ask_total_vol, dn_ask_n, dn_ask_val = parse_book_side(asks_dn)

    up_bb = up_bid_levels[0][0] if up_bid_levels[0][0] > 0 else 0.0
    up_ba = up_ask_levels[0][0] if up_ask_levels[0][0] > 0 else 0.0
    dn_bb = dn_bid_levels[0][0] if dn_bid_levels[0][0] > 0 else 0.0
    dn_ba = dn_ask_levels[0][0] if dn_ask_levels[0][0] > 0 else 0.0

    if up_bb == 0 and up_ba == 0:
        quality = "STALE"
    elif up_bb == 0 or up_ba == 0 or dn_bb == 0 or dn_ba == 0:
        quality = "PARTIAL"

    up_mid = (up_bb + up_ba) / 2 if (up_bb > 0 and up_ba > 0) else (up_bb or up_ba)
    dn_mid = (dn_bb + dn_ba) / 2 if (dn_bb > 0 and dn_ba > 0) else (dn_bb or dn_ba)

    up_spread = up_ba - up_bb if (up_bb > 0 and up_ba > 0) else 0.0
    dn_spread = dn_ba - dn_bb if (dn_bb > 0 and dn_ba > 0) else 0.0
    up_spread_pct = (up_spread / up_mid * 100) if up_mid > 0 else 0.0
    dn_spread_pct = (dn_spread / dn_mid * 100) if dn_mid > 0 else 0.0

    safe_inv = lambda x: round(1.0 / x, 6) if x > 0 else 0.0

    raw_sum = up_ba + dn_ba if (up_ba > 0 and dn_ba > 0) else 0
    if raw_sum > 0:
        factor = 1.026 / raw_sum
        up_odds_norm = round(1 / (up_ba * factor), 6)
        dn_odds_norm = round(1 / (dn_ba * factor), 6)
    else:
        up_odds_norm = 0.0
        dn_odds_norm = 0.0

    up_slips = {}
    dn_slips = {}
    for usd in SLIP_SIZES:
        up_slips[usd] = estimate_slippage(asks_up, usd)
        dn_slips[usd] = estimate_slippage(asks_dn, usd)

    def _imbalance(bid_vol, ask_vol):
        total = bid_vol + ask_vol
        return round((bid_vol - ask_vol) / total, 4) if total > 0 else 0.0

    def _ratio(bid_vol, ask_vol):
        return round(bid_vol / ask_vol, 4) if ask_vol > 0 else 0.0

    btc = fetch_btc_price()

    prob_sum = up_mid + dn_mid
    implied_up = round(up_mid / prob_sum, 6) if prob_sum > 0 else 0.0
    implied_dn = round(dn_mid / prob_sum, 6) if prob_sum > 0 else 0.0

    utc_str = datetime.fromtimestamp(now_ts, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S.%f")[:-3]

    # ── Bygg tick-dokument (nested dict, MongoDB-vänligt) ──────────────
    tick = {
        "sec": sec_in,
        "ts_utc": utc_str,
        "ts_ms": now_ms,

        # UP priser
        "up_bb": round(up_bb, 4),
        "up_ba": round(up_ba, 4),
        "up_mid": round(up_mid, 4),
        "up_spread": round(up_spread, 4),
        "up_spread_pct": round(up_spread_pct, 2),

        # DN priser
        "dn_bb": round(dn_bb, 4),
        "dn_ba": round(dn_ba, 4),
        "dn_mid": round(dn_mid, 4),
        "dn_spread": round(dn_spread, 4),
        "dn_spread_pct": round(dn_spread_pct, 2),

        # Odds
        "up_odds_bid": safe_inv(up_bb),
        "up_odds_ask": safe_inv(up_ba),
        "up_odds_mid": safe_inv(up_mid),
        "dn_odds_bid": safe_inv(dn_bb),
        "dn_odds_ask": safe_inv(dn_ba),
        "dn_odds_mid": safe_inv(dn_mid),
        "up_odds_norm": up_odds_norm,
        "dn_odds_norm": dn_odds_norm,

        # Orderdjup UP — nested arrays istället för flat kolumner
        "up_bids": [{"p": round(p, 4), "s": round(s, 2)} for p, s in up_bid_levels],
        "up_asks": [{"p": round(p, 4), "s": round(s, 2)} for p, s in up_ask_levels],
        "up_bid_vol": round(up_bid_total_vol, 2),
        "up_ask_vol": round(up_ask_total_vol, 2),
        "up_bid_n": up_bid_n,
        "up_ask_n": up_ask_n,
        "up_bid_val5": round(up_bid_val, 2),
        "up_ask_val5": round(up_ask_val, 2),

        # Orderdjup DN
        "dn_bids": [{"p": round(p, 4), "s": round(s, 2)} for p, s in dn_bid_levels],
        "dn_asks": [{"p": round(p, 4), "s": round(s, 2)} for p, s in dn_ask_levels],
        "dn_bid_vol": round(dn_bid_total_vol, 2),
        "dn_ask_vol": round(dn_ask_total_vol, 2),
        "dn_bid_n": dn_bid_n,
        "dn_ask_n": dn_ask_n,
        "dn_bid_val5": round(dn_bid_val, 2),
        "dn_ask_val5": round(dn_ask_val, 2),

        # Slippage
        "slippage": {},

        # Orderbok-obalans
        "up_imbalance": _imbalance(up_bid_total_vol, up_ask_total_vol),
        "dn_imbalance": _imbalance(dn_bid_total_vol, dn_ask_total_vol),
        "up_ba_ratio": _ratio(up_bid_total_vol, up_ask_total_vol),
        "dn_ba_ratio": _ratio(dn_bid_total_vol, dn_ask_total_vol),

        # Korsmarknadsdata
        "implied_up": implied_up,
        "implied_dn": implied_dn,
        "overround": round(prob_sum, 6),
        "arb_spread": round(up_bb + dn_bb, 6),

        # BTC
        "btc_price": btc["price"],
        "btc_chg_5m": btc["change_5m_pct"],
        "btc_chg_1m": btc["change_1m_pct"],
        "btc_chg_15s": btc["change_15s_pct"],
        "btc_vol_5m": btc["volatility_5m"],
        "btc_trend": btc["trend"],

        # Meta
        "fetch_ms": fetch_ms,
        "quality": quality,
    }

    # Slippage som nested dict
    for usd in SLIP_SIZES:
        tick["slippage"][f"up_{usd}"] = round(up_slips[usd], 6) if up_slips[usd] > 0 else None
        tick["slippage"][f"dn_{usd}"] = round(dn_slips[usd], 6) if dn_slips[usd] > 0 else None

    return tick


# ═══════════════════════════════════════════════════════════════════════════════
# MONGODB BATCH INSERT — sparar hela intervallet som ett dokument
# ═══════════════════════════════════════════════════════════════════════════════

def flush_interval_to_mongodb(collection, interval_ts: int, ticks: list):
    """
    Sparar alla ticks för ett intervall som ETT MongoDB-dokument.
    Använder upsert för att kunna återuppta vid crash.
    """
    if not ticks:
        return False

    interval_start = datetime.fromtimestamp(
        interval_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    interval_end = datetime.fromtimestamp(
        interval_ts + INTERVAL, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # Beräkna sammanfattningsstatistik
    up_spreads = [t["up_spread_pct"] for t in ticks if t.get("up_spread_pct")]
    dn_spreads = [t["dn_spread_pct"] for t in ticks if t.get("dn_spread_pct")]
    qualities = {}
    for t in ticks:
        q = t.get("quality", "OK")
        qualities[q] = qualities.get(q, 0) + 1

    doc = {
        "interval_ts": interval_ts,
        "interval_start": interval_start,
        "interval_end": interval_end,
        "tick_count": len(ticks),
        "saved_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),

        # Sammanfattning (för snabb överblick utan att ladda alla ticks)
        "summary": {
            "avg_up_spread_pct": round(sum(up_spreads) / len(up_spreads), 2) if up_spreads else 0,
            "avg_dn_spread_pct": round(sum(dn_spreads) / len(dn_spreads), 2) if dn_spreads else 0,
            "min_spread_pct": round(min(min(up_spreads, default=99), min(dn_spreads, default=99)), 2),
            "max_spread_pct": round(max(max(up_spreads, default=0), max(dn_spreads, default=0)), 2),
            "btc_start": ticks[0].get("btc_price", 0),
            "btc_end": ticks[-1].get("btc_price", 0),
            "qualities": qualities,
        },

        # Alla ticks (huvuddata)
        "ticks": ticks,
    }

    try:
        # Upsert: om intervallet redan finns (t.ex. vid restart), ersätt det
        result = collection.replace_one(
            {"interval_ts": interval_ts},
            doc,
            upsert=True
        )
        return True
    except Exception as e:
        print(f"    ❌ MongoDB-skrivfel: {e}")
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# INTERVALL-STATISTIK (för terminal-utskrift)
# ═══════════════════════════════════════════════════════════════════════════════

class IntervalStats:
    def __init__(self):
        self.ticks = 0
        self.up_spreads = []
        self.dn_spreads = []
        self.qualities = {"OK": 0, "PARTIAL": 0, "STALE": 0}

    def add(self, tick: dict):
        self.ticks += 1
        sp_up = tick.get("up_spread_pct", 0)
        sp_dn = tick.get("dn_spread_pct", 0)
        if sp_up: self.up_spreads.append(sp_up)
        if sp_dn: self.dn_spreads.append(sp_dn)
        q = tick.get("quality", "OK")
        self.qualities[q] = self.qualities.get(q, 0) + 1

    def summary(self, interval_str: str) -> str:
        if not self.up_spreads:
            return f"  {interval_str}: {self.ticks} ticks, no spread data"
        avg_sp_up = sum(self.up_spreads) / len(self.up_spreads)
        avg_sp_dn = sum(self.dn_spreads) / len(self.dn_spreads) if self.dn_spreads else 0
        q_str = " ".join(f"{k}:{v}" for k, v in self.qualities.items() if v > 0)
        return (f"  {interval_str}: {self.ticks} ticks | "
                f"Spread UP:{avg_sp_up:.1f}% DN:{avg_sp_dn:.1f}% | {q_str}")


# ═══════════════════════════════════════════════════════════════════════════════
# HUVUD-LOOP
# ═══════════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT (med graceful shutdown — sparar buffer vid Ctrl+C)
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # Globala refs för graceful shutdown
    _shutdown_collection = None
    _shutdown_interval_ts = None
    _shutdown_tick_buffer = None

    def _patched_run():
        """Wrapper som exponerar state för graceful shutdown."""
        global _shutdown_collection, _shutdown_interval_ts, _shutdown_tick_buffer

        # ── Lösenordsinmatning ─────────────────────────────────────────
        print()
        print("╔══════════════════════════════════════════════════════════════╗")
        print("║  📡 POLYMARKET DATA COLLECTOR v2.0 (MongoDB)               ║")
        print("╚══════════════════════════════════════════════════════════════╝")
        print()

        password = getpass.getpass("  🔑 MongoDB-lösenord: ")
        if not password:
            print("  ❌ Inget lösenord angivet.")
            sys.exit(1)

        client, collection = connect_mongodb(password)
        _shutdown_collection = collection

        poll_interval = 1.0 / POLL_RATE_HZ

        print()
        print("╔══════════════════════════════════════════════════════════════╗")
        print("║  KONFIGURATION                                             ║")
        print("╠══════════════════════════════════════════════════════════════╣")
        print(f"║  Poll-frekvens   : {POLL_RATE_HZ} Hz ({poll_interval:.1f}s per snapshot)          ║")
        print(f"║  Orderbok-djup   : {DEPTH_LEVELS} nivåer per sida                      ║")
        print(f"║  Slippage-sim    : ${SLIP_SIZES}                ║")
        print(f"║  MongoDB         : {MONGO_DB}.{MONGO_COLLECTION}        ║")
        print("╠══════════════════════════════════════════════════════════════╣")
        print("║  Sparmetod       : Buffrar → batch-insert per intervall    ║")
        print("║  ~1 MongoDB-write per 5 min (istf 300 writes)             ║")
        print("╚══════════════════════════════════════════════════════════════╝")
        print()
        print("  Tryck Ctrl+C för att stoppa.\n")

        last_interval_ts = None
        interval_stats = None
        tick_buffer = []
        total_ticks = 0
        total_intervals_saved = 0
        session_start = time.time()

        while True:
            loop_start = time.time()
            now_ts = int(time.time())
            interval_ts = (now_ts // INTERVAL) * INTERVAL
            sec_in = now_ts - interval_ts

            # Exponera state för shutdown
            _shutdown_interval_ts = last_interval_ts
            _shutdown_tick_buffer = tick_buffer

            if sec_in >= 298:
                time.sleep(2)
                continue

            if interval_ts != last_interval_ts:
                if last_interval_ts is not None and tick_buffer:
                    success = flush_interval_to_mongodb(
                        collection, last_interval_ts, tick_buffer)
                    if success:
                        total_intervals_saved += 1
                        print(f"  💾 Sparat {len(tick_buffer)} ticks → MongoDB "
                              f"(totalt {total_intervals_saved} intervall)")
                    else:
                        print(f"  ⚠️  MISSLYCKADES spara {len(tick_buffer)} ticks!")

                if interval_stats and last_interval_ts:
                    prev_str = datetime.fromtimestamp(
                        last_interval_ts, tz=timezone.utc).strftime("%H:%M:%S")
                    print(interval_stats.summary(prev_str))

                interval_str = datetime.fromtimestamp(
                    interval_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                interval_end_str = datetime.fromtimestamp(
                    interval_ts + INTERVAL, tz=timezone.utc).strftime("%H:%M:%S")
                print(f"\n{'─'*62}")
                print(f"  🕒 Nytt intervall: {interval_str} → {interval_end_str}")
                print(f"{'─'*62}")

                interval_stats = IntervalStats()
                tick_buffer = []
                _shutdown_tick_buffer = tick_buffer
                last_interval_ts = interval_ts
                _shutdown_interval_ts = interval_ts

            market = discover_market(interval_ts)
            if not market:
                if sec_in < MARKET_DISCOVERY_PATIENCE:
                    time.sleep(poll_interval)
                    continue
                else:
                    if sec_in % 30 < 1:
                        print(f"    ⚠️  Ingen marknad hittad (sec={sec_in})")
                    time.sleep(poll_interval)
                    continue

            tick = build_snapshot(interval_ts, market)

            if tick:
                tick_buffer.append(tick)
                interval_stats.add(tick)
                total_ticks += 1

                if sec_in % 30 == 0 or sec_in <= 2:
                    elapsed = time.time() - session_start
                    rate = total_ticks / elapsed * 3600 if elapsed > 0 else 0
                    buf_size = len(tick_buffer)
                    print(f"    [{sec_in:>3}s] UP {tick['up_bb']:.3f}/{tick['up_ba']:.3f} "
                          f"sp:{tick['up_spread_pct']:.1f}%  |  "
                          f"DN {tick['dn_bb']:.3f}/{tick['dn_ba']:.3f} "
                          f"sp:{tick['dn_spread_pct']:.1f}%  |  "
                          f"BTC:${tick.get('btc_price', 0):,.0f} "
                          f"({tick.get('btc_trend', '?')})  |  "
                          f"{tick['fetch_ms']:.0f}ms {tick['quality']}  "
                          f"[buf:{buf_size} tot:{total_ticks}]")

            elapsed = time.time() - loop_start
            sleep_time = max(0, poll_interval - elapsed)
            time.sleep(sleep_time)

    try:
        _patched_run()
    except KeyboardInterrupt:
        print(f"\n\n  👋 Datainsamling stoppad.")
        # Spara buffrade ticks vid avslut
        if _shutdown_collection and _shutdown_tick_buffer and _shutdown_interval_ts:
            print(f"  💾 Sparar {len(_shutdown_tick_buffer)} buffrade ticks "
                  f"för pågående intervall...")
            success = flush_interval_to_mongodb(
                _shutdown_collection, _shutdown_interval_ts, _shutdown_tick_buffer)
            if success:
                print(f"  ✅ Sparat! Inga ticks förlorade.")
            else:
                print(f"  ❌ Kunde inte spara buffrade ticks.")
        else:
            print(f"  ℹ️  Ingen buffrad data att spara.")
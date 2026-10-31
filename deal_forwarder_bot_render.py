"""
LootSpy - Telegram deal forwarder with price-history verification.

Flow for every post from the source channels:
  1. Parse price + MRP / discount %           (post text and hidden links)
  2. MRP wall: if the post shows a discount, it must be >= MIN_MRP_DISCOUNT (60%)
     Posts that show no MRP / discount skip this wall.
  3. Dedup (per product, 24h)
  4. History lookup (SpendMitra): post price must be >= MIN_MEDIAN_DISCOUNT (10%)
     below the 6-month median. Lookup failure / thin history = no post.
  5. Append the price line and forward to ExtraPe (it converts the links)

Set DRY_RUN=1 to run the whole pipeline but log verdicts instead of forwarding.
"""
import asyncio
import hashlib
import ipaddress
import os
import re
import statistics
import threading
import time
from collections import Counter, OrderedDict, deque
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin, urlparse

import httpx
from dotenv import load_dotenv
from flask import Flask, jsonify
from supabase import create_client
from telethon import TelegramClient, errors, events
from telethon.sessions import StringSession
from telethon.tl.types import MessageEntityTextUrl, MessageMediaWebPage

load_dotenv()


# ================= SETTINGS =================
def _f(name, default):
    return float(os.getenv(name, default))


MIN_MRP_DISCOUNT = _f("MIN_MRP_DISCOUNT", 60)  # wall 1: % off MRP shown in post
MIN_MEDIAN_DISCOUNT = _f("MIN_MEDIAN_DISCOUNT", 10)  # wall 2: % below 6M median
LOOKUPS_PER_MIN = _f("LOOKUPS_PER_MIN", 20)  # SpendMitra limit is ~45/min
LOOKUP_CONCURRENCY = int(_f("LOOKUP_CONCURRENCY", 3))
LOOKUP_TIMEOUT = _f("LOOKUP_TIMEOUT", 45)
MAX_PENDING_LOOKUPS = int(_f("MAX_PENDING_LOOKUPS", 50))
STATS_CACHE_HOURS = _f("STATS_CACHE_HOURS", 3)
HISTORY_DAYS = 180
MIN_HISTORY_DAYS = int(_f("MIN_HISTORY_DAYS", 45))  # distinct days with a price
MIN_SPAN_DAYS = int(_f("MIN_SPAN_DAYS", 90))  # history must reach back this far
MAX_GAP_DAYS = int(_f("MAX_GAP_DAYS", 21))  # biggest allowed hole in history
MIN_PRICE_VS_LOW = _f("MIN_PRICE_VS_LOW", 0.4)  # price < 40% of 6M low = likely parse error
MAX_MESSAGE_AGE_SEC = int(_f("MAX_MESSAGE_AGE_SEC", 900))
EXTRAPE_GAP_SEC = _f("EXTRAPE_GAP_SEC", 1.5)
DEDUP_HOURS = int(_f("DEDUP_HOURS", 24))
SUMMARY_MINUTES = int(_f("SUMMARY_MINUTES", 60))
LOG_FORWARDS = os.getenv("LOG_FORWARDS", "1") == "1"
DRY_RUN = os.getenv("DRY_RUN", "0") == "1"
SPENDMITRA_URL = os.getenv("SPENDMITRA_URL", "https://spendmitra.com/api/product/get-data")
PORT = int(os.getenv("PORT", 5000))

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

# Filled in main()
client = None
supabase = None
HTTP = None
DEST_BOT = None
LOG_CHANNEL_ID = None

COUNTERS = Counter()
START_TIME = time.time()


# ================= FLASK =================
app = Flask(__name__)


@app.route("/")
def home():
    return {"status": "running", "service": "lootspy", "dry_run": DRY_RUN, "stats": dict(COUNTERS)}


@app.route("/ping")
def ping():
    return jsonify({"status": "alive"})


def run_flask():
    app.run(host="0.0.0.0", port=PORT, use_reloader=False)


# ================= LOGGING (quiet by design) =================
LOG_QUEUE = None
_LOG_LAST = {}


def log(msg, key=None):
    """Print always; send to the log channel via a throttled queue.
    `key` throttles repeated messages of the same kind to one per minute."""
    print(msg, flush=True)
    if LOG_QUEUE is None:
        return
    if key:
        now = time.time()
        if now - _LOG_LAST.get(key, 0) < 60:
            return
        _LOG_LAST[key] = now
    try:
        LOG_QUEUE.put_nowait(msg)
    except asyncio.QueueFull:
        pass


async def log_worker():
    while True:
        text = await LOG_QUEUE.get()
        try:
            await client.send_message(LOG_CHANNEL_ID, text[:4000])
        except errors.FloodWaitError as e:
            await asyncio.sleep(e.seconds + 1)
        except Exception as e:
            print("LOG ERROR:", repr(e), flush=True)
        await asyncio.sleep(2)


# ================= TEXT HELPERS =================
def normalize(text):
    text = re.sub(r"[^\w\s]", " ", text.lower())
    return re.sub(r"\s+", " ", text).strip()


def sha(text):
    return hashlib.sha256(text.encode()).hexdigest()


def inr(n):
    """Format a rupee amount the Indian way: 1,23,456."""
    s = str(int(round(n)))
    if len(s) <= 3:
        return "₹" + s
    last3, rest = s[-3:], s[:-3]
    parts = []
    while len(rest) > 2:
        parts.insert(0, rest[-2:])
        rest = rest[:-2]
    if rest:
        parts.insert(0, rest)
    return "₹" + ",".join(parts + [last3])


# ================= DEAL PARSING =================
CUR = r"(?:₹|rs\.?|inr)"
NUM = r"(\d[\d,]*(?:\.\d+)?)"
RE_AMT = re.compile(CUR + r"\s*" + NUM, re.I)
RE_MRP = re.compile(
    r"\b(?:m\.?\s?r\.?\s?p\.?|original\s*price|list\s*price|regular\s*price|was)\b\s*(?:price)?\s*[:\-–=]?\s*(?:"
    + CUR
    + r")?\s*"
    + NUM,
    re.I,
)
RE_PRICE_STRONG = re.compile(
    r"(?:\bdeal\s*price|\boffer\s*price|\bspecial\s*price|\bprice|@)\s*[:\-–=]?\s*(?:" + CUR + r")?\s*" + NUM,
    re.I,
)
RE_PRICE_WEAK = re.compile(
    r"(?:\bat|\bnow|\bjust|\bonly|\bfor|\bloot|➡️|➡|→|->)\s*[:\-–]?\s*" + CUR + r"\s*" + NUM, re.I
)
RE_PCT = re.compile(r"(\d{1,2}(?:\.\d+)?)\s*%\s*(?:off|discount|disc\b)", re.I)
BONUS_BEFORE = re.compile(r"(save|saving|extra|coupon|cashback|discount|upto|up to|flat|bank|off)\W{0,12}$", re.I)
BONUS_AFTER = re.compile(r"^\W{0,3}(off|cashback|discount|coupon|back|saving)", re.I)


def num(s):
    try:
        return float(s.replace(",", ""))
    except (ValueError, AttributeError):
        return None


def _bonus_context(text, start, end):
    before = text[max(0, start - 16) : start]
    after = text[end : end + 12]
    return bool(BONUS_BEFORE.search(before) or BONUS_AFTER.search(after))


def parse_deal(text):
    """Return dict(price, mrp, pct). Any of them can be None."""
    text = text or ""
    mrp = None
    clean = list(text)
    for m in RE_MRP.finditer(text):
        v = num(m.group(1))
        if v and v >= 10 and mrp is None:
            mrp = v
        for i in range(m.start(), m.end()):  # blank the MRP so it is not read as the price
            clean[i] = " "
    clean = "".join(clean)

    price = None
    for rx in (RE_PRICE_STRONG, RE_PRICE_WEAK):
        for m in rx.finditer(clean):
            if _bonus_context(clean, m.start(), m.end()):
                continue
            v = num(m.group(1))
            if v and v >= 5:
                price = v
                break
        if price:
            break

    amounts = []
    for m in RE_AMT.finditer(clean):
        if _bonus_context(clean, m.start(), m.end()):
            continue
        v = num(m.group(1))
        if v and v >= 5:
            amounts.append(v)
    if price is None and amounts:
        price = min(amounts)
    if mrp is None and price and amounts:
        biggest = max(amounts)
        if biggest > price * 1.3:
            mrp = biggest  # unlabeled "was" price, e.g. "₹1999 ➡️ ₹499"
    if price and mrp and mrp <= price:
        mrp = None

    pcts = []
    for m in RE_PCT.finditer(clean):
        before = clean[max(0, m.start() - 18) : m.start()].lower()
        after = clean[m.end() : m.end() + 25].lower()
        if re.search(r"extra|additional|addl|more", before):
            continue
        if re.search(r"bank|card|upi|coupon|cashback", after):
            continue
        pcts.append(float(m.group(1)))
    pct = max(pcts) if pcts else None
    return {"price": price, "mrp": mrp, "pct": pct}


def claimed_discount(parsed):
    """Discount % the post claims vs MRP, or None when the post shows none."""
    if parsed["price"] and parsed["mrp"]:
        return (1 - parsed["price"] / parsed["mrp"]) * 100
    return parsed["pct"]


# ================= LINKS & PRODUCT KEY =================
AMAZON_DOMAINS = ("amazon.in", "amzn.in", "amzn.to", "a.co")
FLIPKART_DOMAINS = ("flipkart.com", "fkrt.co", "fkrt.it", "fkrt.cc", "fkrt.in")
OTHER_SHORTENERS = ("bit.ly", "tinyurl.com", "cutt.ly", "t.ly", "rebrand.ly", "shorturl.at", "is.gd")
RE_URL = re.compile(r"https?://[^\s<>\"']+", re.I)
RE_ASIN = re.compile(r"/(?:dp|gp/product|gp/aw/d|product)/([A-Z0-9]{10})", re.I)
RE_ITM = re.compile(r"/p/(itm[a-z0-9]+)", re.I)
RE_PID = re.compile(r"[?&]pid=([A-Z0-9]{8,20})")


def _host(url):
    try:
        return (urlparse(url).hostname or "").lower()
    except ValueError:
        return ""


def _in(host, domains):
    return any(host == d or host.endswith("." + d) for d in domains)


def collect_urls(msg):
    text = msg.message or ""
    urls = [u.rstrip(".,)]}>\"'") for u in RE_URL.findall(text)]
    for ent in msg.entities or []:
        if isinstance(ent, MessageEntityTextUrl) and ent.url:
            urls.append(ent.url)
    seen, out = set(), []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def key_from_url(url):
    """Product key straight from a URL, or None (short links need resolving)."""
    host = _host(url)
    if _in(host, ("amazon.in",)):
        m = RE_ASIN.search(url)
        if m:
            return f"amazon_{m.group(1).upper()}"
    if _in(host, ("flipkart.com",)):
        m = RE_ITM.search(url)
        if m:
            return f"flipkart_{m.group(1).lower()}"
        m = RE_PID.search(url)
        if m:
            return f"flipkart_{m.group(1)}"
    return None


def candidate_urls(urls):
    """URLs that can be Amazon / Flipkart products (directly or via short links)."""
    out = []
    for u in urls:
        h = _host(u)
        if _in(h, AMAZON_DOMAINS) or _in(h, FLIPKART_DOMAINS) or _in(h, OTHER_SHORTENERS):
            out.append(u)
    return out[:3]


def _public_https(url):
    p = urlparse(url)
    if p.scheme not in ("http", "https") or not p.hostname:
        return False
    host = p.hostname.lower()
    if host == "localhost" or host.endswith((".local", ".internal")):
        return False
    try:
        ipaddress.ip_address(host)
        return False  # raw IPs are never short links
    except ValueError:
        return True


RESOLVE_CACHE = OrderedDict()


async def resolve_short_link(url, max_hops=5):
    """Follow redirects (headers only) until a product URL shows up."""
    if url in RESOLVE_CACHE:
        return RESOLVE_CACHE[url]
    final, cur = None, url
    for _ in range(max_hops):
        if not _public_https(cur):
            break
        try:
            async with HTTP.stream(
                "GET", cur, headers={"User-Agent": UA}, timeout=8, follow_redirects=False
            ) as r:
                loc, status = r.headers.get("location"), r.status_code
        except Exception:
            break
        if status in (301, 302, 303, 307, 308) and loc:
            cur = urljoin(cur, loc)
            if key_from_url(cur):
                final = cur
                break
            continue
        break
    RESOLVE_CACHE[url] = final
    while len(RESOLVE_CACHE) > 2000:
        RESOLVE_CACHE.popitem(last=False)
    return final


async def resolve_product(urls):
    """-> (product_key, url_to_look_up) or None when no Amazon/Flipkart product."""
    for u in candidate_urls(urls):
        key = key_from_url(u)
        final = u
        if not key:
            final = await resolve_short_link(u)
            key = key_from_url(final) if final else None
        if key:
            if key.startswith("amazon_"):
                return key, f"https://www.amazon.in/dp/{key.split('_', 1)[1]}"
            return key, final
    # Unresolved short link from a known shop: still look it up, dedup by link hash
    for u in candidate_urls(urls):
        h = _host(u)
        if _in(h, AMAZON_DOMAINS) or _in(h, FLIPKART_DOMAINS):
            return f"url_{sha(u.split('?')[0])}", u
    return None


# ================= SPENDMITRA LOOKUP =================
class LookupFailed(Exception):
    pass


class LookupGate:
    """Spaces lookups out, caps concurrency and honours Retry-After on 429."""

    def __init__(self, per_min, concurrency):
        self.interval = 60.0 / per_min
        self.sem = asyncio.Semaphore(concurrency)
        self.lock = asyncio.Lock()
        self.next_at = 0.0
        self.pause_until = 0.0

    async def wait_turn(self):
        async with self.lock:
            now = time.monotonic()
            start = max(now, self.next_at, self.pause_until)
            self.next_at = start + self.interval
        if start > now:
            await asyncio.sleep(start - now)
        while time.monotonic() < self.pause_until:
            await asyncio.sleep(1)


GATE = None
PENDING = {"n": 0}
STATS_CACHE = OrderedDict()


def parse_ts(s):
    ts = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


def compute_stats(history, now=None):
    """-> (stats, None) or (None, reason). Daily median over the last 180 days."""
    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(days=HISTORY_DAYS)
    pts = []
    for h in history or []:
        try:
            ts, price = parse_ts(h["timestamp"]), float(h["price"])
        except (KeyError, ValueError, TypeError):
            continue
        if price > 0 and ts >= cutoff:
            pts.append((ts, price))
    if not pts:
        return None, "history: no data in last 6 months"
    pts.sort()
    by_day = {}
    for ts, price in pts:
        by_day[ts.date()] = price  # last price of each day
    days = sorted(by_day)
    if len(days) < MIN_HISTORY_DAYS:
        return None, f"history: only {len(days)} days of data"
    span = (now.date() - days[0]).days
    if span < MIN_SPAN_DAYS:
        return None, f"history: only {span} days deep"
    gap = max((b - a).days for a, b in zip(days, days[1:] + [now.date()]))
    if gap > MAX_GAP_DAYS:
        return None, f"history: {gap}-day gap"
    return {
        "median": statistics.median(by_day[d] for d in days),
        "low": min(p for _, p in pts),
        "days": len(days),
        "span": span,
    }, None


async def fetch_history(url):
    """Call SpendMitra. Retries on 429 / timeouts. Raises LookupFailed."""
    async with GATE.sem:
        last = "unknown"
        for attempt in range(3):
            await GATE.wait_turn()
            try:
                r = await HTTP.post(
                    SPENDMITRA_URL,
                    json={"url": url},
                    timeout=httpx.Timeout(LOOKUP_TIMEOUT, connect=15),
                )
            except (httpx.TimeoutException, httpx.TransportError) as e:
                last = f"timeout/network ({type(e).__name__})"
                await asyncio.sleep(2)
                continue
            if r.status_code == 429:
                try:
                    wait = float(r.headers.get("Retry-After", 35))
                except ValueError:
                    wait = 35.0
                GATE.pause_until = max(GATE.pause_until, time.monotonic() + wait)
                COUNTERS["rate_limited"] += 1
                last = "rate limited"
                continue
            if r.status_code >= 500:
                last = f"server error {r.status_code}"
                await asyncio.sleep(3)
                continue
            try:
                body = r.json()
            except ValueError:
                raise LookupFailed("bad response")
            if not body.get("success"):
                raise LookupFailed(f"api: {str(body.get('msg'))[:80]}")
            data = body.get("data") or {}
            return data.get("priceHistory") or []
        raise LookupFailed(last)


async def get_stats(product_key, url):
    """Cached history stats for a product. Returns (stats, reason)."""
    now = time.time()
    cached = STATS_CACHE.get(product_key)
    if cached and cached[0] > now:
        return cached[1]
    if PENDING["n"] >= MAX_PENDING_LOOKUPS:
        COUNTERS["queue_full"] += 1
        return None, "lookup: queue full"
    PENDING["n"] += 1
    try:
        stats, reason = compute_stats(await fetch_history(url))
    except LookupFailed as e:
        stats, reason = None, f"lookup: {e}"
    finally:
        PENDING["n"] -= 1
    # transient failures retry soon, thin history is remembered longer
    ttl = STATS_CACHE_HOURS * 3600 if stats else (300 if (reason or "").startswith("lookup") else 3600)
    STATS_CACHE[product_key] = (now + ttl, (stats, reason))
    while len(STATS_CACHE) > 3000:
        STATS_CACHE.popitem(last=False)
    return stats, reason


def evaluate(price, stats):
    """-> (ok, reason, pct_below_median)."""
    pct = (1 - price / stats["median"]) * 100
    if price < stats["low"] * MIN_PRICE_VS_LOW:
        return False, "price far below 6M low (likely misparsed)", pct
    if pct < MIN_MEDIAN_DISCOUNT:
        return False, f"only {pct:.0f}% below median", pct
    return True, "ok", pct


def price_line(stats, pct):
    return f"📉 6M Low: {inr(stats['low'])} | Median: {inr(stats['median'])} | Ab: {pct:.0f}% median se neeche"


# ================= DEDUP (memory + Supabase) =================
RECENT = OrderedDict()  # product_key -> time forwarded
PROCESSED_IDS = set()
PROCESSED_ORDER = deque()
LAST_SEEN = {}
TASKS = set()


class KeyedLock:
    """One lock per product key, cleaned up when unused."""

    def __init__(self):
        self._locks, self._users = {}, Counter()

    @asynccontextmanager
    async def hold(self, key):
        lock = self._locks.setdefault(key, asyncio.Lock())
        self._users[key] += 1
        try:
            async with lock:
                yield
        finally:
            self._users[key] -= 1
            if self._users[key] <= 0:
                self._locks.pop(key, None)
                del self._users[key]


KEY_LOCKS = KeyedLock()


def _db_is_dup(product_key):
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=DEDUP_HOURS)).isoformat()
    res = (
        supabase.table("forwarded_deals")
        .select("id")
        .eq("product_key", product_key)
        .gte("created_at", cutoff)
        .limit(1)
        .execute()
    )
    return bool(res.data)


async def is_duplicate(product_key):
    ts = RECENT.get(product_key)
    if ts and time.time() - ts < DEDUP_HOURS * 3600:
        return True
    try:
        return await asyncio.to_thread(_db_is_dup, product_key)
    except Exception as e:
        log(f"❌ DUP CHECK ERROR\n{repr(e)}", key="dup_err")
        return False


def _db_save(product_key, product_name, source):
    supabase.table("forwarded_deals").insert(
        {
            "product_key": product_key,
            "product_name": product_name,
            "source_channel": source,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
    ).execute()


async def save_deal(product_key, product_name, source):
    RECENT[product_key] = time.time()
    while len(RECENT) > 5000:
        RECENT.popitem(last=False)
    try:
        await asyncio.to_thread(_db_save, product_key, product_name, source)
    except Exception as e:
        log(f"❌ SAVE ERROR\n{repr(e)}", key="save_err")


# ================= SENDING TO EXTRAPE =================
SEND_LOCK = None
LAST_SEND = [0.0]


async def send_to_extrape(msg, text, line):
    """Forward original text/media (hidden links kept) with the price line added."""
    async with SEND_LOCK:
        wait = EXTRAPE_GAP_SEC - (time.monotonic() - LAST_SEND[0])
        if wait > 0:
            await asyncio.sleep(wait)
        has_media = bool(msg.media) and not isinstance(msg.media, MessageMediaWebPage)
        ents = msg.entities or None
        bodies = [text + "\n\n" + line, text] if line else [text]
        for i, body in enumerate(bodies):
            try:
                if has_media:
                    await client.send_file(DEST_BOT, msg.media, caption=body, formatting_entities=ents)
                else:
                    await client.send_message(DEST_BOT, body, formatting_entities=ents)
                break
            except (errors.MediaCaptionTooLongError, errors.MessageTooLongError):
                if i == len(bodies) - 1:
                    raise  # even the original does not fit
        LAST_SEND[0] = time.monotonic()


# ================= MAIN PROCESSOR =================
async def process_message(msg, entity):
    try:
        uid = f"{entity.id}_{msg.id}"
        if uid in PROCESSED_IDS:
            return
        PROCESSED_IDS.add(uid)
        PROCESSED_ORDER.append(uid)
        while len(PROCESSED_ORDER) > 5000:
            PROCESSED_IDS.discard(PROCESSED_ORDER.popleft())
        COUNTERS["seen"] += 1

        text = msg.message or ""
        if not text.strip():
            return
        if msg.date and (datetime.now(timezone.utc) - msg.date).total_seconds() > MAX_MESSAGE_AGE_SEC:
            COUNTERS["too_old"] += 1
            return

        parsed = parse_deal(text)
        claimed = claimed_discount(parsed)

        # ---- wall 1: discount shown in the post ----
        if claimed is not None and claimed < MIN_MRP_DISCOUNT:
            COUNTERS["wall_mrp"] += 1
            return
        if not parsed["price"]:
            COUNTERS["no_price"] += 1
            return

        product = await resolve_product(collect_urls(msg))
        if not product:
            COUNTERS["no_product_url"] += 1
            return
        product_key, lookup_url = product

        source = getattr(entity, "username", None) or str(entity.id)
        async with KEY_LOCKS.hold(product_key):
            if await is_duplicate(product_key):
                COUNTERS["duplicate"] += 1
                return

            # ---- wall 2: price history ----
            stats, reason = await get_stats(product_key, lookup_url)
            if not stats:
                COUNTERS["lookup_failed"] += 1
                if DRY_RUN:
                    log(f"🧪 SKIP (no verdict)\n{product_key}\n{reason}", key=f"skip_{reason[:12]}")
                return
            ok, why, pct = evaluate(parsed["price"], stats)
            if not ok:
                COUNTERS["rejected"] += 1
                if DRY_RUN:
                    log(f"🧪 REJECTED\n{product_key}\nprice {inr(parsed['price'])} | {why}")
                return

            line = price_line(stats, pct)
            if DRY_RUN:
                COUNTERS["would_forward"] += 1
                log(f"🧪 WOULD FORWARD\n{product_key}\nprice {inr(parsed['price'])}\n{line}")
                return

            await send_to_extrape(msg, text, line)
            await save_deal(product_key, normalize(text)[:200], source)
            COUNTERS["forwarded"] += 1
            if LOG_FORWARDS:
                log(f"✅ FORWARDED\n{product_key} @ {inr(parsed['price'])}\n{line}")
    except Exception as e:
        COUNTERS["errors"] += 1
        log(f"❌ PROCESS ERROR\n{repr(e)}", key="process_err")


def spawn(coro):
    task = asyncio.create_task(coro)
    TASKS.add(task)
    task.add_done_callback(TASKS.discard)


# ================= POLLING =================
async def poll_channel(entity):
    try:
        messages = []
        async for msg in client.iter_messages(
            entity, min_id=LAST_SEEN.get(entity.id, 0), limit=20, reverse=True
        ):
            messages.append(msg)
        for msg in messages:
            LAST_SEEN[entity.id] = max(LAST_SEEN.get(entity.id, 0), msg.id)
            spawn(process_message(msg, entity))
    except Exception as e:
        log(f"❌ POLL ERROR\n{entity.id}\n{repr(e)}", key=f"poll_{entity.id}")


async def polling_loop(entities):
    while True:
        for ent in entities:
            await poll_channel(ent)
        await asyncio.sleep(15)


async def summary_loop():
    last = Counter()
    while True:
        await asyncio.sleep(SUMMARY_MINUTES * 60)
        delta = {k: v - last.get(k, 0) for k, v in COUNTERS.items() if v - last.get(k, 0)}
        last = Counter(COUNTERS)
        parts = " | ".join(f"{k}: {v}" for k, v in sorted(delta.items())) or "no activity"
        log(f"📊 LAST {SUMMARY_MINUTES} MIN\n{parts}")


# ================= MAIN =================
def need(name):
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def parse_sources(raw):
    out = []
    for ch in raw.split(","):
        ch = ch.strip()
        if ch:
            out.append(ch if ch.startswith("@") else int(ch))
    return out


async def main():
    global client, supabase, HTTP, DEST_BOT, LOG_CHANNEL_ID, GATE, SEND_LOCK, LOG_QUEUE

    api_id = int(need("TG_API_ID"))
    api_hash = need("TG_API_HASH")
    session = need("SESSION_STRING")
    sources = parse_sources(need("SOURCE_CHANNELS"))
    DEST_BOT = need("EXTRAPE_BOT")
    LOG_CHANNEL_ID = int(need("LOG_CHANNEL_ID"))
    supabase = create_client(need("SUPABASE_URL"), need("SUPABASE_KEY"))

    HTTP = httpx.AsyncClient()
    GATE = LookupGate(LOOKUPS_PER_MIN, LOOKUP_CONCURRENCY)
    SEND_LOCK = asyncio.Lock()
    LOG_QUEUE = asyncio.Queue(maxsize=100)
    client = TelegramClient(StringSession(session), api_id, api_hash)

    print("Starting LootSpy...", flush=True)
    await client.start()
    me = await client.get_me()
    asyncio.create_task(log_worker())
    log(
        f"🚀 USERBOT STARTED{' (DRY RUN)' if DRY_RUN else ''}\n"
        f"Logged in as: {me.first_name}\n"
        f"Wall 1: {MIN_MRP_DISCOUNT:.0f}% off MRP | Wall 2: {MIN_MEDIAN_DISCOUNT:.0f}% below 6M median"
    )

    entities = []
    for ch in sources:
        try:
            ent = await client.get_entity(ch)
            entities.append(ent)
            latest = await client.get_messages(ent, limit=1)
            LAST_SEEN[ent.id] = latest[0].id if latest else 0
        except Exception as e:
            log(f"❌ SOURCE FAILED\n{ch}\n{repr(e)}")
    if not entities:
        log("❌ NO VALID SOURCE CHANNELS")
        return
    log(f"👀 WATCHING {len(entities)} CHANNELS\n➡️ FORWARDING TO {DEST_BOT}")

    async def handler(event):
        chat = await event.get_chat()
        LAST_SEEN[chat.id] = max(LAST_SEEN.get(chat.id, 0), event.message.id)
        spawn(process_message(event.message, chat))

    client.add_event_handler(handler, events.NewMessage(chats=entities))
    asyncio.create_task(polling_loop(entities))
    asyncio.create_task(summary_loop())

    print("LootSpy running...", flush=True)
    await client.run_until_disconnected()


if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    asyncio.run(main())

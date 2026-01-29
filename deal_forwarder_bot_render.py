import os
import re
import hashlib
import asyncio
import threading
from datetime import datetime, timedelta, timezone, date
from typing import Optional, List, Dict, Tuple

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, RPCError
from dotenv import load_dotenv
from flask import Flask, jsonify

from supabase import create_client, Client
from postgrest.exceptions import APIError

# ================= ENV =================
load_dotenv()

def must_getenv(name: str) -> str:
    v = os.getenv(name)
    if not v or not v.strip():
        raise RuntimeError(f"Missing required env var: {name}")
    return v.strip()

API_ID = int(must_getenv("TG_API_ID"))
API_HASH = must_getenv("TG_API_HASH")
SESSION_STRING = must_getenv("SESSION_STRING")

SOURCE_CHANNELS_RAW = must_getenv("SOURCE_CHANNELS")   # comma-separated: @usernames and/or -100ids
DEST_BOT = must_getenv("EXTRAPE_BOT")                  # @ExtraPeBot (or any bot/user entity)

LOG_CHANNEL_ID = int(os.getenv("LOG_CHANNEL_ID", "-1003060200056"))
PORT = int(os.getenv("PORT", "10000"))

SUPABASE_URL = must_getenv("SUPABASE_URL")
SUPABASE_KEY = must_getenv("SUPABASE_KEY")

# Dedup policy: 1 forward per product_key per day (DB enforces via unique index)
# Table must have: product_key (text not null), product_name, source_channel, day_bucket (date), created_at (timestamptz)
# Unique index: (product_key, day_bucket)
# ================= WEB SERVER =================
app = Flask(__name__)

@app.route("/")
def home():
    return "Userbot running", 200

@app.route("/ping")
def ping():
    return jsonify({"status": "ok", "time": datetime.now(timezone.utc).isoformat()})

def run_web():
    app.run(host="0.0.0.0", port=PORT)

# ================= TELEGRAM =================
client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

# ================= SUPABASE =================
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ================= HELPERS =================
def parse_sources(raw: str) -> List[object]:
    result: List[object] = []
    for ch in (raw or "").split(","):
        ch = ch.strip()
        if not ch:
            continue
        if ch.startswith("@"):
            result.append(ch)
        else:
            result.append(int(ch))
    return result

SOURCE_CHANNELS = parse_sources(SOURCE_CHANNELS_RAW)

async def safe_sleep(seconds: int):
    try:
        await asyncio.sleep(seconds)
    except Exception:
        pass

async def log(msg: str):
    # Always print locally + attempt TG log
    print(msg, flush=True)
    try:
        await client.send_message(LOG_CHANNEL_ID, msg)
    except Exception as e:
        print(f"[LOG SEND FAIL] {repr(e)}", flush=True)

def normalize_text(text: str) -> str:
    text = (text or "").lower()
    text = re.sub(r"[^\w\s]", " ", text, flags=re.UNICODE)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def text_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

_URL_RE = re.compile(r"(https?://[^\s<>\"]+)", re.IGNORECASE)

def extract_urls(text: str) -> List[str]:
    if not text:
        return []
    urls: List[str] = []
    for m in _URL_RE.finditer(text):
        u = m.group(1).strip()
        u = u.rstrip(").,;!?:]}'\"")  # strip common trailing punctuation
        urls.append(u)
    return urls

def extract_product_key(url: str) -> Optional[str]:
    if not url:
        return None

    # Amazon
    m = re.search(r"/dp/([A-Z0-9]{10})(?:[/?]|$)", url)
    if m:
        return f"amazon_{m.group(1)}"

    m = re.search(r"/gp/product/([A-Z0-9]{10})(?:[/?]|$)", url)
    if m:
        return f"amazon_{m.group(1)}"

    m = re.search(r"(?:asin=|ASIN=)([A-Z0-9]{10})", url)
    if m:
        return f"amazon_{m.group(1)}"

    # Flipkart
    m = re.search(r"(?:\?|&)pid=([A-Z0-9]{8,20})(?:&|$)", url, re.IGNORECASE)
    if m:
        return f"flipkart_{m.group(1)}"

    m = re.search(r"(itm[a-zA-Z0-9]{6,})(?:[/?]|$)", url)
    if m:
        return f"flipkart_{m.group(1)}"

    return None

def message_link(chat, msg_id: int) -> Optional[str]:
    """
    Public: https://t.me/<username>/<msg_id>
    Private/supergroup without username: https://t.me/c/<internal>/<msg_id>
    """
    try:
        username = getattr(chat, "username", None)
        if username:
            return f"https://t.me/{username}/{msg_id}"

        cid = getattr(chat, "id", None)
        if cid is None:
            return None

        internal = abs(int(cid)) - 1000000000000
        if internal > 0:
            return f"https://t.me/c/{internal}/{msg_id}"
    except Exception:
        return None
    return None

def source_display(chat) -> Tuple[str, str]:
    """
    Returns:
      src_pretty: '@username' or 'Title' or id
      src_tag: best identifier for DB
    """
    username = getattr(chat, "username", None)
    title = getattr(chat, "title", None)
    if username:
        return f"@{username}", username
    if title:
        return title, str(getattr(chat, "id", "unknown"))
    return str(getattr(chat, "id", "unknown")), str(getattr(chat, "id", "unknown"))

# ================= DB INSERT-FIRST (ATOMIC DEDUP) =================
def _db_try_insert_sync(product_key: str, product_name: str, source_channel: str) -> bool:
    """
    Inserts a row for today. Returns True if inserted (NEW).
    Returns False if duplicate for today (unique violation on (product_key, day_bucket)).
    """
    try:
        supabase.table("forwarded_deals").insert({
            "product_key": product_key,
            "product_name": product_name or "",
            "source_channel": source_channel or "",
            "day_bucket": date.today().isoformat(),
        }).execute()
        return True
    except APIError as e:
        # Postgres unique violation code
        if "23505" in str(e):
            return False
        raise

# ================= FORWARDING =================
async def forward_text(dest_entity, text: str):
    if not (text or "").strip():
        return
    await client.send_message(dest_entity, text)

async def forward_media(dest_entity, media, caption: str):
    await client.send_file(dest_entity, media, caption=caption or None)

async def forward_album(dest_entity, messages):
    files = []
    caption = None

    for m in messages:
        if getattr(m, "message", None):
            caption = m.message
            break

    for m in messages:
        if getattr(m, "media", None):
            files.append(m.media)

    if files:
        await client.send_file(dest_entity, files, caption=caption or None)
    elif caption:
        await forward_text(dest_entity, caption)

# ================= MAIN =================
async def main():
    await client.start()
    await log("🚀 Userbot started")

    # Resolve destination once
    try:
        dest_entity = await client.get_entity(DEST_BOT)
    except Exception as e:
        await log(f"❌ Cannot resolve DEST_BOT {DEST_BOT}\n{repr(e)}")
        return

    # Resolve sources
    source_entities = []
    for ch in SOURCE_CHANNELS:
        try:
            ent = await client.get_entity(ch)
            source_entities.append(ent)
            await log(f"✅ Source resolved: {ch}")
        except Exception as e:
            await log(f"❌ Cannot access source {ch}\n{repr(e)}")

    if not source_entities:
        await log("❌ No valid source channels. Exiting.")
        return

    await log(f"👀 Watching: {SOURCE_CHANNELS}")
    await log(f"➡️ Forwarding to: {DEST_BOT}")

    # --- Album handler (grouped media)
    @client.on(events.Album(chats=source_entities))
    async def album_handler(event: events.Album.Event):
        chat = await event.get_chat()
        src_pretty, src_tag = source_display(chat)

        first_id = event.messages[0].id if event.messages else None
        link = message_link(chat, first_id) if first_id else None

        all_text = " ".join([(m.message or "") for m in event.messages if m.message])
        urls = extract_urls(all_text)

        product_key = None
        for u in urls:
            product_key = extract_product_key(u)
            if product_key:
                break
        if not product_key:
            product_key = f"album_{text_hash(normalize_text(all_text))}"

        # INSERT FIRST (atomic). If duplicate => skip.
        try:
            inserted = await asyncio.to_thread(
                _db_try_insert_sync,
                product_key,
                normalize_text(all_text)[:200],
                src_tag
            )
        except Exception as e:
            await log(f"❌ DB INSERT ERROR (album)\nfrom={src_pretty}\nkey={product_key}\n{repr(e)}")
            return

        if not inserted:
            await log(f"🔁 DUPLICATE SKIPPED (album)\nfrom={src_pretty}\nkey={product_key}\nlink={link or 'n/a'}")
            return

        # Forward after insert succeeded
        try:
            try:
                await forward_album(dest_entity, event.messages)
            except FloodWaitError as fw:
                await log(f"⏳ FloodWait {fw.seconds}s (album). Sleeping then retry.\nfrom={src_pretty}")
                await safe_sleep(int(fw.seconds) + 1)
                await forward_album(dest_entity, event.messages)

            await log(f"✅ FORWARDED (album)\nfrom={src_pretty}\nkey={product_key}\nlink={link or 'n/a'}")

        except RPCError as e:
            await log(f"❌ RPC ERROR (album)\nfrom={src_pretty}\nkey={product_key}\nlink={link or 'n/a'}\n{repr(e)}")
        except Exception as e:
            await log(f"❌ HANDLER ERROR (album)\nfrom={src_pretty}\nkey={product_key}\nlink={link or 'n/a'}\n{repr(e)}")

    # --- Single message handler (skip grouped media)
    @client.on(events.NewMessage(chats=source_entities))
    async def handler(event: events.NewMessage.Event):
        msg = event.message
        if getattr(msg, "grouped_id", None):
            # album handler will process
            return

        chat = await event.get_chat()
        src_pretty, src_tag = source_display(chat)
        link = message_link(chat, msg.id)

        text = msg.message or ""
        urls = extract_urls(text)

        product_key = None
        for u in urls:
            product_key = extract_product_key(u)
            if product_key:
                break

        if not product_key:
            normalized = normalize_text(text)
            # If media-only message with no text, make key stable-ish
            if msg.media and not normalized:
                normalized = f"media_only_{event.chat_id}_{msg.id}"
            product_key = f"text_{text_hash(normalized)}"

        # INSERT FIRST (atomic). If duplicate => skip.
        try:
            inserted = await asyncio.to_thread(
                _db_try_insert_sync,
                product_key,
                normalize_text(text)[:200],
                src_tag
            )
        except Exception as e:
            await log(f"❌ DB INSERT ERROR\nfrom={src_pretty}\nkey={product_key}\nlink={link or 'n/a'}\n{repr(e)}")
            return

        if not inserted:
            await log(f"🔁 DUPLICATE SKIPPED\nfrom={src_pretty}\nkey={product_key}\nlink={link or 'n/a'}")
            return

        # Forward after insert succeeded
        try:
            try:
                if msg.media:
                    await forward_media(dest_entity, msg.media, text)
                else:
                    if text.strip():
                        await forward_text(dest_entity, text)
                    else:
                        await log(f"⚠️ SKIP empty message\nfrom={src_pretty}\nlink={link or 'n/a'}")
                        return
            except FloodWaitError as fw:
                await log(f"⏳ FloodWait {fw.seconds}s. Sleeping then retry.\nfrom={src_pretty}")
                await safe_sleep(int(fw.seconds) + 1)
                if msg.media:
                    await forward_media(dest_entity, msg.media, text)
                else:
                    await forward_text(dest_entity, text)

            await log(f"✅ FORWARDED\nfrom={src_pretty}\nkey={product_key}\nlink={link or 'n/a'}")

        except RPCError as e:
            await log(f"❌ RPC ERROR\nfrom={src_pretty}\nkey={product_key}\nlink={link or 'n/a'}\n{repr(e)}")
        except Exception as e:
            await log(f"❌ HANDLER ERROR\nfrom={src_pretty}\nkey={product_key}\nlink={link or 'n/a'}\n{repr(e)}")

    await client.run_until_disconnected()

# ================= ENTRY =================
if __name__ == "__main__":
    threading.Thread(target=run_web, daemon=True).start()
    asyncio.run(main())

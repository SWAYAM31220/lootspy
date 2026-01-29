import os
import re
import uuid
import hashlib
import asyncio
import threading
from datetime import datetime, timezone, date
from typing import Optional, List, Tuple

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
DEST_BOT = must_getenv("EXTRAPE_BOT")                  # @ExtraPeBot

LOG_CHANNEL_ID = int(os.getenv("LOG_CHANNEL_ID", "-1003060200056"))
PORT = int(os.getenv("PORT", "10000"))

SUPABASE_URL = must_getenv("SUPABASE_URL")
SUPABASE_KEY = must_getenv("SUPABASE_KEY")

INSTANCE_ID = uuid.uuid4().hex[:6]

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

def normalize_text(text: str) -> str:
    text = (text or "").lower()
    text = re.sub(r"[^\w\s]", " ", text, flags=re.UNICODE)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def text_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

# Better URL extraction (avoids trailing punctuation)
_URL_RE = re.compile(r"(https?://[^\s<>\"]+)", re.IGNORECASE)

def extract_urls(text: str) -> List[str]:
    if not text:
        return []
    urls: List[str] = []
    for m in _URL_RE.finditer(text):
        u = m.group(1).strip().rstrip(").,;!?:]}'\"")
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
    username = getattr(chat, "username", None)
    title = getattr(chat, "title", None)
    if username:
        return f"@{username}", username
    if title:
        return title, str(getattr(chat, "id", "unknown"))
    return str(getattr(chat, "id", "unknown")), str(getattr(chat, "id", "unknown"))

async def safe_sleep(seconds: int):
    try:
        await asyncio.sleep(seconds)
    except Exception:
        pass

# ================= BEAUTIFUL LOGGING (HTML) =================
def _escape_html(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

async def log_html(html: str):
    # Print a readable version to stdout too
    print(re.sub(r"<[^>]+>", "", html), flush=True)
    try:
        await client.send_message(LOG_CHANNEL_ID, html, parse_mode="html", link_preview=False)
    except Exception as e:
        print(f"[LOG SEND FAIL] {repr(e)}", flush=True)

def hdr(title: str) -> str:
    return f"🧾 <b>{_escape_html(title)}</b>  <code>{INSTANCE_ID}</code>"

def kv(k: str, v: str) -> str:
    return f"<b>{_escape_html(k)}:</b> {_escape_html(v)}"

def code_line(label: str, value: str) -> str:
    return f"<b>{_escape_html(label)}:</b> <code>{_escape_html(value)}</code>"

def link_line(link: Optional[str]) -> str:
    if not link:
        return "<b>Link:</b> n/a"
    return f"<b>Link:</b> <a href=\"{_escape_html(link)}\">open</a>"

# ================= DB (insert-first + rollback on forward fail) =================
def _db_try_insert_sync(product_key: str, product_name: str, source_channel: str) -> Tuple[bool, Optional[int]]:
    """
    Returns:
      (True, inserted_id) if inserted
      (False, None) if duplicate (unique violation)
    """
    try:
        res = supabase.table("forwarded_deals").insert({
            "product_key": product_key,
            "product_name": product_name or "",
            "source_channel": source_channel or "",
            "day_bucket": date.today().isoformat(),
        }).execute()

        inserted_id = None
        if res.data and isinstance(res.data, list) and len(res.data) > 0:
            inserted_id = res.data[0].get("id")
        return True, inserted_id

    except APIError as e:
        if "23505" in str(e):  # unique_violation
            return False, None
        raise

def _db_delete_by_id_sync(row_id: int) -> None:
    supabase.table("forwarded_deals").delete().eq("id", row_id).execute()

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
    await log_html(f"{hdr('Userbot started')}")

    # Resolve destination once
    try:
        dest_entity = await client.get_entity(DEST_BOT)
        await log_html(f"{hdr('Destination resolved')}\n{kv('To', DEST_BOT)}")
    except Exception as e:
        await log_html(f"❌ {hdr('DEST resolve failed')}\n{kv('To', DEST_BOT)}\n{code_line('Error', repr(e))}")
        return

    # Resolve sources
    source_entities = []
    for ch in SOURCE_CHANNELS:
        try:
            ent = await client.get_entity(ch)
            source_entities.append(ent)
            await log_html(f"✅ {hdr('Source resolved')}\n{kv('Source', str(ch))}")
        except Exception as e:
            await log_html(f"❌ {hdr('Source access failed')}\n{kv('Source', str(ch))}\n{code_line('Error', repr(e))}")

    if not source_entities:
        await log_html(f"❌ {hdr('No valid sources')}")
        return

    await log_html(
        f"{hdr('Watching')}\n"
        f"{kv('Sources', ', '.join(map(str, SOURCE_CHANNELS)))}\n"
        f"{kv('Forwarding to', DEST_BOT)}"
    )

    # ---- Album handler
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

        await log_html(
            f"📥 {hdr('RECEIVED (album)')}\n"
            f"{kv('From', src_pretty)}\n"
            f"{code_line('Key', product_key)}\n"
            f"{link_line(link)}"
        )

        # Insert-first
        try:
            inserted, row_id = await asyncio.to_thread(
                _db_try_insert_sync,
                product_key,
                normalize_text(all_text)[:200],
                src_tag
            )
        except Exception as e:
            await log_html(f"❌ {hdr('DB insert error (album)')}\n{kv('From', src_pretty)}\n{code_line('Key', product_key)}\n{code_line('Error', repr(e))}")
            return

        if not inserted:
            await log_html(f"🔁 {hdr('Duplicate skipped (album)')}\n{kv('From', src_pretty)}\n{code_line('Key', product_key)}\n{link_line(link)}")
            return

        # Forward and rollback DB on failure
        try:
            try:
                await forward_album(dest_entity, event.messages)
            except FloodWaitError as fw:
                await log_html(f"⏳ {hdr('FloodWait (album)')}\n{kv('Seconds', str(fw.seconds))}\n{kv('From', src_pretty)}\n{code_line('Key', product_key)}")
                await safe_sleep(int(fw.seconds) + 1)
                await forward_album(dest_entity, event.messages)

            await log_html(f"✅ {hdr('FORWARDED (album)')}\n{kv('From', src_pretty)}\n{code_line('Key', product_key)}\n{link_line(link)}")

        except Exception as e:
            # rollback reservation if we know the row id
            if row_id is not None:
                try:
                    await asyncio.to_thread(_db_delete_by_id_sync, row_id)
                except Exception as rollback_err:
                    await log_html(f"⚠️ {hdr('Rollback failed (album)')}\n{code_line('Row ID', str(row_id))}\n{code_line('Error', repr(rollback_err))}")

            await log_html(f"❌ {hdr('Forward failed (album)')}\n{kv('From', src_pretty)}\n{code_line('Key', product_key)}\n{link_line(link)}\n{code_line('Error', repr(e))}")

    # ---- Single message handler
    @client.on(events.NewMessage(chats=source_entities))
    async def handler(event: events.NewMessage.Event):
        msg = event.message
        if getattr(msg, "grouped_id", None):
            return  # album handler will handle

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
            if msg.media and not normalized:
                normalized = f"media_only_{event.chat_id}_{msg.id}"
            product_key = f"text_{text_hash(normalized)}"

        await log_html(
            f"📥 {hdr('RECEIVED')}\n"
            f"{kv('From', src_pretty)}\n"
            f"{code_line('Key', product_key)}\n"
            f"{link_line(link)}"
        )

        # Insert-first
        try:
            inserted, row_id = await asyncio.to_thread(
                _db_try_insert_sync,
                product_key,
                normalize_text(text)[:200],
                src_tag
            )
        except Exception as e:
            await log_html(f"❌ {hdr('DB insert error')}\n{kv('From', src_pretty)}\n{code_line('Key', product_key)}\n{link_line(link)}\n{code_line('Error', repr(e))}")
            return

        if not inserted:
            await log_html(f"🔁 {hdr('Duplicate skipped')}\n{kv('From', src_pretty)}\n{code_line('Key', product_key)}\n{link_line(link)}")
            return

        # Forward and rollback DB on failure
        try:
            try:
                if msg.media:
                    await forward_media(dest_entity, msg.media, text)
                else:
                    if text.strip():
                        await forward_text(dest_entity, text)
                    else:
                        await log_html(f"⚠️ {hdr('Empty message skipped')}\n{kv('From', src_pretty)}\n{link_line(link)}")
                        # rollback because we didn't forward
                        if row_id is not None:
                            await asyncio.to_thread(_db_delete_by_id_sync, row_id)
                        return

            except FloodWaitError as fw:
                await log_html(f"⏳ {hdr('FloodWait')}\n{kv('Seconds', str(fw.seconds))}\n{kv('From', src_pretty)}\n{code_line('Key', product_key)}")
                await safe_sleep(int(fw.seconds) + 1)
                if msg.media:
                    await forward_media(dest_entity, msg.media, text)
                else:
                    await forward_text(dest_entity, text)

            await log_html(f"✅ {hdr('FORWARDED')}\n{kv('From', src_pretty)}\n{code_line('Key', product_key)}\n{link_line(link)}")

        except (RPCError, Exception) as e:
            # rollback reservation
            if row_id is not None:
                try:
                    await asyncio.to_thread(_db_delete_by_id_sync, row_id)
                except Exception as rollback_err:
                    await log_html(f"⚠️ {hdr('Rollback failed')}\n{code_line('Row ID', str(row_id))}\n{code_line('Error', repr(rollback_err))}")

            await log_html(f"❌ {hdr('Forward failed')}\n{kv('From', src_pretty)}\n{code_line('Key', product_key)}\n{link_line(link)}\n{code_line('Error', repr(e))}")

    await client.run_until_disconnected()

# ================= ENTRY =================
if __name__ == "__main__":
    threading.Thread(target=run_web, daemon=True).start()
    asyncio.run(main())

import os
import re
import hashlib
import asyncio
import threading
from datetime import datetime, timedelta

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from dotenv import load_dotenv
from flask import Flask, jsonify
from supabase import create_client, Client

# ================= ENV =================
load_dotenv()

API_ID = int(os.getenv("TG_API_ID"))
API_HASH = os.getenv("TG_API_HASH")
SESSION_STRING = os.getenv("SESSION_STRING")

SOURCE_CHANNELS_RAW = os.getenv("SOURCE_CHANNELS")
DEST_BOT = os.getenv("EXTRAPE_BOT")
LOG_CHANNEL_ID = -1003060200056
PORT = int(os.getenv("PORT", 10000))

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# ================= WEB =================
app = Flask(__name__)

@app.route("/")
def home():
    return "lootspy running", 200

@app.route("/ping")
def ping():
    return jsonify({"status": "ok", "time": datetime.utcnow().isoformat()})

def run_web():
    app.run(host="0.0.0.0", port=PORT)

# ================= TELEGRAM =================
client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

# ================= SUPABASE =================
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ================= HELPERS =================
def parse_sources(raw):
    out = []
    for ch in raw.split(","):
        ch = ch.strip()
        if not ch:
            continue
        if ch.startswith("@"):
            out.append(ch)
        else:
            out.append(int(ch))
    return out

SOURCE_CHANNELS = parse_sources(SOURCE_CHANNELS_RAW)

async def log(msg):
    try:
        await client.send_message(LOG_CHANNEL_ID, msg)
    except:
        pass

def normalize(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()

def sha(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()

def extract_urls(text):
    return re.findall(r"https?://\S+", text or "")

def extract_product_key(text):
    for url in extract_urls(text):
        m = re.search(r"(pid=|/p/)(itm\w+)", url)
        if m:
            return f"flipkart_{m.group(2)}"

        m = re.search(r"/dp/([A-Z0-9]{10})", url)
        if m:
            return f"amazon_{m.group(1)}"

        m = re.search(r"/gp/product/([A-Z0-9]{10})", url)
        if m:
            return f"amazon_{m.group(1)}"

    return f"text_{sha(normalize(text))}"

async def is_duplicate(key):
    try:
        cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat()
        r = (
            supabase.table("forwarded_deals")
            .select("id")
            .eq("product_key", key)
            .gte("created_at", cutoff)
            .execute()
        )
        return bool(r.data)
    except:
        return False

async def save(key, name, source):
    try:
        supabase.table("forwarded_deals").insert({
            "product_key": key,
            "product_name": name,
            "source_channel": source,
            "created_at": datetime.utcnow().isoformat()
        }).execute()
    except:
        pass

# ================= CORE PROCESSOR =================
PROCESSED_IDS = set()

async def process_message(msg, entity):
    if msg.id in PROCESSED_IDS:
        return

    PROCESSED_IDS.add(msg.id)
    text = msg.message or ""
    key = extract_product_key(text)

    if await is_duplicate(key):
        return

    if msg.media:
        await client.send_file(DEST_BOT, msg.media, caption=text)
    else:
        if text.strip():
            await client.send_message(DEST_BOT, text)

    await save(
        key,
        normalize(text)[:200],
        entity.username or str(entity.id)
    )

    await log(f"✅ FORWARDED\n{key}")

# ================= POLLING =================
LAST_SEEN = {}

async def poll_channel(entity):
    last = LAST_SEEN.get(entity.id, 0)

    async for msg in client.iter_messages(entity, min_id=last, limit=5):
        LAST_SEEN[entity.id] = max(LAST_SEEN.get(entity.id, 0), msg.id)
        await process_message(msg, entity)

async def polling_loop(entities):
    while True:
        for ent in entities:
            try:
                await poll_channel(ent)
            except Exception as e:
                await log(f"❌ POLL ERROR {ent.id}\n{e}")
        await asyncio.sleep(20)

# ================= MAIN =================
async def main():
    await client.start()
    await log("🚀 lootspy started (HYBRID MODE)")

    ENTITIES = []
    for ch in SOURCE_CHANNELS:
        try:
            ent = await client.get_entity(ch)
            ENTITIES.append(ent)
            await log(f"✅ Source OK: {ch}")
        except Exception as e:
            await log(f"❌ Source FAIL: {ch}\n{e}")

    if not ENTITIES:
        await log("❌ No valid sources, exiting")
        return

    # EVENT LISTENER
    @client.on(events.NewMessage(chats=ENTITIES))
    async def handler(event):
        await process_message(event.message, event.chat)

    asyncio.create_task(polling_loop(ENTITIES))
    await client.run_until_disconnected()

# ================= ENTRY =================
if __name__ == "__main__":
    threading.Thread(target=run_web, daemon=True).start()
    asyncio.run(main())


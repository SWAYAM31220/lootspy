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

SOURCE_CHANNELS_RAW = os.getenv("SOURCE_CHANNELS")   # @usernames + -100IDs
DEST_BOT = os.getenv("EXTRAPE_BOT")                  # @ExtraPeBot
LOG_CHANNEL_ID = -1003060200056    # -100xxxx
PORT = int(os.getenv("PORT", 10000))

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# ================= WEB SERVER =================
app = Flask(__name__)

@app.route("/")
def home():
    return "Userbot running", 200

@app.route("/ping")
def ping():
    return jsonify({
        "status": "ok",
        "time": datetime.utcnow().isoformat()
    })

def run_web():
    app.run(host="0.0.0.0", port=PORT)

# ================= TELEGRAM =================
client = TelegramClient(
    StringSession(SESSION_STRING),
    API_ID,
    API_HASH
)

# ================= SUPABASE =================
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ================= HELPERS =================
def parse_sources(raw):
    result = []
    for ch in raw.split(","):
        ch = ch.strip()
        if not ch:
            continue
        if ch.startswith("@"):
            result.append(ch)
        else:
            result.append(int(ch))
    return result

SOURCE_CHANNELS = parse_sources(SOURCE_CHANNELS_RAW)

async def log(msg):
    try:
        await client.send_message(LOG_CHANNEL_ID, msg)
    except:
        pass

def normalize_text(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def text_hash(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()

def extract_urls(text: str):
    if not text:
        return []
    return re.findall(r"https?://\S+", text)

def extract_product_key(url: str):
    # Flipkart
    m = re.search(r"(pid=|/p/)(itm[a-zA-Z0-9]+)", url)
    if m:
        return f"flipkart_{m.group(2)}"

    # Amazon
    m = re.search(r"/dp/([A-Z0-9]{10})", url)
    if m:
        return f"amazon_{m.group(1)}"

    m = re.search(r"/gp/product/([A-Z0-9]{10})", url)
    if m:
        return f"amazon_{m.group(1)}"

    return None

async def is_duplicate(product_key: str) -> bool:
    try:
        cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat()
        res = (
            supabase.table("forwarded_deals")
            .select("id")
            .eq("product_key", product_key)
            .gte("created_at", cutoff)
            .execute()
        )
        return bool(res.data)
    except Exception as e:
        await log(f"❌ Supabase check error\n{repr(e)}")
        return False

async def save_deal(product_key: str, product_name: str, source: str):
    try:
        supabase.table("forwarded_deals").insert({
            "product_key": product_key,
            "product_name": product_name,
            "source_channel": source,
            "created_at": datetime.utcnow().isoformat()
        }).execute()
    except Exception as e:
        await log(f"❌ Supabase insert error\n{repr(e)}")

# ================= MAIN =================
async def main():
    await client.start()
    await log("🚀 Userbot started")

    SOURCE_ENTITIES = []

    for ch in SOURCE_CHANNELS:
        try:
            ent = await client.get_entity(ch)
            SOURCE_ENTITIES.append(ent)
            await log(f"✅ Source resolved: {ch}")
        except Exception as e:
            await log(f"❌ Cannot access source {ch}\n{repr(e)}")

    if not SOURCE_ENTITIES:
        await log("❌ No valid source channels. Exiting.")
        return

    await log(f"👀 Watching: {SOURCE_CHANNELS}")
    await log(f"➡️ Forwarding to: {DEST_BOT}")

    @client.on(events.NewMessage(chats=SOURCE_ENTITIES))
    async def handler(event):
        try:
            msg = event.message
            text = msg.message or ""

            urls = extract_urls(text)
            product_key = None

            for url in urls:
                product_key = extract_product_key(url)
                if product_key:
                    break

            if not product_key:
                normalized = normalize_text(text)
                product_key = f"text_{text_hash(normalized)}"

            if await is_duplicate(product_key):
                await log(f"🔁 DUPLICATE SKIPPED\n{product_key}")
                return

            if msg.media:
                await client.send_file(
                    DEST_BOT,
                    msg.media,
                    caption=text
                )
            else:
                if text.strip():
                    await client.send_message(DEST_BOT, text)
                else:
                    return

            await save_deal(
                product_key,
                normalize_text(text)[:200],
                event.chat.username or str(event.chat_id)
            )

            await log(f"✅ FORWARDED\n{product_key}")

        except Exception as e:
            await log(f"❌ HANDLER ERROR\n{repr(e)}")

    await client.run_until_disconnected()

# ================= ENTRY =================
if __name__ == "__main__":
    threading.Thread(target=run_web, daemon=True).start()
    asyncio.run(main())

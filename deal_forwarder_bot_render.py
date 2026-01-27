import os
import re
import asyncio
import threading
from datetime import datetime, timedelta

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from supabase import create_client, Client
from dotenv import load_dotenv
from flask import Flask

# ==================================================
# ENV LOAD
# ==================================================
load_dotenv()

API_ID = int(os.getenv("TG_API_ID"))
API_HASH = os.getenv("TG_API_HASH")
SESSION_STRING = os.getenv("SESSION_STRING", "")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

SOURCE_CHANNELS_RAW = os.getenv("SOURCE_CHANNELS", "")
EXTRAPE_BOT = os.getenv("EXTRAPE_BOT")           # @botusername or ID
LOG_CHANNEL_ID = -1003060200056 # -100xxxxxx

PORT = int(os.getenv("PORT", 10000))

# ==================================================
# WEB SERVICE (Render requirement)
# ==================================================
app = Flask(__name__)

@app.route("/")
def home():
    return "Userbot running", 200

def run_web():
    app.run(host="0.0.0.0", port=PORT)

# ==================================================
# PARSE SOURCE CHANNELS (IDs + usernames)
# ==================================================
def parse_source_channels(raw: str):
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

RAW_SOURCE_CHANNELS = parse_source_channels(SOURCE_CHANNELS_RAW)

# ==================================================
# TELEGRAM CLIENT
# ==================================================
client = TelegramClient(
    StringSession(SESSION_STRING) if SESSION_STRING else StringSession(),
    API_ID,
    API_HASH
)

# ==================================================
# SUPABASE
# ==================================================
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==================================================
# UTIL FUNCTIONS
# ==================================================
def normalize_product_name(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def extract_product_name(text: str) -> str:
    if not text:
        return ""
    lines = text.split("\n")
    for line in lines[:3]:
        if len(line.strip()) > 10:
            return normalize_product_name(line[:200])
    return normalize_product_name(text[:200])

async def log(msg: str):
    try:
        await client.send_message(LOG_CHANNEL_ID, msg)
    except Exception as e:
        print("[LOG ERROR]", e)

async def is_duplicate(product_name: str) -> bool:
    try:
        cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat()
        res = (
            supabase.table("forwarded_deals")
            .select("id")
            .eq("product_name", product_name)
            .gte("created_at", cutoff)
            .execute()
        )
        return bool(res.data)
    except Exception as e:
        await log(f"❌ Duplicate check error\n{repr(e)}")
        return False

async def save_deal(product_name: str, source: str):
    try:
        supabase.table("forwarded_deals").insert({
            "product_name": product_name,
            "source_channel": source,
            "created_at": datetime.utcnow().isoformat()
        }).execute()
    except Exception as e:
        await log(f"❌ Save deal error\n{repr(e)}")

async def cleanup_old():
    try:
        cutoff = (datetime.utcnow() - timedelta(hours=48)).isoformat()
        supabase.table("forwarded_deals").delete().lt(
            "created_at", cutoff
        ).execute()
        await log("🧹 Old records cleaned")
    except Exception as e:
        await log(f"❌ Cleanup error\n{repr(e)}")

# ==================================================
# TELEGRAM MAIN LOGIC
# ==================================================
async def telegram_main():
    await client.start()

    if not SESSION_STRING:
        sess = client.session.save()
        print("\nSAVE THIS SESSION STRING:\n", sess)
        await log("⚠️ SESSION_STRING missing, check logs")

    SOURCE_ENTITIES = []
    for ch in RAW_SOURCE_CHANNELS:
        try:
            ent = await client.get_entity(ch)
            SOURCE_ENTITIES.append(ent)
            await log(f"✅ Source resolved: {ch}")
        except Exception as e:
            await log(f"❌ Cannot access source {ch}\n{repr(e)}")

    if not SOURCE_ENTITIES:
        await log("❌ No valid source channels. Exiting.")
        return

    await log("🚀 Userbot started")
    await log(f"👀 Watching: {RAW_SOURCE_CHANNELS}")
    await log(f"➡️ Forwarding to: {EXTRAPE_BOT}")

    @client.on(events.NewMessage(chats=SOURCE_ENTITIES))
    async def handler(event):
        try:
            msg = event.message

            # text OR caption
            text = msg.message
            if not text:
                await log(f"⚠️ No text/caption | Chat {event.chat_id} | Msg {msg.id}")
                return

            product_name = extract_product_name(text)
            if len(product_name) < 5:
                await log(f"⚠️ Short product name | {text[:60]}")
                return

            if await is_duplicate(product_name):
                await log(f"🔁 Duplicate skipped\n{product_name}")
                return

            await client.forward_messages(EXTRAPE_BOT, msg)

            await save_deal(
                product_name,
                event.chat.username or str(event.chat_id)
            )

            await log(
                f"✅ FORWARDED\n"
                f"📦 {product_name}\n"
                f"📍 From: {event.chat.username or event.chat_id}\n"
                f"🆔 Msg ID: {msg.id}"
            )

        except Exception as e:
            await log(f"❌ Handler error\n{repr(e)}")

    async def periodic_cleanup():
        while True:
            await asyncio.sleep(3600)
            await cleanup_old()

    asyncio.create_task(periodic_cleanup())
    await client.run_until_disconnected()

# ==================================================
# ENTRYPOINT
# ==================================================
if __name__ == "__main__":
    threading.Thread(target=run_web, daemon=True).start()
    asyncio.run(telegram_main())

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
# Load env
# ==================================================
load_dotenv()

API_ID = int(os.getenv("TG_API_ID"))
API_HASH = os.getenv("TG_API_HASH")
SESSION_STRING = os.getenv("SESSION_STRING", "")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

EXTRAPE_BOT = os.getenv("EXTRAPE_BOT")
SOURCE_CHANNELS_RAW = os.getenv("SOURCE_CHANNELS", "")

PORT = int(os.getenv("PORT", 10000))

# ==================================================
# Minimal HTTP server (for Render Web Service)
# ==================================================
app = Flask(__name__)

@app.route("/")
def home():
    return "Userbot is running", 200

def run_web():
    app.run(host="0.0.0.0", port=PORT)

# ==================================================
# Parse source channels (IDs + usernames)
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
# Telegram client
# ==================================================
client = TelegramClient(
    StringSession(SESSION_STRING) if SESSION_STRING else StringSession(),
    API_ID,
    API_HASH
)

# ==================================================
# Supabase
# ==================================================
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==================================================
# Helpers
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
        print("[Supabase] duplicate check error:", e)
        return False

async def save_deal(product_name: str, source: str):
    try:
        supabase.table("forwarded_deals").insert({
            "product_name": product_name,
            "source_channel": source,
            "created_at": datetime.utcnow().isoformat()
        }).execute()
    except Exception as e:
        print("[Supabase] insert error:", e)

async def cleanup_old():
    try:
        cutoff = (datetime.utcnow() - timedelta(hours=48)).isoformat()
        supabase.table("forwarded_deals").delete().lt(
            "created_at", cutoff
        ).execute()
        print("[Cleanup] old records removed")
    except Exception as e:
        print("[Cleanup] error:", e)

# ==================================================
# Telegram logic
# ==================================================
async def telegram_main():
    await client.start()

    if not SESSION_STRING:
        print("\nSAVE THIS SESSION STRING:\n")
        print(client.session.save())
        print("\nAdd it to Render env as SESSION_STRING\n")

    SOURCE_ENTITIES = []
    for ch in RAW_SOURCE_CHANNELS:
        try:
            ent = await client.get_entity(ch)
            SOURCE_ENTITIES.append(ent)
            print(f"[OK] Resolved source: {ch}")
        except Exception as e:
            print(f"[ERROR] Cannot access {ch}: {e}")

    if not SOURCE_ENTITIES:
        raise RuntimeError("No valid source channels resolved")

    @client.on(events.NewMessage(chats=SOURCE_ENTITIES))
    async def handler(event):
        try:
            text = event.message.text
            if not text:
                return

            product_name = extract_product_name(text)
            if len(product_name) < 5:
                return

            if await is_duplicate(product_name):
                print("[SKIP] duplicate:", product_name[:40])
                return

            await client.forward_messages(EXTRAPE_BOT, event.message)
            await save_deal(
                product_name,
                event.chat.username or str(event.chat_id)
            )

            print("[FORWARDED]", product_name[:40])

        except Exception as e:
            print("[Handler error]", e)

    async def periodic_cleanup():
        while True:
            await asyncio.sleep(3600)
            await cleanup_old()

    asyncio.create_task(periodic_cleanup())
    print("Userbot running...")
    await client.run_until_disconnected()

# ==================================================
# Entrypoint
# ==================================================
if __name__ == "__main__":
    # Start web server in separate thread
    threading.Thread(target=run_web, daemon=True).start()

    # Start telegram bot
    asyncio.run(telegram_main())

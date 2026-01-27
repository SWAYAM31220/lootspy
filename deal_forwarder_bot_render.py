import os
import asyncio
import threading
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from dotenv import load_dotenv
from flask import Flask

# ================== ENV ==================
load_dotenv()

API_ID = int(os.getenv("TG_API_ID"))
API_HASH = os.getenv("TG_API_HASH")
SESSION_STRING = os.getenv("SESSION_STRING")

SOURCE_CHANNELS_RAW = os.getenv("SOURCE_CHANNELS")   # usernames + IDs
DEST_BOT = os.getenv("EXTRAPE_BOT")                  # @ExtraPeBot
LOG_CHANNEL_ID = -1003060200056    # -100xxxx
PORT = int(os.getenv("PORT", 10000))

# ================== WEB SERVER ==================
app = Flask(__name__)

@app.route("/")
def home():
    return "Userbot running", 200

def run_web():
    app.run(host="0.0.0.0", port=PORT)

# ================== TELETHON ==================
client = TelegramClient(
    StringSession(SESSION_STRING),
    API_ID,
    API_HASH
)

# ================== HELPERS ==================
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

# ================== MAIN ==================
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
            await log(f"❌ Cannot access source {ch}\n{e}")

    if not SOURCE_ENTITIES:
        await log("❌ No valid source channels. Exiting.")
        return

    await log(f"👀 Watching: {SOURCE_CHANNELS}")
    await log(f"➡️ Forwarding to: {DEST_BOT}")

    # ================== AUTO FORWARD ==================
    @client.on(events.NewMessage(chats=SOURCE_ENTITIES))
    async def handler(event):
        try:
            msg = event.message
            text = msg.message or ""

            # MEDIA (image / video / doc / gif)
            if msg.media:
                await client.send_file(
                    DEST_BOT,
                    msg.media,
                    caption=text
                )
                await log(
                    f"✅ SENT MEDIA\n"
                    f"From: {event.chat.username or event.chat_id}\n"
                    f"Msg ID: {msg.id}"
                )

            # TEXT ONLY
            else:
                if text.strip():
                    await client.send_message(DEST_BOT, text)
                    await log(
                        f"✅ SENT TEXT\n"
                        f"From: {event.chat.username or event.chat_id}\n"
                        f"Msg ID: {msg.id}"
                    )

        except Exception as e:
            await log(f"❌ SEND ERROR\n{repr(e)}")

    await client.run_until_disconnected()

# ================== ENTRY ==================
if __name__ == "__main__":
    threading.Thread(target=run_web, daemon=True).start()
    asyncio.run(main())

import os
import re
import hashlib
import asyncio
import threading
from datetime import datetime, timedelta

from flask import Flask, jsonify

from telethon import TelegramClient, events
from telethon.sessions import StringSession

from dotenv import load_dotenv

from supabase import create_client, Client

# ================= LOAD ENV =================
load_dotenv()

API_ID = int(os.getenv("TG_API_ID"))
API_HASH = os.getenv("TG_API_HASH")
SESSION_STRING = os.getenv("SESSION_STRING")

SOURCE_CHANNELS_RAW = os.getenv("SOURCE_CHANNELS")

DEST_BOT = os.getenv("EXTRAPE_BOT")

LOG_CHANNEL_ID = int(
    os.getenv("LOG_CHANNEL_ID")
)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

PORT = int(
    os.getenv("PORT", 5000)
)

# ================= FLASK =================
app = Flask(__name__)

@app.route("/")
def home():

    return {
        "status": "running",
        "service": "lootspy"
    }

@app.route("/ping")
def ping():

    return jsonify({
        "status": "alive"
    })

def run_flask():

    app.run(
        host="0.0.0.0",
        port=PORT
    )

# ================= TELEGRAM =================
client = TelegramClient(
    StringSession(SESSION_STRING),
    API_ID,
    API_HASH
)

# ================= SUPABASE =================
supabase: Client = create_client(
    SUPABASE_URL,
    SUPABASE_KEY
)

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

SOURCE_CHANNELS = parse_sources(
    SOURCE_CHANNELS_RAW
)

async def log(msg):

    try:

        print(msg)

        await client.send_message(
            LOG_CHANNEL_ID,
            msg
        )

    except Exception as e:

        print(
            "LOG ERROR:",
            e
        )

def normalize(text: str) -> str:

    text = text.lower()

    text = re.sub(
        r"[^\w\s]",
        " ",
        text
    )

    return re.sub(
        r"\s+",
        " ",
        text
    ).strip()

def sha(text: str) -> str:

    return hashlib.sha256(
        text.encode()
    ).hexdigest()

def extract_urls(text):

    return re.findall(
        r"https?://\S+",
        text or ""
    )

# ================= SMART PRODUCT KEY =================
def extract_product_key(text):

    urls = extract_urls(text)

    for url in urls:

        # ================= FLIPKART =================
        m = re.search(
            r"(pid=|/p/)(itm[a-zA-Z0-9]+)",
            url
        )

        if m:

            return (
                f"flipkart_{m.group(2)}"
            )

        # ================= AMAZON =================
        m = re.search(
            r"/dp/([A-Z0-9]{10})",
            url
        )

        if m:

            return (
                f"amazon_{m.group(1)}"
            )

        m = re.search(
            r"/gp/product/([A-Z0-9]{10})",
            url
        )

        if m:

            return (
                f"amazon_{m.group(1)}"
            )

    # ================= FALLBACK =================
    return (
        f"text_{sha(normalize(text))}"
    )

# ================= DUP CHECK =================
async def is_duplicate(product_key):

    try:

        cutoff = (
            datetime.utcnow()
            - timedelta(hours=24)
        ).isoformat()

        res = (
            supabase
            .table("forwarded_deals")
            .select("id")
            .eq(
                "product_key",
                product_key
            )
            .gte(
                "created_at",
                cutoff
            )
            .execute()
        )

        return bool(res.data)

    except Exception as e:

        await log(
            f"❌ DUP CHECK ERROR\n"
            f"{repr(e)}"
        )

        return False

# ================= SAVE =================
async def save_deal(
    product_key,
    product_name,
    source
):

    try:

        supabase.table(
            "forwarded_deals"
        ).insert({

            "product_key": product_key,

            "product_name": product_name,

            "source_channel": source,

            "created_at":
            datetime.utcnow().isoformat()

        }).execute()

    except Exception as e:

        await log(
            f"❌ SAVE ERROR\n"
            f"{repr(e)}"
        )

# ================= GLOBALS =================
PROCESSED_IDS = set()

LAST_SEEN = {}

# ================= MAIN PROCESSOR =================
async def process_message(
    msg,
    entity
):

    try:

        unique_msg_id = (
            f"{entity.id}_{msg.id}"
        )

        if unique_msg_id in PROCESSED_IDS:
            return

        PROCESSED_IDS.add(
            unique_msg_id
        )

        text = msg.message or ""

        if (
            not text.strip()
            and not msg.media
        ):
            return

        product_key = (
            extract_product_key(text)
        )

        # ================= DUP CHECK =================
        if await is_duplicate(product_key):

            await log(
                f"🔁 DUPLICATE SKIPPED\n"
                f"{product_key}"
            )

            return

        # ================= SEND =================
        if msg.media:

            await client.send_file(
                DEST_BOT,
                msg.media,
                caption=text
            )

        else:

            if text.strip():

                await client.send_message(
                    DEST_BOT,
                    text
                )

        # ================= SAVE =================
        await save_deal(

            product_key,

            normalize(text)[:200],

            entity.username
            or str(entity.id)

        )

        await log(
            f"✅ FORWARDED\n"
            f"{product_key}"
        )

    except Exception as e:

        await log(
            f"❌ PROCESS ERROR\n"
            f"{repr(e)}"
        )

# ================= POLLING =================
async def poll_channel(entity):

    try:

        last_id = LAST_SEEN.get(
            entity.id,
            0
        )

        messages = []

        async for msg in client.iter_messages(

            entity,

            min_id=last_id,

            limit=10,

            reverse=True

        ):

            messages.append(msg)

        for msg in messages:

            LAST_SEEN[entity.id] = max(

                LAST_SEEN.get(
                    entity.id,
                    0
                ),

                msg.id

            )

            await process_message(
                msg,
                entity
            )

    except Exception as e:

        await log(
            f"❌ POLL ERROR\n"
            f"{entity.id}\n"
            f"{repr(e)}"
        )

# ================= BACKGROUND LOOP =================
async def polling_loop(entities):

    while True:

        for ent in entities:

            await poll_channel(ent)

        await asyncio.sleep(15)

# ================= MAIN =================
async def main():

    print(
        "Starting LootSpy..."
    )

    await client.start()

    me = await client.get_me()

    await log(
        f"🚀 USERBOT STARTED\n"
        f"Logged in as: "
        f"{me.first_name}"
    )

    ENTITIES = []

    # ================= RESOLVE CHANNELS =================
    for ch in SOURCE_CHANNELS:

        try:

            ent = await client.get_entity(ch)

            ENTITIES.append(ent)

            # ================= START FROM LATEST =================
            latest = await client.get_messages(
                ent,
                limit=1
            )

            if latest:

                LAST_SEEN[ent.id] = (
                    latest[0].id
                )

            else:

                LAST_SEEN[ent.id] = 0

            await log(
                f"✅ SOURCE RESOLVED\n"
                f"{ch}"
            )

        except Exception as e:

            await log(
                f"❌ SOURCE FAILED\n"
                f"{ch}\n"
                f"{repr(e)}"
            )

    if not ENTITIES:

        await log(
            "❌ NO VALID SOURCE CHANNELS"
        )

        return

    await log(
        f"👀 WATCHING "
        f"{len(ENTITIES)} CHANNELS\n"
        f"➡️ FORWARDING TO "
        f"{DEST_BOT}"
    )

    # ================= EVENT LISTENER =================
    @client.on(
        events.NewMessage(
            chats=ENTITIES
        )
    )
    async def handler(event):

        await process_message(
            event.message,
            event.chat
        )

    # ================= START POLLING =================
    asyncio.create_task(
        polling_loop(ENTITIES)
    )

    print(
        "LootSpy running..."
    )

    await client.run_until_disconnected()

# ================= ENTRY =================
if __name__ == "__main__":

    threading.Thread(
        target=run_flask,
        daemon=True
    ).start()

    asyncio.run(main())

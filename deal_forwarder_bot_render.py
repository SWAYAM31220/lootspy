import os
import re
import asyncio
from datetime import datetime, timedelta
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.getenv('TG_API_ID'))
API_HASH = os.getenv('TG_API_HASH')
SESSION_STRING = os.getenv('SESSION_STRING', '')
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
SOURCE_CHANNELS = [ch.strip() for ch in os.getenv('SOURCE_CHANNELS').split(',')]
EXTRAPE_BOT = os.getenv('EXTRAPE_BOT')

if SESSION_STRING:
    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
else:
    client = TelegramClient(StringSession(), API_ID, API_HASH)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def normalize_product_name(text):
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r'[^\w\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    text = text.strip()
    return text

def extract_product_name(message_text):
    if not message_text:
        return ""
    lines = message_text.split('\n')
    
    for line in lines[:3]:
        if len(line.strip()) > 10:
            product_name = line.strip()[:200]
            return normalize_product_name(product_name)
    
    return normalize_product_name(message_text[:200])

async def is_duplicate(product_name):
    try:
        time_threshold = (datetime.utcnow() - timedelta(hours=24)).isoformat()
        
        response = supabase.table('forwarded_deals').select('*').eq('product_name', product_name).gte('created_at', time_threshold).execute()
        
        return len(response.data) > 0
    except Exception as e:
        print(f"Error checking duplicate: {e}")
        return False

async def save_deal(product_name, source_channel):
    try:
        supabase.table('forwarded_deals').insert({
            'product_name': product_name,
            'source_channel': source_channel,
            'created_at': datetime.utcnow().isoformat()
        }).execute()
    except Exception as e:
        print(f"Error saving deal: {e}")

async def cleanup_old_records():
    try:
        time_threshold = (datetime.utcnow() - timedelta(hours=48)).isoformat()
        supabase.table('forwarded_deals').delete().lt('created_at', time_threshold).execute()
        print(f"Cleaned up old records before {time_threshold}")
    except Exception as e:
        print(f"Error cleaning up: {e}")

@client.on(events.NewMessage(chats=SOURCE_CHANNELS))
async def handler(event):
    try:
        message_text = event.message.text
        
        if not message_text:
            return
        
        product_name = extract_product_name(message_text)
        
        if not product_name or len(product_name) < 5:
            return
        
        if await is_duplicate(product_name):
            print(f"Duplicate detected: {product_name[:50]}... - Skipping")
            return
        
        await client.forward_messages(EXTRAPE_BOT, event.message)
        
        await save_deal(product_name, event.chat.username or str(event.chat_id))
        
        print(f"Forwarded: {product_name[:50]}... from {event.chat.username or event.chat_id}")
        
    except Exception as e:
        print(f"Error in handler: {e}")

async def periodic_cleanup():
    while True:
        await asyncio.sleep(3600)
        await cleanup_old_records()

async def main():
    await client.start()
    
    if not SESSION_STRING:
        print("\n" + "="*50)
        print("IMPORTANT: Save this session string!")
        print("="*50)
        print(client.session.save())
        print("="*50)
        print("\nAdd this as SESSION_STRING in Render environment variables")
        print("\n")
    
    print("Bot started successfully!")
    print(f"Monitoring channels: {SOURCE_CHANNELS}")
    print(f"Forwarding to: {EXTRAPE_BOT}")
    
    asyncio.create_task(periodic_cleanup())
    
    await client.run_until_disconnected()

if __name__ == '__main__':
    asyncio.run(main())

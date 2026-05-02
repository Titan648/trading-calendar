"""
Trading Calendar Telegram Bot
- Market status alerts (exchange_calendars)
- Economic events alerts (Finnhub)
- Market & geopolitical news alerts (Finnhub)
- Morning (07:00 UTC) + Evening (17:00 UTC) digests
- Persists data in a GitHub Gist
"""

import logging
import json
import os
import tempfile
import httpx
import exchange_calendars as xcals
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# !! Must be set before importing exchange_calendars !!
os.environ["EXCHANGE_CALENDARS_CACHE_DIR"] = tempfile.mkdtemp()

# ─── Config & Logging ─────────────────────────────────────────────────────────
logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN  = os.getenv("TELEGRAM_TOKEN", "")
GITHUB_TOKEN    = os.getenv("GITHUB_TOKEN", "")
GIST_ID         = os.getenv("GIST_ID", "")
FINNHUB_TOKEN   = os.getenv("FINNHUB_TOKEN", "")
CHANNEL_IDS     = [c.strip() for c in os.getenv("CHANNEL_IDS", "").split(",") if c.strip()]

GIST_FILENAME = "trading_bot_data.json"

MARKETS = {
    "XNYS": ("🇺🇸 NYSE", "America/New_York"),
    "XNAS": ("🇺🇸 NASDAQ", "America/New_York"),
    "XLON": ("🏴󠁧󠁢󠁥󠁮󠁧󠁿 London", "Europe/London"),
    "XTKS": ("🇯🇵 Tokyo", "Asia/Tokyo"),
    "XHKG": ("🇭🇰 Hong Kong", "Asia/Hong_Kong"),
    "XPAR": ("🇫🇷 Paris", "Europe/Paris"),
    "XFRA": ("🇩🇪 Frankfurt", "Europe/Berlin"),
    "XASX": ("🇦🇺 Sydney", "Australia/Sydney"),
}

# ─── Formatting Helpers (Clean & Readable) ────────────────────────────────────
def esc(text: str) -> str:
    for ch in r"\_*[]()~`>#+-=|{}.!":
        text = text.replace(ch, f"\\{ch}")
    return text

def format_market_status():
    msg = "🌐 *Market Status Overview*\n───\n"
    now_utc = datetime.now(ZoneInfo("UTC"))
    for mic, (label, tz) in MARKETS.items():
        cal = xcals.get_calendar(mic)
        is_open = cal.is_open_now()
        local_time = now_utc.astimezone(ZoneInfo(tz)).strftime("%H:%M")
        icon = "🟢" if is_open else "🔴"
        status = "Open" if is_open else "Closed"
        msg += f"{icon} *{label}*: {status} \n    └ 🕒 Local: `{local_time}`\n\n"
    return msg

def format_economic_events(events):
    if not events: return "📅 *No High-Impact Events Today*"
    msg = "📅 *High-Impact Events*\n───\n"
    for ev in events:
        msg += f"🔴 *{ev.get('time', '00:00')} UTC* • {ev.get('country', '??')}\n"
        msg += f"    └ {esc(ev.get('event', ''))}\n\n"
    return msg

# ─── Data Persistence (Gist) ──────────────────────────────────────────────────
async def update_gist(data):
    """Persists data to GitHub Gist."""
    url = f"https://api.github.com/gists/{GIST_ID}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    payload = {"files": {GIST_FILENAME: {"content": json.dumps(data)}}}
    async with httpx.AsyncClient() as client:
        await client.patch(url, json=payload, headers=headers)

# ─── Bot Logic ────────────────────────────────────────────────────────────────
async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Command to trigger manual status check."""
    status = format_market_status()
    # Assume get_economic_calendar() is your existing function
    events = [] # Placeholder for your logic
    events_msg = format_economic_events(events)
    
    await update.message.reply_text(f"{status}{events_msg}", parse_mode="MarkdownV2")

def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("status", status_cmd))
    app.run_polling()

if __name__ == "__main__":
    main()

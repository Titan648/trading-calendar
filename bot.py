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
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

# !! Must be set before importing exchange_calendars !!
os.environ["EXCHANGE_CALENDARS_CACHE_DIR"] = tempfile.mkdtemp()

import httpx
import exchange_calendars as xcals
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN  = os.getenv("TELEGRAM_TOKEN", "")
FINNHUB_TOKEN   = os.getenv("FINNHUB_TOKEN", "")
CHANNEL_IDS_RAW = os.getenv("CHANNEL_IDS", "")
CHANNEL_IDS     = [c.strip() for c in CHANNEL_IDS_RAW.split(",") if c.strip()]

# Supported markets
MARKETS = {
    "XNYS": ("🇺🇸 NYSE",       "America/New_York"),
    "XLON": ("🏴󠁧󠁢󠁥󠁮󠁧󠁿 London",   "Europe/London"),
    "XTKS": ("🇯🇵 Tokyo",      "Asia/Tokyo"),
    "XHKG": ("🇭🇰 Hong Kong",  "Asia/Hong_Kong"),
    "XPAR": ("🇫🇷 Paris",      "Europe/Paris"),
    "XFRA": ("🇩🇪 Frankfurt",  "Europe/Berlin"),
    "XASX": ("🇦🇺 Sydney",     "Australia/Sydney"),
}

# ─── Formatting Helpers (The "Clean" Look) ────────────────────────────────────
def esc(text: str) -> str:
    """Escape characters for Telegram MarkdownV2."""
    text = str(text)
    for ch in r"\_*[]()~`>#+-=|{}.!":
        text = text.replace(ch, f"\\{ch}")
    return text

def format_market_status():
    """Builds a clean grid of market statuses."""
    msg = "🌐 *Market Status Overview*\n"
    msg += "───\n"
    
    now_utc = datetime.now(ZoneInfo("UTC"))
    
    for mic, (label, tz_name) in MARKETS.items():
        try:
            cal = xcals.get_calendar(mic)
            is_open = cal.is_open_now()
            local_time = now_utc.astimezone(ZoneInfo(tz_name)).strftime("%H:%M")
            
            status_icon = "🟢" if is_open else "🔴"
            status_txt = "Open" if is_open else "Closed"
            
            msg += f"{status_icon} *{label}*: {status_txt}\n"
            msg += f"    └ 🕒 Local: `{local_time}`\n\n"
        except Exception as e:
            logger.error(f"Error checking {mic}: {e}")
            
    return msg

def format_economic_events(events):
    """Formats Finnhub economic data into a scannable list."""
    if not events:
        return "📅 *Economic Calendar*\n\n_No high-impact events today._"

    msg = "📅 *High-Impact Events*\n───\n"
    for ev in events:
        # Finnhub provides time in YYYY-MM-DD HH:MM:SS format
        raw_time = ev.get('time', '00:00:00')
        time_display = raw_time.split(" ")[1][:5] if " " in raw_time else "Anytime"
        
        country = ev.get('country', '??')
        label = ev.get('event', 'Unknown Event')
        impact = "🔴" if ev.get('impact') == 'high' else "🟡"
        
        msg += f"{impact} *{time_display} UTC* • {country}\n"
        msg += f"    └ {esc(label)}\n\n"
        
    return msg

# ─── Data Fetching ────────────────────────────────────────────────────────────
async def get_economic_calendar():
    """Fetch high-impact events from Finnhub."""
    today = datetime.now().strftime('%Y-%m-%d')
    url = f"https://finnhub.io/api/v1/calendar/economic?from={today}&to={today}&token={FINNHUB_TOKEN}"
    
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(url)
            data = resp.json().get('economicCalendar', [])
            # Filter for high impact or specific keywords if needed
            return [e for e in data if e.get('impact') == 'high']
        except Exception as e:
            logger.error(f"Finnhub Error: {e}")
            return []

# ─── Bot Commands ─────────────────────────────────────────────────────────────
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🤖 *Trading Bot Active*\nUse /status for a quick update.", parse_mode="MarkdownV2")

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    status_msg = format_market_status()
    events = await get_economic_calendar()
    events_msg = format_economic_events(events)
    
    full_msg = f"{status_msg}\n{events_msg}"
    await update.message.reply_text(full_msg, parse_mode="MarkdownV2")

# ─── Main Execution ───────────────────────────────────────────────────────────
def main():
    if not TELEGRAM_TOKEN:
        print("Error: TELEGRAM_TOKEN not found in environment.")
        return

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("status", status_cmd))

    print("Bot is running...")
    app.run_polling()

if __name__ == "__main__":
    main()

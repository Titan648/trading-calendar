"""
Trading Calendar Telegram Bot
- Uses exchange_calendars Python library directly (no external API needed)
- Persists data in a GitHub Gist
- Sends daily market status alerts to users AND Telegram channels
"""

import logging
import json
import os
import calendar as cal_module
from datetime import datetime, date
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
import exchange_calendars as xcals
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)
from datetime import time as dtime

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN  = os.getenv("TELEGRAM_TOKEN", "")
GITHUB_TOKEN    = os.getenv("GITHUB_TOKEN", "")
GIST_ID         = os.getenv("GIST_ID", "")
CHANNEL_IDS_RAW = os.getenv("CHANNEL_IDS", "")
CHANNEL_IDS     = [c.strip() for c in CHANNEL_IDS_RAW.split(",") if c.strip()]

GIST_FILENAME = "trading_bot_data.json"

ALERT_HOUR   = 7
ALERT_MINUTE = 0

# ─── Supported markets ────────────────────────────────────────────────────────
# MIC code -> (display name, flag, timezone)
MARKETS = {
    "XNYS": ("🇺🇸 NYSE",          "America/New_York"),
    "XNAS": ("🇺🇸 NASDAQ",         "America/New_York"),
    "XLON": ("🏴󠁧󠁢󠁥󠁮󠁧󠁿 London",        "Europe/London"),
    "XTKS": ("🇯🇵 Tokyo",           "Asia/Tokyo"),
    "XHKG": ("🇭🇰 Hong Kong",       "Asia/Hong_Kong"),
    "XPAR": ("🇫🇷 Paris",           "Europe/Paris"),
    "XFRA": ("🇩🇪 Frankfurt",       "Europe/Berlin"),
    "XASX": ("🇦🇺 Sydney",          "Australia/Sydney"),
    "XSHG": ("🇨🇳 Shanghai",        "Asia/Shanghai"),
    "XBOM": ("🇮🇳 Mumbai",          "Asia/Kolkata"),
    "XKRX": ("🇰🇷 Seoul",           "Asia/Seoul"),
    "XTSE": ("🇨🇦 Toronto",         "America/Toronto"),
}

DEFAULT_MICS = ["XNYS", "XLON", "XTKS", "XHKG", "XPAR", "XFRA", "XASX", "XSHG"]


# ─── Market status logic ──────────────────────────────────────────────────────
def get_market_status(mic: str) -> dict:
    """Get current open/closed status for a market using exchange_calendars."""
    label, tz_name = MARKETS.get(mic, (mic, "UTC"))
    now_utc = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
    today   = now_utc.astimezone(ZoneInfo(tz_name)).date()

    try:
        cal = xcals.get_calendar(mic, start="2020-01-01", end="2030-12-31")
    except Exception:
        return {"mic": mic, "name": label, "status": "Unknown", "note": "Calendar not found"}

    is_session   = cal.is_session(today.isoformat())
    is_open_now  = False
    note         = ""
    local_time   = now_utc.astimezone(ZoneInfo(tz_name)).strftime("%H:%M")

    if is_session:
        # Check if currently within trading hours
        try:
            open_t  = cal.session_open(today.isoformat()).to_pydatetime()
            close_t = cal.session_close(today.isoformat()).to_pydatetime()
            is_open_now = open_t <= now_utc <= close_t
            if is_open_now:
                closes_in = int((close_t - now_utc).total_seconds() / 60)
                note = f"Closes in {closes_in}m"
            else:
                if now_utc < open_t:
                    opens_in = int((open_t - now_utc).total_seconds() / 60)
                    note = f"Opens in {opens_in}m"
                else:
                    note = "Closed for today"
        except Exception:
            is_open_now = False
            note = "Trading day"
    else:
        # Check if it's a holiday
        try:
            holidays = cal.regular_holidays.holidays()
            today_ts = str(today)
            if today_ts in [str(h.date()) for h in holidays]:
                # Find the holiday name
                for h in cal.regular_holidays.holidays():
                    if h.date() == today:
                        note = "Holiday"
                        break
            else:
                note = "Weekend"
        except Exception:
            note = "Closed"

    return {
        "mic":        mic,
        "name":       label,
        "status":     "Open" if is_open_now else "Closed",
        "local_time": local_time,
        "tz":         tz_name,
        "note":       note,
    }


def get_upcoming_holidays(mic: str, days: int = 30) -> list:
    """Get upcoming holidays for a market within the next N days."""
    label, tz_name = MARKETS.get(mic, (mic, "UTC"))
    today = date.today()
    results = []
    try:
        cal = xcals.get_calendar(mic, start="2020-01-01", end="2030-12-31")
        for h in cal.regular_holidays.holidays():
            hd = h.date()
            if today <= hd <= date.fromordinal(today.toordinal() + days):
                results.append({"mic": mic, "name": label, "date": str(hd), "holiday": str(h)})
    except Exception as e:
        logger.warning(f"Holiday fetch failed for {mic}: {e}")
    return results


# ─── Message builder ──────────────────────────────────────────────────────────
def esc(text: str) -> str:
    """Escape special chars for Telegram MarkdownV2."""
    for ch in r"\_*[]()~`>#+-=|{}.!":
        text = text.replace(ch, f"\\{ch}")
    return text


def build_status_message(mics: list, title: str = "📊 Market Status") -> str:
    today = datetime.utcnow().strftime("%A, %d %B %Y")
    lines = [f"*{esc(title)}*", f"🗓 _{esc(today)} \\(UTC\\)_", ""]

    for mic in mics:
        s      = get_market_status(mic)
        emoji  = "🟢" if s["status"] == "Open" else "🔴"
        name   = s.get("name", mic)
        status = s["status"]
        note   = s.get("note", "")
        lt     = s.get("local_time", "")

        line = f"{emoji} *{esc(name)}* — {esc(status)}"
        if note:
            line += f"\n    💬 _{esc(note)}_"
        if lt:
            line += f"  🕐 _{esc(lt)} local_"
        lines.append(line)

    lines += ["", "📌 _Powered by exchange\\-calendars_"]
    return "\n".join(lines)


# ─── Gist persistence ─────────────────────────────────────────────────────────
_cache: dict | None = None


def _gist_headers() -> dict:
    return {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def load_data() -> dict:
    global _cache
    if _cache is not None:
        return _cache

    empty = {"subscribers": {}, "channels": list(CHANNEL_IDS)}

    if not GIST_ID or not GITHUB_TOKEN:
        logger.warning("GIST_ID or GITHUB_TOKEN not set — using in-memory storage only.")
        _cache = empty
        return _cache

    try:
        resp = httpx.get(
            f"https://api.github.com/gists/{GIST_ID}",
            headers=_gist_headers(), timeout=10,
        )
        resp.raise_for_status()
        content = resp.json()["files"][GIST_FILENAME]["content"]
        _cache  = json.loads(content)
        for ch in CHANNEL_IDS:
            if ch not in _cache.setdefault("channels", []):
                _cache["channels"].append(ch)
        logger.info("Data loaded from Gist.")
    except Exception as e:
        logger.error(f"Failed to load Gist: {e}. Using empty state.")
        _cache = empty

    return _cache


def save_data():
    global _cache
    if _cache is None or not GIST_ID or not GITHUB_TOKEN:
        return
    try:
        httpx.patch(
            f"https://api.github.com/gists/{GIST_ID}",
            headers=_gist_headers(),
            json={"files": {GIST_FILENAME: {"content": json.dumps(_cache, indent=2)}}},
            timeout=10,
        ).raise_for_status()
        logger.info("Data saved to Gist.")
    except Exception as e:
        logger.error(f"Failed to save Gist: {e}")


def get_subscribers() -> dict:
    return load_data().get("subscribers", {})

def get_channels() -> list:
    return load_data().get("channels", [])

def add_subscriber(chat_id: int, markets: list | None = None):
    data = load_data()
    data["subscribers"][str(chat_id)] = markets or DEFAULT_MICS
    save_data()

def remove_subscriber(chat_id: int):
    data = load_data()
    data["subscribers"].pop(str(chat_id), None)
    save_data()

def get_subscriber_markets(chat_id: int) -> list:
    return get_subscribers().get(str(chat_id), DEFAULT_MICS)

def set_subscriber_markets(chat_id: int, markets: list):
    data = load_data()
    data["subscribers"][str(chat_id)] = markets
    save_data()

def add_channel(channel_id: str):
    data = load_data()
    if channel_id not in data["channels"]:
        data["channels"].append(channel_id)
        save_data()

def remove_channel(channel_id: str):
    data = load_data()
    if channel_id in data["channels"]:
        data["channels"].remove(channel_id)
        save_data()


# ─── Commands ─────────────────────────────────────────────────────────────────
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 *Welcome to Trading Calendar Bot\\!*\n\n"
        "📋 *Commands:*\n"
        "/status — Check market status now\n"
        "/subscribe — Daily alerts at 07:00 UTC\n"
        "/unsubscribe — Stop daily alerts\n"
        "/markets — Choose exchanges to track\n"
        "/holidays — Upcoming holidays \\(next 30 days\\)\n\n"
        "📢 *Channel commands:*\n"
        "/addchannel @chan — Post alerts to a channel\n"
        "/removechannel @chan — Remove a channel\n"
        "/listchannels — Show all channels",
        parse_mode="MarkdownV2",
    )

async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await cmd_start(update, ctx)

async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    mics    = get_subscriber_markets(chat_id) or DEFAULT_MICS
    await update.message.reply_text("⏳ Checking market status…")
    try:
        msg = build_status_message(mics)
        await update.message.reply_text(msg, parse_mode="MarkdownV2")
    except Exception as e:
        logger.error(f"Status error: {e}")
        await update.message.reply_text("❌ Something went wrong\\. Try again later\\.", parse_mode="MarkdownV2")

async def cmd_subscribe(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    add_subscriber(update.effective_chat.id)
    await update.message.reply_text(
        "✅ *Subscribed\\!* Daily alert at *07:00 UTC*\\.\n\nUse /markets to pick exchanges\\.",
        parse_mode="MarkdownV2",
    )

async def cmd_unsubscribe(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if str(chat_id) in get_subscribers():
        remove_subscriber(chat_id)
        await update.message.reply_text("🔕 Unsubscribed\\.", parse_mode="MarkdownV2")
    else:
        await update.message.reply_text("You weren't subscribed\\. Use /subscribe\\.", parse_mode="MarkdownV2")

async def cmd_markets(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    current = get_subscriber_markets(chat_id)
    keyboard, row = [], []
    for mic, (label, _) in MARKETS.items():
        checked = "✅ " if mic in current else ""
        row.append(InlineKeyboardButton(f"{checked}{label}", callback_data=f"toggle:{mic}"))
        if len(row) == 2:
            keyboard.append(row); row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("💾 Save & Close", callback_data="markets:done")])
    await update.message.reply_text(
        "🌍 *Select markets to track:*\n_\\(tap to toggle, ✅ \\= active\\)_",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="MarkdownV2",
    )

async def callback_markets(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    await query.answer()
    chat_id = update.effective_chat.id
    data    = query.data

    if data == "markets:done":
        tracked = get_subscriber_markets(chat_id)
        names   = [MARKETS[m][0] for m in tracked if m in MARKETS]
        await query.edit_message_text(
            "✅ *Saved\\!* Tracking:\n" + "\n".join(f"  • {esc(n)}" for n in names),
            parse_mode="MarkdownV2",
        )
        return

    if data.startswith("toggle:"):
        mic     = data.split(":")[1]
        current = get_subscriber_markets(chat_id)
        if mic in current:
            current.remove(mic)
        else:
            current.append(mic)
        set_subscriber_markets(chat_id, current)

        keyboard, row = [], []
        for m, (label, _) in MARKETS.items():
            checked = "✅ " if m in current else ""
            row.append(InlineKeyboardButton(f"{checked}{label}", callback_data=f"toggle:{m}"))
            if len(row) == 2:
                keyboard.append(row); row = []
        if row:
            keyboard.append(row)
        keyboard.append([InlineKeyboardButton("💾 Save & Close", callback_data="markets:done")])
        await query.edit_message_reply_markup(InlineKeyboardMarkup(keyboard))

async def cmd_holidays(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    mics    = (get_subscriber_markets(chat_id) or DEFAULT_MICS)[:6]
    await update.message.reply_text("⏳ Fetching upcoming holidays…")

    all_holidays = []
    for mic in mics:
        all_holidays.extend(get_upcoming_holidays(mic, days=30))

    if not all_holidays:
        await update.message.reply_text("🎉 No holidays in the next 30 days for your markets\\!", parse_mode="MarkdownV2")
        return

    all_holidays.sort(key=lambda h: h["date"])
    lines = ["*🎌 Upcoming Holidays \\(next 30 days\\)*", ""]
    for h in all_holidays:
        lines.append(f"📅 *{esc(h['date'])}* — {esc(h['name'])}\n    {esc(h['holiday'])}")

    await update.message.reply_text("\n".join(lines), parse_mode="MarkdownV2")

async def cmd_addchannel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text(
            "Usage: `/addchannel @yourchannel`\n\n⚠️ Bot must be *Admin* first\\!",
            parse_mode="MarkdownV2",
        )
        return
    channel = ctx.args[0].strip()
    try:
        chat          = await ctx.bot.get_chat(channel)
        channel_id    = str(chat.id)
        channel_title = esc(chat.title or channel)
    except Exception:
        await update.message.reply_text(
            f"❌ Cannot access `{esc(channel)}`\\.\nMake sure bot is Admin of the channel\\.",
            parse_mode="MarkdownV2",
        )
        return
    add_channel(channel_id)
    await update.message.reply_text(
        f"✅ *{channel_title}* added\\! Daily alerts at *07:00 UTC*\\.",
        parse_mode="MarkdownV2",
    )

async def cmd_removechannel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text("Usage: `/removechannel @yourchannel`", parse_mode="MarkdownV2")
        return
    channel = ctx.args[0].strip()
    try:
        chat       = await ctx.bot.get_chat(channel)
        channel_id = str(chat.id)
        title      = esc(chat.title or channel)
    except Exception:
        channel_id = channel
        title      = esc(channel)
    remove_channel(channel_id)
    await update.message.reply_text(f"🗑 *{title}* removed\\.", parse_mode="MarkdownV2")

async def cmd_listchannels(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    channels = get_channels()
    if not channels:
        await update.message.reply_text("No channels yet\\. Use `/addchannel @yourchannel`\\.", parse_mode="MarkdownV2")
        return
    lines = ["*📢 Registered Channels:*", ""]
    for ch in channels:
        try:
            chat = await ctx.bot.get_chat(ch)
            lines.append(f"  • *{esc(chat.title)}* \\(`{esc(ch)}`\\)")
        except Exception:
            lines.append(f"  • `{esc(ch)}`")
    await update.message.reply_text("\n".join(lines), parse_mode="MarkdownV2")


# ─── Daily alert job ──────────────────────────────────────────────────────────
async def daily_alert_job(ctx: ContextTypes.DEFAULT_TYPE):
    subs     = get_subscribers()
    channels = get_channels()
    targets  = {cid: mics for cid, mics in subs.items()}
    for ch in channels:
        if ch not in targets:
            targets[ch] = DEFAULT_MICS

    if not targets:
        logger.info("Daily alert: no targets.")
        return

    logger.info(f"Daily alert: sending to {len(targets)} target(s).")
    for chat_id, mics in targets.items():
        try:
            msg = build_status_message(mics, title="🌅 Daily Market Alert")
            await ctx.bot.send_message(chat_id=chat_id, text=msg, parse_mode="MarkdownV2")
            logger.info(f"  ✓ Sent to {chat_id}")
        except Exception as e:
            logger.error(f"  ✗ Failed {chat_id}: {e}")


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    if not TELEGRAM_TOKEN:
        raise ValueError("❌ TELEGRAM_TOKEN is not set!")
    if not GIST_ID:
        logger.warning("⚠️  GIST_ID not set — data won't persist across restarts!")

    load_data()

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start",         cmd_start))
    app.add_handler(CommandHandler("help",          cmd_help))
    app.add_handler(CommandHandler("status",        cmd_status))
    app.add_handler(CommandHandler("subscribe",     cmd_subscribe))
    app.add_handler(CommandHandler("unsubscribe",   cmd_unsubscribe))
    app.add_handler(CommandHandler("markets",       cmd_markets))
    app.add_handler(CommandHandler("holidays",      cmd_holidays))
    app.add_handler(CommandHandler("addchannel",    cmd_addchannel))
    app.add_handler(CommandHandler("removechannel", cmd_removechannel))
    app.add_handler(CommandHandler("listchannels",  cmd_listchannels))
    app.add_handler(CallbackQueryHandler(callback_markets))

    app.job_queue.run_daily(
        daily_alert_job,
        time=dtime(hour=ALERT_HOUR, minute=ALERT_MINUTE),
        name="daily_market_alert",
    )

    logger.info("🤖 Bot is running.")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()

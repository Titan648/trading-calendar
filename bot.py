"""
Trading Calendar Telegram Bot
- Runs inside GitHub Actions (no server needed)
- Persists subscriber & channel data in a GitHub Gist
- Sends daily market status alerts to users AND Telegram channels
"""

import logging
import json
import os
import calendar as cal_module
from datetime import datetime, time
from pathlib import Path

import httpx
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ─── Config — all from GitHub Actions Secrets ─────────────────────────────────
TELEGRAM_TOKEN  = os.getenv("TELEGRAM_TOKEN", "")
GITHUB_TOKEN    = os.getenv("GITHUB_TOKEN", "")   # Actions provides this automatically
GIST_ID         = os.getenv("GIST_ID", "")        # You create one Gist, paste its ID here
CHANNEL_IDS_RAW = os.getenv("CHANNEL_IDS", "")
CHANNEL_IDS     = [c.strip() for c in CHANNEL_IDS_RAW.split(",") if c.strip()]

API_BASE = "https://api.apptasticsoftware.com/trading-calendar/v1"

GIST_FILENAME = "trading_bot_data.json"

DEFAULT_MARKETS = {
    "XNYS": "🇺🇸 NYSE",
    "XLON": "🏴󠁧󠁢󠁥󠁮󠁧󠁿 London",
    "XTKS": "🇯🇵 Tokyo",
    "XHKG": "🇭🇰 Hong Kong",
    "XPAR": "🇫🇷 Paris",
    "XFRA": "🇩🇪 Frankfurt",
    "XASX": "🇦🇺 Sydney",
    "XSHG": "🇨🇳 Shanghai",
}

ALERT_HOUR   = 7
ALERT_MINUTE = 0

# ─── Gist persistence ─────────────────────────────────────────────────────────
# Data shape stored in Gist:
# {
#   "subscribers": { "chat_id": ["MIC1", "MIC2", ...] },
#   "channels":    ["@chan1", "-100123456789"]
# }

_cache: dict | None = None  # in-memory cache so we don't hammer the API


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
            headers=_gist_headers(),
            timeout=10,
        )
        resp.raise_for_status()
        content = resp.json()["files"][GIST_FILENAME]["content"]
        _cache = json.loads(content)
        # Merge env-var channels into persisted list
        for ch in CHANNEL_IDS:
            if ch not in _cache.setdefault("channels", []):
                _cache["channels"].append(ch)
        logger.info("Data loaded from Gist.")
    except Exception as e:
        logger.error(f"Failed to load Gist data: {e}. Using empty state.")
        _cache = empty

    return _cache


def save_data():
    global _cache
    if _cache is None:
        return
    if not GIST_ID or not GITHUB_TOKEN:
        logger.warning("No GIST_ID/GITHUB_TOKEN — data won't be persisted.")
        return
    try:
        resp = httpx.patch(
            f"https://api.github.com/gists/{GIST_ID}",
            headers=_gist_headers(),
            json={"files": {GIST_FILENAME: {"content": json.dumps(_cache, indent=2)}}},
            timeout=10,
        )
        resp.raise_for_status()
        logger.info("Data saved to Gist.")
    except Exception as e:
        logger.error(f"Failed to save Gist data: {e}")


# ─── Subscriber helpers ───────────────────────────────────────────────────────
def get_subscribers() -> dict:
    return load_data().get("subscribers", {})


def get_channels() -> list:
    return load_data().get("channels", [])


def add_subscriber(chat_id: int, markets: list | None = None):
    data = load_data()
    data["subscribers"][str(chat_id)] = markets or list(DEFAULT_MARKETS.keys())
    save_data()


def remove_subscriber(chat_id: int):
    data = load_data()
    data["subscribers"].pop(str(chat_id), None)
    save_data()


def get_subscriber_markets(chat_id: int) -> list:
    return get_subscribers().get(str(chat_id), list(DEFAULT_MARKETS.keys()))


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


# ─── Trading Calendar API ─────────────────────────────────────────────────────
async def fetch_market_status(mics: list) -> list:
    url = f"{API_BASE}/markets/status?mic={','.join(mics)}"
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.json()


# ─── Message builder ──────────────────────────────────────────────────────────
def esc(text: str) -> str:
    """Escape special chars for Telegram MarkdownV2."""
    for ch in r"\_*[]()~`>#+-=|{}.!":
        text = text.replace(ch, f"\\{ch}")
    return text


def build_status_message(statuses: list, title: str = "📊 Market Status") -> str:
    today = datetime.utcnow().strftime("%A, %d %B %Y")
    lines = [f"*{esc(title)}*", f"🗓 _{esc(today)} \\(UTC\\)_", ""]

    for s in statuses:
        mic        = s.get("mic", "?")
        status     = s.get("status", "Unknown")
        exchange   = s.get("exchange", mic)
        holiday    = s.get("holiday_name")
        is_early   = s.get("is_early_close", False)
        local_time = s.get("local_time", "")

        emoji = {"Open": "🟢", "Closed": "🔴"}.get(status, "🟡")
        line  = f"{emoji} *{esc(exchange)}* \\({esc(mic)}\\) — {esc(status)}"

        if holiday:
            line += f"\n    🎌 _{esc(holiday)}_"
        elif is_early:
            line += "\n    ⏰ _Early close_"

        if local_time:
            try:
                lt  = datetime.fromisoformat(local_time)
                tz  = s.get("timezone_abbr", "")
                line += f"\n    🕐 Local: {esc(lt.strftime('%H:%M'))} {esc(tz)}"
            except Exception:
                pass

        lines.append(line)

    lines += ["", "📌 _Data via Trading Calendar API_"]
    return "\n".join(lines)


# ─── Commands ─────────────────────────────────────────────────────────────────
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 *Welcome to Trading Calendar Bot\\!*\n\n"
        "📋 *User commands:*\n"
        "/status — Check market status now\n"
        "/subscribe — Get daily alerts at 07:00 UTC\n"
        "/unsubscribe — Stop daily alerts\n"
        "/markets — Choose exchanges to track\n"
        "/holidays — Upcoming holidays this month\n\n"
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
    mics    = get_subscriber_markets(chat_id) or list(DEFAULT_MARKETS.keys())
    await update.message.reply_text("⏳ Fetching market status…")
    try:
        statuses = await fetch_market_status(mics)
        await update.message.reply_text(
            build_status_message(statuses),
            parse_mode="MarkdownV2",
            disable_web_page_preview=True,
        )
    except Exception as e:
        logger.error(f"Status error: {e}")
        await update.message.reply_text("❌ Failed to fetch data\\. Try again later\\.", parse_mode="MarkdownV2")


async def cmd_subscribe(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    add_subscriber(update.effective_chat.id)
    await update.message.reply_text(
        "✅ *Subscribed\\!* Daily alert at *07:00 UTC*\\.\n\n"
        "Use /markets to pick exchanges\\.\nUse /unsubscribe to stop\\.",
        parse_mode="MarkdownV2",
    )


async def cmd_unsubscribe(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if str(chat_id) in get_subscribers():
        remove_subscriber(chat_id)
        await update.message.reply_text("🔕 Unsubscribed from daily alerts\\.", parse_mode="MarkdownV2")
    else:
        await update.message.reply_text("You weren't subscribed\\. Use /subscribe\\.", parse_mode="MarkdownV2")


async def cmd_markets(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    current = get_subscriber_markets(chat_id)
    keyboard, row = [], []
    for mic, label in DEFAULT_MARKETS.items():
        checked = "✅ " if mic in current else ""
        row.append(InlineKeyboardButton(f"{checked}{label}", callback_data=f"toggle:{mic}"))
        if len(row) == 2:
            keyboard.append(row)
            row = []
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
        names   = [DEFAULT_MARKETS.get(m, m) for m in tracked]
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
        for m, label in DEFAULT_MARKETS.items():
            checked = "✅ " if m in current else ""
            row.append(InlineKeyboardButton(f"{checked}{label}", callback_data=f"toggle:{m}"))
            if len(row) == 2:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)
        keyboard.append([InlineKeyboardButton("💾 Save & Close", callback_data="markets:done")])
        await query.edit_message_reply_markup(InlineKeyboardMarkup(keyboard))


async def cmd_holidays(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    mics    = (get_subscriber_markets(chat_id) or list(DEFAULT_MARKETS.keys()))[:5]
    now     = datetime.utcnow()
    start   = now.strftime("%Y-%m-%d")
    last_d  = cal_module.monthrange(now.year, now.month)[1]
    end     = now.replace(day=last_d).strftime("%Y-%m-%d")

    await update.message.reply_text("⏳ Fetching upcoming holidays…")

    all_holidays = []
    async with httpx.AsyncClient(timeout=15) as client:
        for mic in mics:
            try:
                resp = await client.get(f"{API_BASE}/markets/holidays?mic={mic}&start={start}&end={end}")
                if resp.status_code == 200:
                    all_holidays.extend(resp.json())
            except Exception as e:
                logger.warning(f"Holidays fetch failed {mic}: {e}")

    if not all_holidays:
        await update.message.reply_text("🎉 No holidays found for your markets this month\\!", parse_mode="MarkdownV2")
        return

    all_holidays.sort(key=lambda h: h.get("date", ""))
    lines = [f"*🎌 Upcoming Holidays \\({esc(start)} → {esc(end)}\\)*", ""]
    for h in all_holidays:
        date     = esc(h.get("date", "?"))
        name     = esc(h.get("holiday_name", "Holiday"))
        exchange = esc(h.get("exchange", h.get("mic", "?")))
        flag     = h.get("flag", "🏳️")
        tag      = "⏰ Early close" if h.get("is_early_close") else "🚫 Closed"
        lines.append(f"📅 *{date}* — {flag} {exchange}\n    {name} \\({esc(tag)}\\)")

    await update.message.reply_text("\n".join(lines), parse_mode="MarkdownV2")


# ─── Channel commands ─────────────────────────────────────────────────────────
async def cmd_addchannel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text(
            "Usage: `/addchannel @yourchannel`\n\n"
            "⚠️ Bot must be *Admin* of that channel first\\!",
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
            f"❌ Cannot access `{esc(channel)}`\\.\n\n"
            "Check:\n1️⃣ Bot is in the channel\n2️⃣ Bot is *Admin*\n3️⃣ Username is correct",
            parse_mode="MarkdownV2",
        )
        return

    add_channel(channel_id)
    await update.message.reply_text(
        f"✅ *{channel_title}* added\\! Daily alerts will post there at *07:00 UTC*\\.",
        parse_mode="MarkdownV2",
    )


async def cmd_removechannel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text("Usage: `/removechannel @yourchannel`", parse_mode="MarkdownV2")
        return
    channel = ctx.args[0].strip()
    try:
        chat          = await ctx.bot.get_chat(channel)
        channel_id    = str(chat.id)
        channel_title = esc(chat.title or channel)
    except Exception:
        channel_id    = channel
        channel_title = esc(channel)

    remove_channel(channel_id)
    await update.message.reply_text(f"🗑 *{channel_title}* removed\\.", parse_mode="MarkdownV2")


async def cmd_listchannels(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    channels = get_channels()
    if not channels:
        await update.message.reply_text(
            "No channels yet\\. Use `/addchannel @yourchannel`\\.", parse_mode="MarkdownV2"
        )
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

    targets = {}
    for chat_id, mics in subs.items():
        targets[chat_id] = mics
    for ch in channels:
        if ch not in targets:
            targets[ch] = list(DEFAULT_MARKETS.keys())

    if not targets:
        logger.info("Daily alert: no targets.")
        return

    logger.info(f"Daily alert: sending to {len(targets)} target(s).")
    for chat_id, mics in targets.items():
        try:
            statuses = await fetch_market_status(mics)
            msg      = build_status_message(statuses, title="🌅 Daily Market Alert")
            await ctx.bot.send_message(
                chat_id=chat_id,
                text=msg,
                parse_mode="MarkdownV2",
                disable_web_page_preview=True,
            )
            logger.info(f"  ✓ Sent to {chat_id}")
        except Exception as e:
            logger.error(f"  ✗ Failed {chat_id}: {e}")


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    if not TELEGRAM_TOKEN:
        raise ValueError("❌ TELEGRAM_TOKEN is not set!")
    if not GIST_ID:
        logger.warning("⚠️  GIST_ID not set — data won't persist across bot restarts!")
    if not GITHUB_TOKEN:
        logger.warning("⚠️  GITHUB_TOKEN not set — data won't persist across bot restarts!")

    # Pre-load data at startup
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
        time=time(hour=ALERT_HOUR, minute=ALERT_MINUTE),
        name="daily_market_alert",
    )

    logger.info("🤖 Bot is running.")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()

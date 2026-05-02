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
import calendar as cal_module
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

# !! Must be set before importing exchange_calendars !!
os.environ["EXCHANGE_CALENDARS_CACHE_DIR"] = tempfile.mkdtemp()

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
FINNHUB_TOKEN   = os.getenv("FINNHUB_TOKEN", "")
CHANNEL_IDS_RAW = os.getenv("CHANNEL_IDS", "")
CHANNEL_IDS     = [c.strip() for c in CHANNEL_IDS_RAW.split(",") if c.strip()]

GIST_FILENAME = "trading_bot_data.json"

# Alert times (UTC)
MORNING_HOUR   = 7
MORNING_MINUTE = 0
EVENING_HOUR   = 17
EVENING_MINUTE = 0

# ─── Supported markets ────────────────────────────────────────────────────────
MARKETS = {
    "XNYS": ("🇺🇸 NYSE",       "America/New_York",  "NYSE"),
    "XNAS": ("🇺🇸 NASDAQ",     "America/New_York",  "XNAS"),
    "XLON": ("🏴󠁧󠁢󠁥󠁮󠁧󠁿 London",   "Europe/London",     "XLON"),
    "XTKS": ("🇯🇵 Tokyo",      "Asia/Tokyo",        "XTKS"),
    "XHKG": ("🇭🇰 Hong Kong",  "Asia/Hong_Kong",    "XHKG"),
    "XPAR": ("🇫🇷 Paris",      "Europe/Paris",      "XPAR"),
    "XFRA": ("🇩🇪 Frankfurt",  "Europe/Berlin",     "XFRA"),
    "XASX": ("🇦🇺 Sydney",     "Australia/Sydney",  "XASX"),
    "XSHG": ("🇨🇳 Shanghai",   "Asia/Shanghai",     "XSHG"),
    "XBOM": ("🇮🇳 Mumbai",     "Asia/Kolkata",      "XBOM"),
    "XKRX": ("🇰🇷 Seoul",      "Asia/Seoul",        "XKRX"),
    "XTSE": ("🇨🇦 Toronto",    "America/Toronto",   "XTSE"),
}

DEFAULT_MICS = ["XNYS", "XLON", "XTKS", "XHKG", "XPAR", "XFRA", "XASX", "XSHG"]

# High-impact economic event keywords
HIGH_IMPACT_KEYWORDS = [
    "fomc", "federal reserve", "fed rate", "interest rate", "rate decision",
    "nonfarm", "nfp", "cpi", "inflation", "gdp", "unemployment",
    "ecb", "boe", "boj", "pboc", "rba",
    "cease", "war", "sanction", "crisis", "default", "recession",
    "iran", "israel", "russia", "ukraine", "taiwan", "china",
    "opec", "oil", "nuclear", "invasion", "conflict",
]


# ─── Escape helper ────────────────────────────────────────────────────────────
def esc(text: str) -> str:
    for ch in r"\_*[]()~`>#+-=|{}.!":
        text = text.replace(ch, f"\\{ch}")
    return text


# ─── Market status logic ──────────────────────────────────────────────────────
def get_market_status(mic: str) -> dict:
    label, tz_name, cal_code = MARKETS.get(mic, (mic, "UTC", mic))
    now_utc    = datetime.now(ZoneInfo("UTC"))
    today      = now_utc.astimezone(ZoneInfo(tz_name)).date()
    local_time = now_utc.astimezone(ZoneInfo(tz_name)).strftime("%H:%M")

    try:
        end_date = "2025-12-31" if cal_code == "XSHG" else "2027-12-31"
        cal = xcals.get_calendar(cal_code, start="2025-01-01", end=end_date)
    except Exception as e:
        logger.warning(f"Calendar not found for {mic} ({cal_code}): {e}")
        return {"mic": mic, "name": label, "status": "Unknown", "note": "Unavailable", "local_time": local_time}

    try:
        is_session = cal.is_session(today.isoformat())
    except Exception:
        is_session = False

    is_open_now = False
    note        = ""

    if is_session:
        try:
            open_t  = cal.session_open(today.isoformat()).to_pydatetime()
            close_t = cal.session_close(today.isoformat()).to_pydatetime()
            is_open_now = open_t <= now_utc <= close_t
            if is_open_now:
                closes_in = int((close_t - now_utc).total_seconds() / 60)
                note = f"Closes in {closes_in}m"
            elif now_utc < open_t:
                opens_in = int((open_t - now_utc).total_seconds() / 60)
                note = f"Opens in {opens_in}m"
            else:
                note = "Closed for today"
        except Exception:
            note = "Trading day"
    else:
        try:
            holidays = [str(h.date()) for h in cal.regular_holidays.holidays()]
            note = "Holiday" if today.isoformat() in holidays else "Weekend"
        except Exception:
            note = "Closed"

    return {
        "mic":        mic,
        "name":       label,
        "status":     "Open" if is_open_now else "Closed",
        "local_time": local_time,
        "note":       note,
    }


def get_upcoming_holidays(mic: str, days: int = 30) -> list:
    label, tz_name, cal_code = MARKETS.get(mic, (mic, "UTC", mic))
    today   = date.today()
    end_day = today + timedelta(days=days)
    results = []
    try:
        end_date = "2025-12-31" if cal_code == "XSHG" else "2027-12-31"
        cal = xcals.get_calendar(cal_code, start="2025-01-01", end=end_date)
        for h in cal.regular_holidays.holidays():
            hd = h.date()
            if today <= hd <= end_day:
                results.append({"mic": mic, "name": label, "date": str(hd), "holiday": str(h)})
    except Exception as e:
        logger.warning(f"Holiday fetch failed for {mic}: {e}")
    return results


# ─── Finnhub API ──────────────────────────────────────────────────────────────
async def fetch_economic_events(days_ahead: int = 7) -> list:
    """Fetch upcoming HIGH-impact economic events from Finnhub."""
    if not FINNHUB_TOKEN:
        return []
    today    = date.today()
    end_date = today + timedelta(days=days_ahead)
    url = (
        f"https://finnhub.io/api/v1/calendar/economic"
        f"?from={today}&to={end_date}&token={FINNHUB_TOKEN}"
    )
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            events = resp.json().get("economicCalendar", [])

        # Filter high-impact only
        high = []
        for e in events:
            impact = str(e.get("impact", "")).lower()
            name   = str(e.get("event", "")).lower()
            if impact in ("high", "3") or any(kw in name for kw in HIGH_IMPACT_KEYWORDS):
                high.append(e)
        return sorted(high, key=lambda x: x.get("time", ""))
    except Exception as ex:
        logger.error(f"Finnhub economic events error: {ex}")
        return []


async def fetch_market_news(category: str = "general", count: int = 8) -> list:
    """Fetch latest market & geopolitical news from Finnhub."""
    if not FINNHUB_TOKEN:
        return []
    url = f"https://finnhub.io/api/v1/news?category={category}&token={FINNHUB_TOKEN}"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            news = resp.json()

        # Filter for high-impact / geopolitical relevance
        filtered = []
        for item in news:
            headline = str(item.get("headline", "")).lower()
            summary  = str(item.get("summary",  "")).lower()
            combined = headline + " " + summary
            if any(kw in combined for kw in HIGH_IMPACT_KEYWORDS):
                filtered.append(item)

        # Return latest N, fallback to all news if no keyword match
        result = filtered[:count] if filtered else news[:count]
        return result
    except Exception as ex:
        logger.error(f"Finnhub news error: {ex}")
        return []


# ─── Message builders ─────────────────────────────────────────────────────────
def build_status_message(mics: list, title: str = "📊 Market Status") -> str:
    today = datetime.now(ZoneInfo("UTC")).strftime("%A, %d %B %Y")
    lines = [f"*{esc(title)}*", f"🗓 _{esc(today)} \\(UTC\\)_", ""]

    for mic in mics:
        s      = get_market_status(mic)
        emoji  = "🟢" if s["status"] == "Open" else ("🔴" if s["status"] == "Closed" else "⚪")
        name   = s.get("name", mic)
        status = s["status"]
        note   = s.get("note", "")
        lt     = s.get("local_time", "")

        line = f"{emoji} *{esc(name)}* — {esc(status)}"
        extra = []
        if note:
            extra.append(f"💬 _{esc(note)}_")
        if lt:
            extra.append(f"🕐 _{esc(lt)}_")
        if extra:
            line += "\n    " + "  ".join(extra)
        lines.append(line)

    lines += ["", "📌 _Powered by exchange\\-calendars_"]
    return "\n".join(lines)


def build_events_message(events: list, title: str = "📅 Upcoming High-Impact Events") -> str:
    if not events:
        return f"*{esc(title)}*\n\n_No high\\-impact events found for the next 7 days\\._"

    lines = [f"*{esc(title)}*", ""]
    for e in events[:10]:
        name     = esc(e.get("event",    "Unknown event"))
        country  = esc(e.get("country",  ""))
        time_str = e.get("time", "")
        actual   = e.get("actual",   "")
        forecast = e.get("estimate", "")
        prev     = e.get("prev",     "")

        # Format date/time
        try:
            dt      = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
            display = dt.strftime("%a %d %b, %H:%M UTC")
        except Exception:
            display = time_str[:16] if time_str else "TBD"

        line = f"🔴 *{name}*"
        if country:
            line += f" \\({country}\\)"
        line += f"\n    📆 _{esc(display)}_"
        if forecast:
            line += f"  •  Forecast: `{esc(str(forecast))}`"
        if prev:
            line += f"  •  Prev: `{esc(str(prev))}`"
        if actual:
            line += f"  •  Actual: `{esc(str(actual))}`"
        lines.append(line)

    lines += ["", "📌 _Source: Finnhub Economic Calendar_"]
    return "\n".join(lines)


def build_news_message(news: list, title: str = "📰 Market & Geopolitical News") -> str:
    if not news:
        return f"*{esc(title)}*\n\n_No major news found at this time\\._"

    lines = [f"*{esc(title)}*", ""]
    for item in news[:8]:
        headline = esc(item.get("headline", "No title")[:120])
        source   = esc(item.get("source",   "Unknown"))
        url      = item.get("url", "")
        ts       = item.get("datetime", 0)

        try:
            dt      = datetime.fromtimestamp(ts, tz=ZoneInfo("UTC"))
            display = dt.strftime("%H:%M UTC")
        except Exception:
            display = ""

        line = f"📌 *{headline}*"
        if display:
            line += f"\n    🕐 _{esc(display)}_ — _{source}_"
        else:
            line += f"\n    _{source}_"
        if url:
            line += f"\n    [Read more]({url})"
        lines.append(line)

    lines += ["", "📌 _Source: Finnhub News_"]
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
        logger.warning("GIST_ID or GITHUB_TOKEN not set — using in-memory storage.")
        _cache = empty
        return _cache
    try:
        resp = httpx.get(f"https://api.github.com/gists/{GIST_ID}", headers=_gist_headers(), timeout=10)
        resp.raise_for_status()
        _cache = json.loads(resp.json()["files"][GIST_FILENAME]["content"])
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
    if not _cache or not GIST_ID or not GITHUB_TOKEN:
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
        "📋 *Market commands:*\n"
        "/status — Live market open\\/closed status\n"
        "/subscribe — Daily morning \\+ evening alerts\n"
        "/unsubscribe — Stop alerts\n"
        "/markets — Choose exchanges to track\n"
        "/holidays — Upcoming market holidays\n\n"
        "📅 *News commands:*\n"
        "/events — High\\-impact economic events \\(FOMC, CPI…\\)\n"
        "/news — Market \\& geopolitical news\n\n"
        "📢 *Channel commands:*\n"
        "/addchannel @chan — Post alerts to a channel\n"
        "/removechannel @chan — Remove a channel\n"
        "/listchannels — Show registered channels",
        parse_mode="MarkdownV2",
    )

async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await cmd_start(update, ctx)

async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    mics    = get_subscriber_markets(chat_id) or DEFAULT_MICS
    await update.message.reply_text("⏳ Checking market status…")
    try:
        await update.message.reply_text(build_status_message(mics), parse_mode="MarkdownV2")
    except Exception as e:
        logger.error(f"Status error: {e}")
        await update.message.reply_text("❌ Something went wrong\\. Try again later\\.", parse_mode="MarkdownV2")

async def cmd_events(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not FINNHUB_TOKEN:
        await update.message.reply_text(
            "⚠️ *Finnhub API key not set\\!*\n\n"
            "Add `FINNHUB_TOKEN` to your GitHub Actions secrets\\.\n"
            "Get a free key at: https://finnhub\\.io",
            parse_mode="MarkdownV2",
        )
        return
    await update.message.reply_text("⏳ Fetching high\\-impact events…", parse_mode="MarkdownV2")
    events = await fetch_economic_events(days_ahead=7)
    await update.message.reply_text(
        build_events_message(events),
        parse_mode="MarkdownV2",
        disable_web_page_preview=True,
    )

async def cmd_news(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not FINNHUB_TOKEN:
        await update.message.reply_text(
            "⚠️ *Finnhub API key not set\\!*\n\n"
            "Add `FINNHUB_TOKEN` to your GitHub Actions secrets\\.\n"
            "Get a free key at: https://finnhub\\.io",
            parse_mode="MarkdownV2",
        )
        return
    await update.message.reply_text("⏳ Fetching latest news…", parse_mode="MarkdownV2")
    news = await fetch_market_news()
    await update.message.reply_text(
        build_news_message(news),
        parse_mode="MarkdownV2",
        disable_web_page_preview=True,
    )

async def cmd_subscribe(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    add_subscriber(update.effective_chat.id)
    await update.message.reply_text(
        "✅ *Subscribed\\!*\n\n"
        "You'll receive:\n"
        "🌅 *Morning digest* at 07:00 UTC — market status \\+ events \\+ news\n"
        "🌆 *Evening digest* at 17:00 UTC — closing recap \\+ news\n\n"
        "Use /markets to pick exchanges\\.",
        parse_mode="MarkdownV2",
    )

async def cmd_unsubscribe(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if str(chat_id) in get_subscribers():
        remove_subscriber(chat_id)
        await update.message.reply_text("🔕 Unsubscribed from all alerts\\.", parse_mode="MarkdownV2")
    else:
        await update.message.reply_text("You weren't subscribed\\. Use /subscribe\\.", parse_mode="MarkdownV2")

async def cmd_markets(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    current = get_subscriber_markets(chat_id)
    keyboard, row = [], []
    for mic, (label, _, __) in MARKETS.items():
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
        for m, (label, _, __) in MARKETS.items():
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
        await update.message.reply_text("🎉 No holidays in the next 30 days\\!", parse_mode="MarkdownV2")
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
            f"❌ Cannot access `{esc(channel)}`\\.\nMake sure bot is Admin\\.",
            parse_mode="MarkdownV2",
        )
        return
    add_channel(channel_id)
    await update.message.reply_text(
        f"✅ *{channel_title}* added\\! Alerts will be posted at 07:00 and 17:00 UTC\\.",
        parse_mode="MarkdownV2",
    )

async def cmd_removechannel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text("Usage: `/removechannel @yourchannel`", parse_mode="MarkdownV2")
        return
    channel = ctx.args[0].strip()
    try:
        chat = await ctx.bot.get_chat(channel)
        channel_id = str(chat.id)
        title = esc(chat.title or channel)
    except Exception:
        channel_id = channel
        title = esc(channel)
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


# ─── Digest jobs ──────────────────────────────────────────────────────────────
async def send_digest(ctx, is_morning: bool):
    subs     = get_subscribers()
    channels = get_channels()
    targets  = {cid: mics for cid, mics in subs.items()}
    for ch in channels:
        if ch not in targets:
            targets[ch] = DEFAULT_MICS

    if not targets:
        return

    label = "🌅 Morning" if is_morning else "🌆 Evening"
    logger.info(f"{label} digest: sending to {len(targets)} target(s).")

    # Fetch news & events once for everyone
    events = await fetch_economic_events(days_ahead=7) if FINNHUB_TOKEN else []
    news   = await fetch_market_news() if FINNHUB_TOKEN else []

    for chat_id, mics in targets.items():
        try:
            # 1. Market status
            market_msg = build_status_message(
                mics,
                title=f"{'🌅 Morning' if is_morning else '🌆 Evening'} Market Digest"
            )
            await ctx.bot.send_message(chat_id=chat_id, text=market_msg, parse_mode="MarkdownV2")

            # 2. Economic events (morning only — forward-looking)
            if is_morning and events:
                await ctx.bot.send_message(
                    chat_id=chat_id,
                    text=build_events_message(events, "📅 High\\-Impact Events This Week"),
                    parse_mode="MarkdownV2",
                    disable_web_page_preview=True,
                )

            # 3. News (both morning and evening)
            if news:
                await ctx.bot.send_message(
                    chat_id=chat_id,
                    text=build_news_message(
                        news,
                        "📰 Morning Market News" if is_morning else "📰 Evening Market News"
                    ),
                    parse_mode="MarkdownV2",
                    disable_web_page_preview=True,
                )

            logger.info(f"  ✓ Sent to {chat_id}")
        except Exception as e:
            logger.error(f"  ✗ Failed {chat_id}: {e}")


async def morning_digest_job(ctx: ContextTypes.DEFAULT_TYPE):
    await send_digest(ctx, is_morning=True)

async def evening_digest_job(ctx: ContextTypes.DEFAULT_TYPE):
    await send_digest(ctx, is_morning=False)


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    if not TELEGRAM_TOKEN:
        raise ValueError("❌ TELEGRAM_TOKEN is not set!")
    if not FINNHUB_TOKEN:
        logger.warning("⚠️  FINNHUB_TOKEN not set — news/events commands will be disabled.")
    if not GIST_ID:
        logger.warning("⚠️  GIST_ID not set — data won't persist across restarts!")

    load_data()

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start",         cmd_start))
    app.add_handler(CommandHandler("help",          cmd_help))
    app.add_handler(CommandHandler("status",        cmd_status))
    app.add_handler(CommandHandler("events",        cmd_events))
    app.add_handler(CommandHandler("news",          cmd_news))
    app.add_handler(CommandHandler("subscribe",     cmd_subscribe))
    app.add_handler(CommandHandler("unsubscribe",   cmd_unsubscribe))
    app.add_handler(CommandHandler("markets",       cmd_markets))
    app.add_handler(CommandHandler("holidays",      cmd_holidays))
    app.add_handler(CommandHandler("addchannel",    cmd_addchannel))
    app.add_handler(CommandHandler("removechannel", cmd_removechannel))
    app.add_handler(CommandHandler("listchannels",  cmd_listchannels))
    app.add_handler(CallbackQueryHandler(callback_markets))

    # Morning digest: 07:00 UTC
    app.job_queue.run_daily(
        morning_digest_job,
        time=dtime(hour=MORNING_HOUR, minute=MORNING_MINUTE),
        name="morning_digest",
    )

    # Evening digest: 17:00 UTC
    app.job_queue.run_daily(
        evening_digest_job,
        time=dtime(hour=EVENING_HOUR, minute=EVENING_MINUTE),
        name="evening_digest",
    )

    logger.info("🤖 Bot is running.")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()

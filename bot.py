"""
Trading Calendar Telegram Bot
- Marché en temps réel (exchange_calendars)
- Événements économiques + actualités (Finnhub)
- Digest matin 07:00 UTC + soir 17:00 UTC
- Persistance GitHub Gist
"""

import logging
import json
import os
import tempfile
from collections import defaultdict
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

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

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN  = os.getenv("TELEGRAM_TOKEN", "")
GITHUB_TOKEN    = os.getenv("GITHUB_TOKEN", "")
GIST_ID         = os.getenv("GIST_ID", "")
FINNHUB_TOKEN   = os.getenv("FINNHUB_TOKEN", "")
CHANNEL_IDS_RAW = os.getenv("CHANNEL_IDS", "")
CHANNEL_IDS     = [c.strip() for c in CHANNEL_IDS_RAW.split(",") if c.strip()]

GIST_FILENAME = "trading_bot_data.json"
MORNING_HOUR, MORNING_MINUTE = 7, 0
EVENING_HOUR, EVENING_MINUTE = 17, 0

MARKETS = {
    "XNYS": ("🇺🇸 NYSE",       "America/New_York",  "NYSE"),
    "XNAS": ("🇺🇸 NASDAQ",     "America/New_York",  "XNAS"),
    "XLON": ("🏴󠁧󠁢󠁥󠁮󠁧󠁿 Londres",   "Europe/London",     "XLON"),
    "XTKS": ("🇯🇵 Tokyo",      "Asia/Tokyo",        "XTKS"),
    "XHKG": ("🇭🇰 Hong Kong",  "Asia/Hong_Kong",    "XHKG"),
    "XPAR": ("🇫🇷 Paris",      "Europe/Paris",      "XPAR"),
    "XFRA": ("🇩🇪 Francfort",  "Europe/Berlin",     "XFRA"),
    "XASX": ("🇦🇺 Sydney",     "Australia/Sydney",  "XASX"),
    "XSHG": ("🇨🇳 Shanghai",   "Asia/Shanghai",     "XSHG"),
    "XBOM": ("🇮🇳 Mumbai",     "Asia/Kolkata",      "XBOM"),
    "XKRX": ("🇰🇷 Séoul",      "Asia/Seoul",        "XKRX"),
    "XTSE": ("🇨🇦 Toronto",    "America/Toronto",   "XTSE"),
}

DEFAULT_MICS = ["XNYS", "XLON", "XTKS", "XHKG", "XPAR", "XFRA", "XASX", "XSHG"]

COUNTRY_FLAGS = {
    "US": "🇺🇸", "GB": "🏴󠁧󠁢󠁥󠁮󠁧󠁿", "EU": "🇪🇺", "DE": "🇩🇪", "FR": "🇫🇷",
    "JP": "🇯🇵", "CN": "🇨🇳", "AU": "🇦🇺", "CA": "🇨🇦", "CH": "🇨🇭",
    "NZ": "🇳🇿", "IN": "🇮🇳", "KR": "🇰🇷", "HK": "🇭🇰", "SG": "🇸🇬",
    "TR": "🇹🇷", "ZA": "🇿🇦", "MX": "🇲🇽", "BR": "🇧🇷", "IT": "🇮🇹",
    "ES": "🇪🇸", "ID": "🇮🇩", "VN": "🇻🇳", "KZ": "🇰🇿",
}

MAJOR_COUNTRIES = {"US", "EU", "GB", "JP", "CN", "DE", "FR", "CA", "AU", "CH", "NZ"}

HIGH_IMPACT_KEYWORDS = [
    "fomc", "federal reserve", "fed rate", "interest rate", "rate decision",
    "nonfarm", "nfp", "cpi", "inflation", "gdp", "unemployment",
    "ecb", "boe", "boj", "pboc", "rba",
    "cease", "war", "sanction", "crisis", "default", "recession",
    "iran", "israel", "russia", "ukraine", "taiwan",
    "opec", "oil", "nuclear", "invasion", "conflict",
]


# ─── Escape ───────────────────────────────────────────────────────────────────
def esc(text: str) -> str:
    for ch in r"\_*[]()~`>#+-=|{}.!":
        text = text.replace(ch, f"\\{ch}")
    return text


# ─── Market status ────────────────────────────────────────────────────────────
def get_market_status(mic: str) -> dict:
    label, tz_name, cal_code = MARKETS.get(mic, (mic, "UTC", mic))
    now_utc    = datetime.now(ZoneInfo("UTC"))
    today      = now_utc.astimezone(ZoneInfo(tz_name)).date()
    local_time = now_utc.astimezone(ZoneInfo(tz_name)).strftime("%H:%M")

    try:
        end_date = "2025-12-31" if cal_code == "XSHG" else "2027-12-31"
        cal = xcals.get_calendar(cal_code, start="2025-01-01", end=end_date)
    except Exception as e:
        logger.warning(f"Calendrier introuvable {mic}: {e}")
        return {"mic": mic, "name": label, "status": "Inconnu", "note": "Indisponible", "local_time": local_time}

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
                note = f"Ferme dans {closes_in}m"
            elif now_utc < open_t:
                opens_in = int((open_t - now_utc).total_seconds() / 60)
                note = f"Ouvre dans {opens_in}m"
            else:
                note = "Fermé pour aujourd'hui"
        except Exception:
            note = "Jour de trading"
    else:
        try:
            holidays = [str(h.date()) for h in cal.regular_holidays.holidays()]
            note = "Jour férié" if today.isoformat() in holidays else "Week-end"
        except Exception:
            note = "Fermé"

    return {
        "mic":        mic,
        "name":       label,
        "status":     "Ouvert" if is_open_now else "Fermé",
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
                # h.name contains the holiday name (e.g. "Early May Bank Holiday")
                holiday_name = h.name if hasattr(h, "name") and h.name else str(hd)
                results.append({
                    "mic":          mic,
                    "name":         label,
                    "date":         str(hd),
                    "holiday_name": holiday_name,
                })
    except Exception as e:
        logger.warning(f"Jours fériés introuvables {mic}: {e}")
    return results


# ─── Finnhub ──────────────────────────────────────────────────────────────────
async def fetch_economic_events(days_ahead: int = 7) -> list:
    if not FINNHUB_TOKEN:
        return []
    today    = date.today()
    end_date = today + timedelta(days=days_ahead)
    url = f"https://finnhub.io/api/v1/calendar/economic?from={today}&to={end_date}&token={FINNHUB_TOKEN}"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            events = resp.json().get("economicCalendar", [])
        # Keep only major countries
        return sorted(
            [e for e in events if e.get("country", "").upper() in MAJOR_COUNTRIES],
            key=lambda x: x.get("time", "")
        )
    except Exception as ex:
        logger.error(f"Finnhub events error: {ex}")
        return []


async def fetch_market_news(count: int = 10) -> list:
    if not FINNHUB_TOKEN:
        return []
    url = f"https://finnhub.io/api/v1/news?category=general&token={FINNHUB_TOKEN}"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            news = resp.json()
        filtered = [
            item for item in news
            if any(kw in (item.get("headline","") + item.get("summary","")).lower()
                   for kw in HIGH_IMPACT_KEYWORDS)
        ]
        return (filtered or news)[:count]
    except Exception as ex:
        logger.error(f"Finnhub news error: {ex}")
        return []


# ─── Message builders ─────────────────────────────────────────────────────────
def build_status_message(mics: list, title: str = "📊 Marchés") -> str:
    today = datetime.now(ZoneInfo("UTC")).strftime("%A %d %B %Y")
    lines = [f"*{esc(title)}*", f"🗓 _{esc(today)} \\(UTC\\)_", ""]

    for mic in mics:
        s      = get_market_status(mic)
        emoji  = "🟢" if s["status"] == "Ouvert" else ("🔴" if s["status"] == "Fermé" else "⚪")
        line   = f"{emoji} *{esc(s['name'])}*"
        detail = []
        if s.get("note"):
            detail.append(f"_{esc(s['note'])}_")
        if s.get("local_time"):
            detail.append(f"🕐 `{esc(s['local_time'])}`")
        if detail:
            line += "  —  " + "  ".join(detail)
        lines.append(line)

    lines += ["", "📌 _exchange\\-calendars_"]
    return "\n".join(lines)


def build_events_message(events: list, title: str = "📅 Événements de la semaine") -> str:
    if not events:
        return f"*{esc(title)}*\n\n_Aucun événement majeur cette semaine\\._"

    # Group by date
    by_date = defaultdict(list)
    for e in events:
        time_str = e.get("time", "")
        try:
            dt      = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
            day_key = dt.strftime("%A %d %b")
            e["_hour"] = dt.strftime("%H:%M")
        except Exception:
            day_key    = "À venir"
            e["_hour"] = "—"
        by_date[day_key].append(e)

    lines = [f"*{esc(title)}*", ""]
    shown = 0

    for day, day_events in by_date.items():
        if shown >= 15:
            break
        lines.append(f"┌ 📆 *{esc(day)}*")
        for e in day_events[:5]:
            flag     = COUNTRY_FLAGS.get(e.get("country","").upper(), "🌐")
            name     = esc(e.get("event", "?")[:50])
            hour     = esc(e.get("_hour", "—"))
            forecast = e.get("estimate", "")
            prev     = e.get("prev", "")
            actual   = e.get("actual", "")

            line = f"│  {flag} `{hour}`  {name}"
            nums = []
            if actual:
                nums.append(f"Réel: *{esc(str(actual))}*")
            elif forecast:
                nums.append(f"Prévu: `{esc(str(forecast))}`")
            if prev:
                nums.append(f"Préc: `{esc(str(prev))}`")
            if nums:
                line += f"\n│       " + "  ·  ".join(nums)
            lines.append(line)
            shown += 1

        lines.append("└─────────────────")
        lines.append("")

    lines.append("📌 _Source: Finnhub_")
    return "\n".join(lines)


def build_news_message(news: list, title: str = "📰 Actualités marchés") -> str:
    if not news:
        return f"*{esc(title)}*\n\n_Aucune actualité majeure pour le moment\\._"

    lines = [f"*{esc(title)}*", ""]
    for i, item in enumerate(news[:6], 1):
        headline = item.get("headline", "Sans titre")[:100]
        source   = item.get("source", "")
        url      = item.get("url", "")
        ts       = item.get("datetime", 0)

        try:
            dt      = datetime.fromtimestamp(ts, tz=ZoneInfo("UTC"))
            display = dt.strftime("%H:%M UTC")
        except Exception:
            display = ""

        line = f"*{i}\\.* {esc(headline)}"
        meta = []
        if display:
            meta.append(f"🕐 {esc(display)}")
        if source:
            meta.append(f"_{esc(source)}_")
        if meta:
            line += "\n    " + "  ·  ".join(meta)
        if url:
            line += f"\n    [🔗 Lire]({url})"
        lines.append(line)
        lines.append("")

    lines.append("📌 _Source: Finnhub_")
    return "\n".join(lines)


# ─── Gist ─────────────────────────────────────────────────────────────────────
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
        logger.warning("GIST_ID/GITHUB_TOKEN manquant — stockage en mémoire uniquement.")
        _cache = empty
        return _cache
    try:
        resp = httpx.get(f"https://api.github.com/gists/{GIST_ID}", headers=_gist_headers(), timeout=10)
        resp.raise_for_status()
        _cache = json.loads(resp.json()["files"][GIST_FILENAME]["content"])
        for ch in CHANNEL_IDS:
            if ch not in _cache.setdefault("channels", []):
                _cache["channels"].append(ch)
        logger.info("Données chargées depuis Gist.")
    except Exception as e:
        logger.error(f"Erreur Gist: {e}. État vide utilisé.")
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
        logger.info("Données sauvegardées dans Gist.")
    except Exception as e:
        logger.error(f"Erreur sauvegarde Gist: {e}")

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


# ─── Commandes ────────────────────────────────────────────────────────────────
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 *Bienvenue sur Trading Calendar Bot\\!*\n\n"
        "📊 *Marchés:*\n"
        "/statut — Statut des marchés en direct\n"
        "/abonner — Alertes matin \\+ soir automatiques\n"
        "/desabonner — Arrêter les alertes\n"
        "/marches — Choisir les bourses à suivre\n"
        "/feries — Jours fériés à venir\n\n"
        "📅 *Actualités:*\n"
        "/evenements — Événements économiques \\(FOMC, CPI…\\)\n"
        "/actualites — Actualités marchés \\& géopolitiques\n\n"
        "📢 *Canaux:*\n"
        "/ajoutcanal @canal — Publier sur un canal\n"
        "/supprcanal @canal — Retirer un canal\n"
        "/canaux — Voir les canaux enregistrés",
        parse_mode="MarkdownV2",
    )

async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await cmd_start(update, ctx)

async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    mics    = get_subscriber_markets(chat_id) or DEFAULT_MICS
    await update.message.reply_text("⏳ Vérification des marchés…")
    try:
        await update.message.reply_text(build_status_message(mics), parse_mode="MarkdownV2")
    except Exception as e:
        logger.error(f"Erreur statut: {e}")
        await update.message.reply_text("❌ Erreur\\. Réessayez plus tard\\.", parse_mode="MarkdownV2")

async def cmd_events(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not FINNHUB_TOKEN:
        await update.message.reply_text(
            "⚠️ *Clé Finnhub non configurée\\!*\n\nAjoutez `FINNHUB_TOKEN` dans vos secrets GitHub\\.",
            parse_mode="MarkdownV2",
        )
        return
    await update.message.reply_text("⏳ Chargement des événements…")
    events = await fetch_economic_events(days_ahead=7)
    await update.message.reply_text(
        build_events_message(events),
        parse_mode="MarkdownV2",
        disable_web_page_preview=True,
    )

async def cmd_news(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not FINNHUB_TOKEN:
        await update.message.reply_text(
            "⚠️ *Clé Finnhub non configurée\\!*\n\nAjoutez `FINNHUB_TOKEN` dans vos secrets GitHub\\.",
            parse_mode="MarkdownV2",
        )
        return
    await update.message.reply_text("⏳ Chargement des actualités…")
    news = await fetch_market_news()
    await update.message.reply_text(
        build_news_message(news),
        parse_mode="MarkdownV2",
        disable_web_page_preview=True,
    )

async def cmd_subscribe(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    add_subscriber(update.effective_chat.id)
    await update.message.reply_text(
        "✅ *Abonnement activé\\!*\n\n"
        "🌅 *Matin 07:00 UTC* — Marchés \\+ Événements \\+ Actualités\n"
        "🌆 *Soir 17:00 UTC* — Marchés \\+ Actualités\n\n"
        "Utilisez /marches pour choisir vos bourses\\.",
        parse_mode="MarkdownV2",
    )

async def cmd_unsubscribe(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if str(chat_id) in get_subscribers():
        remove_subscriber(chat_id)
        await update.message.reply_text("🔕 Désabonnement effectué\\.", parse_mode="MarkdownV2")
    else:
        await update.message.reply_text("Vous n'étiez pas abonné\\. Utilisez /abonner\\.", parse_mode="MarkdownV2")

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
    keyboard.append([InlineKeyboardButton("💾 Enregistrer", callback_data="markets:done")])
    await update.message.reply_text(
        "🌍 *Choisissez vos marchés:*\n_\\(appuyez pour activer\\/désactiver\\)_",
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
            "✅ *Enregistré\\!* Vous suivez:\n" + "\n".join(f"  • {esc(n)}" for n in names),
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
        keyboard.append([InlineKeyboardButton("💾 Enregistrer", callback_data="markets:done")])
        await query.edit_message_reply_markup(InlineKeyboardMarkup(keyboard))

async def cmd_holidays(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    mics    = (get_subscriber_markets(chat_id) or DEFAULT_MICS)[:6]
    await update.message.reply_text("⏳ Chargement des jours fériés…")
    all_holidays = []
    for mic in mics:
        all_holidays.extend(get_upcoming_holidays(mic, days=30))
    if not all_holidays:
        await update.message.reply_text("🎉 Aucun jour férié dans les 30 prochains jours\!", parse_mode="MarkdownV2")
        return

    # Group by date
    from collections import defaultdict
    by_date = defaultdict(list)
    for h in all_holidays:
        by_date[h["date"]].append(h)

    lines = ["*🎌 Jours fériés — 30 prochains jours*", ""]
    for day in sorted(by_date.keys()):
        entries = by_date[day]
        try:
            dt     = datetime.strptime(day, "%Y-%m-%d")
            day_fr = dt.strftime("%d %B %Y")
        except Exception:
            day_fr = day
        lines.append(f"📅 *{esc(day_fr)}*")
        for h in entries:
            holiday_name = h.get("holiday_name", "Jour férié")
            market_name  = h["name"]
            lines.append(f"   🔴 {esc(market_name)} — _{esc(holiday_name)}_")
        lines.append("")

    await update.message.reply_text("\n".join(lines), parse_mode="MarkdownV2")

async def cmd_addchannel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text(
            "Usage: `/ajoutcanal @votrecanal`\n\n⚠️ Le bot doit être *Administrateur* du canal\\!",
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
            f"❌ Impossible d'accéder à `{esc(channel)}`\\.\nVérifiez que le bot est Admin du canal\\.",
            parse_mode="MarkdownV2",
        )
        return
    add_channel(channel_id)
    await update.message.reply_text(
        f"✅ *{channel_title}* ajouté\\! Alertes à 07:00 et 17:00 UTC\\.",
        parse_mode="MarkdownV2",
    )

async def cmd_removechannel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text("Usage: `/supprcanal @votrecanal`", parse_mode="MarkdownV2")
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
    await update.message.reply_text(f"🗑 *{title}* retiré\\.", parse_mode="MarkdownV2")

async def cmd_listchannels(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    channels = get_channels()
    if not channels:
        await update.message.reply_text("Aucun canal\\. Utilisez `/ajoutcanal @votrecanal`\\.", parse_mode="MarkdownV2")
        return
    lines = ["*📢 Canaux enregistrés:*", ""]
    for ch in channels:
        try:
            chat = await ctx.bot.get_chat(ch)
            lines.append(f"  • *{esc(chat.title)}* \\(`{esc(ch)}`\\)")
        except Exception:
            lines.append(f"  • `{esc(ch)}`")
    await update.message.reply_text("\n".join(lines), parse_mode="MarkdownV2")


# ─── Digest ───────────────────────────────────────────────────────────────────
async def send_digest(ctx, is_morning: bool):
    subs     = get_subscribers()
    channels = get_channels()
    targets  = {cid: mics for cid, mics in subs.items()}
    for ch in channels:
        if ch not in targets:
            targets[ch] = DEFAULT_MICS
    if not targets:
        return

    label = "matin" if is_morning else "soir"
    logger.info(f"Digest {label}: envoi à {len(targets)} destinataire(s).")

    events = await fetch_economic_events() if FINNHUB_TOKEN else []
    news   = await fetch_market_news()     if FINNHUB_TOKEN else []

    for chat_id, mics in targets.items():
        try:
            title = "🌅 Digest Matin" if is_morning else "🌆 Digest Soir"
            await ctx.bot.send_message(
                chat_id=chat_id,
                text=build_status_message(mics, title=title),
                parse_mode="MarkdownV2",
            )
            if is_morning and events:
                await ctx.bot.send_message(
                    chat_id=chat_id,
                    text=build_events_message(events, "📅 Événements de la semaine"),
                    parse_mode="MarkdownV2",
                    disable_web_page_preview=True,
                )
            if news:
                news_title = "📰 Actualités du matin" if is_morning else "📰 Actualités du soir"
                await ctx.bot.send_message(
                    chat_id=chat_id,
                    text=build_news_message(news, news_title),
                    parse_mode="MarkdownV2",
                    disable_web_page_preview=True,
                )
            logger.info(f"  ✓ Envoyé à {chat_id}")
        except Exception as e:
            logger.error(f"  ✗ Échec {chat_id}: {e}")

async def morning_digest_job(ctx: ContextTypes.DEFAULT_TYPE):
    await send_digest(ctx, is_morning=True)

async def evening_digest_job(ctx: ContextTypes.DEFAULT_TYPE):
    await send_digest(ctx, is_morning=False)


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    if not TELEGRAM_TOKEN:
        raise ValueError("❌ TELEGRAM_TOKEN non défini!")
    if not FINNHUB_TOKEN:
        logger.warning("⚠️  FINNHUB_TOKEN non défini — actualités désactivées.")
    if not GIST_ID:
        logger.warning("⚠️  GIST_ID non défini — données non persistantes.")

    load_data()

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start",        cmd_start))
    app.add_handler(CommandHandler("aide",         cmd_help))
    app.add_handler(CommandHandler("help",         cmd_help))
    app.add_handler(CommandHandler("statut",       cmd_status))
    app.add_handler(CommandHandler("status",       cmd_status))
    app.add_handler(CommandHandler("evenements",   cmd_events))
    app.add_handler(CommandHandler("events",       cmd_events))
    app.add_handler(CommandHandler("actualites",   cmd_news))
    app.add_handler(CommandHandler("news",         cmd_news))
    app.add_handler(CommandHandler("abonner",      cmd_subscribe))
    app.add_handler(CommandHandler("subscribe",    cmd_subscribe))
    app.add_handler(CommandHandler("desabonner",   cmd_unsubscribe))
    app.add_handler(CommandHandler("unsubscribe",  cmd_unsubscribe))
    app.add_handler(CommandHandler("marches",      cmd_markets))
    app.add_handler(CommandHandler("markets",      cmd_markets))
    app.add_handler(CommandHandler("feries",       cmd_holidays))
    app.add_handler(CommandHandler("holidays",     cmd_holidays))
    app.add_handler(CommandHandler("ajoutcanal",   cmd_addchannel))
    app.add_handler(CommandHandler("addchannel",   cmd_addchannel))
    app.add_handler(CommandHandler("supprcanal",   cmd_removechannel))
    app.add_handler(CommandHandler("removechannel",cmd_removechannel))
    app.add_handler(CommandHandler("canaux",       cmd_listchannels))
    app.add_handler(CommandHandler("listchannels", cmd_listchannels))
    app.add_handler(CallbackQueryHandler(callback_markets))

    app.job_queue.run_daily(
        morning_digest_job,
        time=dtime(hour=MORNING_HOUR, minute=MORNING_MINUTE),
        name="digest_matin",
    )
    app.job_queue.run_daily(
        evening_digest_job,
        time=dtime(hour=EVENING_HOUR, minute=EVENING_MINUTE),
        name="digest_soir",
    )

    logger.info("🤖 Bot en cours d'exécution.")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()

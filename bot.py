# ============================================================
# bot.py — BPFAM TARANTO (ptb v21+)
# 2 bottoni interni (Menù / Contatti-Info)
# Utenti su SQLite + Admin blindato + Backup (auto+manuale) + Restore
# Anti-conflict + Webhook guard (polling sicuro)
# ============================================================

import os
import csv
import sqlite3
import logging
import shutil
from io import BytesIO
from datetime import datetime, timezone, time as dtime
from pathlib import Path

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

VERSION = "2btn-secure-restore-1.2"

# ===== LOGGING =====
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("bpfam-taranto-bot")

# ===== ENV / CONFIG =====
BOT_TOKEN   = os.environ.get("BOT_TOKEN")  # <-- mettilo su Render (NON nel codice)
ADMIN_ID    = int(os.environ.get("ADMIN_ID", "0"))          # il tuo ID Telegram
DB_FILE     = os.environ.get("DB_FILE", "./data/users.db")
BACKUP_DIR  = os.environ.get("BACKUP_DIR", "./backups")
BACKUP_TIME = os.environ.get("BACKUP_TIME", "03:00")        # UTC HH:MM

PHOTO_URL   = os.environ.get(
    "PHOTO_URL",
    "https://i.postimg.cc/bv4ssL2t/2A3BDCFD-2D21-41BC-8BFA-9C5D238E5C3B.jpg",
)
WELCOME_TEXT = "🥇Benvenuti nel bot ufficiale di BPFAM-TARANTO🥇"

# Testi interni (puoi personalizzarli quando vuoi)
MENU_PAGE_TEXT = (
    "📖 *MENÙ — BPFAM TARANTO*\n"
    "Benvenuto nel menù interno del bot.\n\n"
    "• Voce A\n• Voce B\n• Voce C\n"
)

INFO_PAGE_TEXT = (
    "📲 *CONTATTI & INFO — BPFAM TARANTO*\n"
    "Canali verificati e contatti ufficiali.\n\n"
    "Instagram: @bpfamofficial\n"
    "Canale Telegram: t.me/...\n"
    "Contatto diretto: @contattobpfam\n"
)

# ===== DB =====
def init_db():
    Path(DB_FILE).parent.mkdir(parents=True, exist_ok=True)
    Path(BACKUP_DIR).mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users(
            user_id     INTEGER PRIMARY KEY,
            username    TEXT,
            first_name  TEXT,
            last_name   TEXT,
            first_seen  TEXT,
            last_seen   TEXT
        )
    """)
    conn.commit()
    conn.close()

def upsert_user(u):
    if not u:
        return
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    cur.execute("SELECT 1 FROM users WHERE user_id=?", (u.id,))
    if cur.fetchone():
        cur.execute("""
            UPDATE users SET username=?, first_name=?, last_name=?, last_seen=?
            WHERE user_id=?
        """, (u.username, u.first_name, u.last_name, now, u.id))
    else:
        cur.execute("""
            INSERT INTO users(user_id, username, first_name, last_name, first_seen, last_seen)
            VALUES(?,?,?,?,?,?)
        """, (u.id, u.username, u.first_name, u.last_name, now, now))
    conn.commit()
    conn.close()

# ===== UTILS =====
def is_admin(uid: int | None) -> bool:
    return bool(uid) and uid == ADMIN_ID

def admin_only(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id if update.effective_user else None
        if not is_admin(uid):
            return
        return await func(update, context)
    return wrapper

def parse_backup_time(hhmm: str) -> dtime:
    try:
        hh, mm = hhmm.split(":")
        return dtime(hour=int(hh), minute=int(mm), tzinfo=timezone.utc)
    except Exception:
        return dtime(hour=3, minute=0, tzinfo=timezone.utc)

def main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("📖 MENÙ", callback_data="OPEN_MENU"),
        InlineKeyboardButton("📲 CONTATTI-INFO", callback_data="OPEN_INFO"),
    ]])

# ===== HANDLERS — USER =====
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat
    if user:
        upsert_user(user)
    if not chat:
        return

    try:
        await chat.send_photo(photo=PHOTO_URL, caption=WELCOME_TEXT, reply_markup=main_keyboard())
    except Exception as e:
        logger.warning("Foto non inviata (%s), invio solo testo.", e)
        await chat.send_message(WELCOME_TEXT, reply_markup=main_keyboard())

async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    data = q.data or ""
    if data == "OPEN_MENU":
        await q.message.reply_text(MENU_PAGE_TEXT, reply_markup=main_keyboard(), parse_mode="Markdown")
    elif data == "OPEN_INFO":
        await q.message.reply_text(INFO_PAGE_TEXT, reply_markup=main_keyboard(), parse_mode="Markdown")

# ===== HANDLERS — ADMIN =====
@admin_only
async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = sqlite3.connect(DB_FILE); cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM users"); count = cur.fetchone()[0]
    conn.close()
    msg = (
        f"✅ Online v{VERSION}\n"
        f"👥 Utenti salvati: {count}\n"
        f"💾 DB: {DB_FILE}\n"
        f"🗂️ Backup dir: {BACKUP_DIR}\n"
        f"⏰ Backup auto (UTC): {BACKUP_TIME}"
    )
    await update.effective_chat.send_message(msg)

@admin_only
async def list_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = sqlite3.connect(DB_FILE); cur = conn.cursor()
    cur.execute("SELECT user_id, username, first_name, last_name, last_seen FROM users ORDER BY last_seen DESC LIMIT 50")
    rows = cur.fetchall(); conn.close()
    if not rows:
        await update.effective_chat.send_message("Nessun utente registrato.")
        return
    lines = []
    for uid, un, fn, ln, ls in rows:
        tag = f"@{un}" if un else "-"
        name = " ".join([x for x in [fn, ln] if x]) or "-"
        lines.append(f"• {uid} {tag} — {name} — {ls[:19].replace('T',' ')}Z")
    text = "Ultimi 50 utenti:\n" + "\n".join(lines)
    await update.effective_chat.send_message(text)

@admin_only
async def export_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = sqlite3.connect(DB_FILE); cur = conn.cursor()
    cur.execute("SELECT user_id, username, first_name, last_name, first_seen, last_seen FROM users")
    rows = cur.fetchall(); conn.close()

    buf = BytesIO()
    buf.write("user_id,username,first_name,last_name,first_seen,last_seen\n".encode())
    for r in rows:
        safe = ["" if v is None else str(v).replace(",", " ") for v in r]
        buf.write((",".join(safe) + "\n").encode())
    buf.seek(0)
    filename = f"users_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
    await update.effective_chat.send_document(document=InputFile(buf, filename=filename))

@admin_only
async def backup_db(update: Update, context: ContextTypes.DEFAULT_TYPE):
    Path(BACKUP_DIR).mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    dest = Path(BACKUP_DIR) / f"users_{ts}.db"
    try:
        shutil.copyfile(DB_FILE, dest)
    except Exception as e:
        await update.effective_chat.send_message(f"❌ Errore backup: {e}")
        return
    try:
        await update.effective_chat.send_document(document=InputFile(str(dest)))
    except Exception as e:
        await update.effective_chat.send_message(f"Backup salvato su disco, ma invio fallito: {e}")

@admin_only
async def restore_db(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Ripristina il DB da un file .db che hai INVIATO al bot.
    Usa così:
    1) Invia il file .db al bot (come documento)
    2) Fai REPLY a quel messaggio con il comando /restore_db
    """
    msg = update.effective_message
    if not msg or not msg.reply_to_message or not msg.reply_to_message.document:
        await update.effective_chat.send_message(
            "Per ripristinare: invia un file .db al bot e poi fai *rispondi* a quel file con /restore_db",
            parse_mode="Markdown",
        )
        return

    doc = msg.reply_to_message.document
    if not doc.file_name.endswith(".db"):
        await update.effective_chat.send_message("Il file deve avere estensione .db")
        return

    try:
        file = await doc.get_file()
        tmp_path = Path(BACKUP_DIR) / ("restore_tmp_" + doc.file_unique_id + ".db")
        Path(BACKUP_DIR).mkdir(parents=True, exist_ok=True)
        await file.download_to_drive(custom_path=str(tmp_path))
    except Exception as e:
        await update.effective_chat.send_message(f"❌ Errore download file: {e}")
        return

    try:
        safety_copy = Path(BACKUP_DIR) / f"pre_restore_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.bak"
        if Path(DB_FILE).exists():
            shutil.copyfile(DB_FILE, safety_copy)
    except Exception as e:
        await update.effective_chat.send_message(f"❌ Errore copia di sicurezza: {e}")
        return

    try:
        Path(DB_FILE).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(tmp_path, DB_FILE)
        await update.effective_chat.send_message("✅ Database ripristinato con successo. Usa /status per verificare.")
    except Exception as e:
        await update.effective_chat.send_message(f"❌ Errore ripristino DB: {e}")
    finally:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass

async def daily_backup_job(context: ContextTypes.DEFAULT_TYPE):
    Path(BACKUP_DIR).mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    dest = Path(BACKUP_DIR) / f"users_{ts}.db"
    try:
        shutil.copyfile(DB_FILE, dest)
        logger.info("Backup auto eseguito: %s", dest)
        if ADMIN_ID:
            try:
                await context.bot.send_document(chat_id=ADMIN_ID, document=InputFile(str(dest)))
            except Exception as e:
                logger.warning("Invio backup auto fallito: %s", e)
    except Exception as e:
        logger.error("Errore backup auto: %s", e)

# --- Webhook guard (anti-conflict) ---
async def _post_init(app):
    try:
        await app.bot.delete_webhook(drop_pending_updates=True)
        logger.info("Webhook rimosso (guard) — polling sicuro.")
    except Exception as e:
        logger.warning("Impossibile rimuovere webhook: %s", e)

# ===== MAIN =====
def main():
    if not BOT_TOKEN:
        raise SystemExit("Errore: variabile d'ambiente BOT_TOKEN mancante.")
    if not ADMIN_ID:
        logger.warning("ADMIN_ID non impostato: i comandi admin saranno inaccessibili.")

    init_db()

    app = ApplicationBuilder().token(BOT_TOKEN).post_init(_post_init).build()

    # --- FIX: assicurati che la JobQueue esista (evita AttributeError: NoneType run_daily)
    if not getattr(app, "job_queue", None):
        from telegram.ext import JobQueue
        jq = JobQueue()
        jq.set_application(app)
        app.job_queue = jq

    # Comandi utente
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(on_button))

    # Comandi admin
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("list", list_users))
    app.add_handler(CommandHandler("export", export_users))
    app.add_handler(CommandHandler("backup_db", backup_db))
    app.add_handler(CommandHandler("restore_db", restore_db))

    # (opzionale) intercetta documenti (serve per /restore_db via reply)
    app.add_handler(MessageHandler(filters.Document.ALL, lambda *_: None))

    # job di backup giornaliero (UTC)
    bt = parse_backup_time(BACKUP_TIME)
    app.job_queue.run_daily(daily_backup_job, time=bt, name="daily-backup")

    logger.info("Bot avviato — v%s", VERSION)
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()
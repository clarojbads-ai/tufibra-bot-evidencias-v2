# bot_fotos.py
# Requisitos:
#   pip install -U python-telegram-bot==21.6 gspread google-auth
#
# PowerShell (PC):
#   cd "C:\Users\Diego_Siancas\Desktop\BOT TuFibra"
#   $env:BOT_TOKEN="TU_TOKEN"
#   $env:ROUTING_JSON='{"-5252607752":{"evidence":"-5143236367","summary":"-5143236367"}}'
#   $env:SHEET_ID="TU_SHEET_ID"
#   $env:GOOGLE_CREDS_JSON="google_creds.json"
#   $env:BOT_VERSION="1.0.0"
#   python bot_fotos.py

import os
import json
import sqlite3
import logging
import time
import uuid
import re
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple

import gspread
from google.oauth2.service_account import Credentials

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Message
from telegram.error import BadRequest
from telegram.request import HTTPXRequest
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# =========================
# CONFIG
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DB_PATH = os.getenv("DB_PATH", "bot_fotos.sqlite3")
ROUTING_JSON = os.getenv("ROUTING_JSON", "").strip()

MAX_MEDIA_PER_STEP = 8

# Perú (UTC-5)
PERU_TZ = timezone(timedelta(hours=-5))

# (Deprecated as hardcode) - mantenido solo como fallback si la hoja TECNICOS está vacía o no disponible
TECHNICIANS_FALLBACK = [
    "FLORO FERNANDEZ VASQUEZ",
    "ANTONY SALVADOR CORONADO",
    "DANIEL EDUARDO LUCENA PIÑANGO",
    "JOSE RODAS BERECHE",
    "LUIS OMAR EPEQUIN ZAPATA",
    "CESAR ABRAHAM VASQUEZ MEZA",
]

SERVICE_TYPES = ["ALTA NUEVA", "POSTVENTA", "AVERIAS"]

# EXTERNA (1..11) -> step_no interno 5..15
EXTERNA_MENU: List[Tuple[int, str, int]] = [
    (1, "FACHADA", 5),
    (2, "CTO", 6),
    (3, "POTENCIA EN CTO", 7),
    (4, "PRECINTO ROTULADOR", 8),
    (5, "FALSO TRAMO", 9),
    (6, "ANCLAJE", 10),
    (7, "ROSETA + MEDICION POTENCIA", 11),
    (8, "MAC ONT", 12),
    (9, "ONT", 13),
    (10, "TEST DE VELOCIDAD", 14),
    (11, "ACTA DE INSTALACION", 15),
]

# INTERNA (1..9)
INTERNA_MENU: List[Tuple[int, str, int]] = [
    (1, "FACHADA", 5),
    (2, "CTO", 6),
    (3, "POTENCIA EN CTO", 7),
    (4, "PRECINTO ROTULADOR", 8),
    (5, "ROSETA + MEDICION POTENCIA", 11),
    (6, "MAC ONT", 12),
    (7, "ONT", 13),
    (8, "TEST DE VELOCIDAD", 14),
    (9, "ACTA DE INSTALACION", 15),
]

STEP_MEDIA_DEFS = {
    5:  ("FACHADA", "Envía foto de Fachada con placa de dirección y/o suministro eléctrico"),
    6:  ("CTO", "Envía foto panorámica de la CTO o FAT rotulada"),
    7:  ("POTENCIA EN CTO", "Envía la foto de la medida de potencia del puerto a utilizar"),
    8:  ("PRECINTO ROTULADOR", "Envía la foto del cintillo rotulado identificando al cliente (DNI o CE y nro de puerto)"),
    9:  ("FALSO TRAMO", "Envía foto del tramo de ingreso al domicilio"),
    10: ("ANCLAJE", "Envía foto del punto de anclaje de la fibra drop en el domicilio"),
    11: ("ROSETA + MEDICION POTENCIA", "Envía foto de la roseta abierta y medición de potencia"),
    12: ("MAC ONT", "Envía foto de la MAC (Etiqueta) de la ONT y/o equipos usados"),
    13: ("ONT", "Envía foto panorámica de la ONT operativa"),
    14: ("TEST DE VELOCIDAD", "Envía foto del test de velocidad App Speedtest mostrar ID y fecha claramente"),
    15: ("ACTA DE INSTALACION", "Envía foto del acta de instalación completa con la firma de cliente y datos llenos"),
}

# =========================
# Google Sheets CONFIG
# =========================
SHEET_ID = os.getenv("SHEET_ID", "").strip()
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON", "").strip()
GOOGLE_CREDS_JSON_TEXT = os.getenv("GOOGLE_CREDS_JSON_TEXT", "").strip()
BOT_VERSION = os.getenv("BOT_VERSION", "1.0.0").strip()

# Tabs existentes (historial)
CASOS_COLUMNS = [
    "case_id", "estado", "chat_id_origen", "fecha_inicio", "hora_inicio", "fecha_cierre", "hora_cierre", "duracion_min",
    "tecnico_nombre", "tecnico_user_id", "tipo_servicio", "codigo_abonado", "modo_instalacion", "latitud", "longitud",
    "link_maps", "total_pasos", "pasos_aprobados", "pasos_rechazados", "total_evidencias", "requiere_aprobacion",
    "registrado_en", "version_bot"
]
DETALLE_PASOS_COLUMNS = [
    "case_id", "paso_numero", "paso_nombre", "attempt", "estado_paso", "revisado_por", "fecha_revision", "hora_revision",
    "motivo_rechazo", "cantidad_fotos", "ids_mensajes"
]
EVIDENCIAS_COLUMNS = [
    "case_id", "paso_numero", "attempt", "file_id", "file_unique_id", "mensaje_telegram_id", "fecha_carga", "hora_carga",
    "grupo_evidencias"
]
CONFIG_COLUMNS = ["parametro", "valor"]

# Tabs nuevas (config pro)
TECNICOS_TAB = "TECNICOS"
ROUTING_TAB = "ROUTING"
PAIRING_TAB = "PAIRING"

TECNICOS_COLUMNS = ["nombre", "activo", "orden", "alias", "updated_at"]
ROUTING_COLUMNS = ["origin_chat_id", "evidence_chat_id", "summary_chat_id", "alias", "activo", "updated_by", "updated_at"]
PAIRING_COLUMNS = ["code", "origin_chat_id", "purpose", "expires_at", "used", "created_by", "created_at", "used_by", "used_at"]

# Cache/refresh
TECH_CACHE_TTL_SEC = int(os.getenv("TECH_CACHE_TTL_SEC", "180"))     # 3 min default
ROUTING_CACHE_TTL_SEC = int(os.getenv("ROUTING_CACHE_TTL_SEC", "180"))  # 3 min default
PAIRING_TTL_MINUTES = int(os.getenv("PAIRING_TTL_MINUTES", "10"))    # 10 min default

# =========================
# Logging
# =========================
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("tufibra_bot")

# =========================
# Safe Telegram helpers (anti-crash callbacks)
# =========================
async def safe_q_answer(q, text: Optional[str] = None, show_alert: bool = False) -> None:
    """
    Evita crash por:
    - Query is too old and response timeout expired or query id is invalid
    - Invalid callback query
    """
    if q is None:
        return
    try:
        await q.answer(text=text, show_alert=show_alert, cache_time=0)
    except BadRequest as e:
        msg = str(e).lower()
        if "query is too old" in msg or "response timeout expired" in msg or "query id is invalid" in msg:
            return
        if "invalid callback query" in msg:
            return
        log.warning(f"safe_q_answer BadRequest: {e}")
    except Exception as e:
        log.warning(f"safe_q_answer error: {e}")


async def safe_edit_message_text(q, text: str, **kwargs) -> None:
    """
    Evita crash por:
    - Message is not modified
    - Message to edit not found
    """
    if q is None:
        return
    try:
        await q.edit_message_text(text=text, **kwargs)
    except BadRequest as e:
        msg = str(e).lower()
        if "message is not modified" in msg:
            return
        if "message to edit not found" in msg:
            return
        if "query is too old" in msg or "response timeout expired" in msg or "query id is invalid" in msg:
            return
        log.warning(f"safe_edit_message_text BadRequest: {e}")
    except Exception as e:
        log.warning(f"safe_edit_message_text error: {e}")

# =========================
# DB helpers
# =========================
def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_iso(dt_s: str) -> Optional[datetime]:
    if not dt_s:
        return None
    try:
        d = datetime.fromisoformat(dt_s)
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d
    except Exception:
        return None


def fmt_time_pe(dt_s: str) -> str:
    d = parse_iso(dt_s)
    if not d:
        return "-"
    return d.astimezone(PERU_TZ).strftime("%H:%M")


def fmt_date_pe(dt_s: str) -> str:
    d = parse_iso(dt_s)
    if not d:
        return "-"
    return d.astimezone(PERU_TZ).strftime("%Y-%m-%d")


def _col_exists(conn: sqlite3.Connection, table: str, col: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r["name"] == col for r in rows)


def init_db():
    with db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cases (
                case_id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                username TEXT,
                created_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT NOT NULL,
                step_index INTEGER NOT NULL,
                phase TEXT,
                pending_step_no INTEGER,
                technician_name TEXT,
                service_type TEXT,
                abonado_code TEXT,
                location_lat REAL,
                location_lon REAL,
                location_at TEXT,
                install_mode TEXT
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_cases_open ON cases(chat_id, user_id, status);")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_config (
                chat_id INTEGER PRIMARY KEY,
                approval_required INTEGER NOT NULL DEFAULT 1,
                updated_at TEXT
            );
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS step_state (
                case_id INTEGER NOT NULL,
                step_no INTEGER NOT NULL,
                attempt INTEGER NOT NULL DEFAULT 1,
                submitted INTEGER NOT NULL DEFAULT 0,
                approved INTEGER,
                reviewed_by INTEGER,
                reviewed_at TEXT,
                created_at TEXT NOT NULL,
                reject_reason TEXT,
                reject_reason_by INTEGER,
                reject_reason_at TEXT,
                PRIMARY KEY(case_id, step_no, attempt),
                FOREIGN KEY(case_id) REFERENCES cases(case_id)
            );
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS media (
                media_id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id INTEGER NOT NULL,
                step_no INTEGER NOT NULL,
                attempt INTEGER NOT NULL,
                file_type TEXT NOT NULL,
                file_id TEXT NOT NULL,
                file_unique_id TEXT,
                tg_message_id INTEGER NOT NULL,
                meta_json TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(case_id) REFERENCES cases(case_id)
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_media_case_step ON media(case_id, step_no, attempt);")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS auth_text (
                auth_id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id INTEGER NOT NULL,
                step_no INTEGER NOT NULL,
                attempt INTEGER NOT NULL,
                text TEXT NOT NULL,
                tg_message_id INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(case_id) REFERENCES cases(case_id)
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_auth_text_case_step ON auth_text(case_id, step_no, attempt);")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pending_inputs (
                pending_id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                kind TEXT NOT NULL,
                case_id INTEGER NOT NULL,
                step_no INTEGER NOT NULL,
                attempt INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                reply_to_message_id INTEGER,
                tech_user_id INTEGER
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pending_inputs ON pending_inputs(chat_id, user_id, kind);")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sheet_outbox (
                outbox_id INTEGER PRIMARY KEY AUTOINCREMENT,
                sheet_name TEXT NOT NULL,
                op_type TEXT NOT NULL,
                dedupe_key TEXT NOT NULL,
                row_json TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'PENDING',
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                next_retry_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_outbox_pending ON sheet_outbox(status, next_retry_at);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_outbox_key ON sheet_outbox(sheet_name, dedupe_key);")

        # Soft migrations
        for col, ddl in [
            ("finished_at", "TEXT"),
            ("phase", "TEXT"),
            ("pending_step_no", "INTEGER"),
            ("technician_name", "TEXT"),
            ("service_type", "TEXT"),
            ("abonado_code", "TEXT"),
            ("location_lat", "REAL"),
            ("location_lon", "REAL"),
            ("location_at", "TEXT"),
            ("install_mode", "TEXT"),
        ]:
            if not _col_exists(conn, "cases", col):
                conn.execute(f"ALTER TABLE cases ADD COLUMN {col} {ddl};")

        for col, ddl in [
            ("reject_reason", "TEXT"),
            ("reject_reason_by", "INTEGER"),
            ("reject_reason_at", "TEXT"),
        ]:
            if not _col_exists(conn, "step_state", col):
                conn.execute(f"ALTER TABLE step_state ADD COLUMN {col} {ddl};")

        for col, ddl in [
            ("reply_to_message_id", "INTEGER"),
            ("tech_user_id", "INTEGER"),
        ]:
            if not _col_exists(conn, "pending_inputs", col):
                conn.execute(f"ALTER TABLE pending_inputs ADD COLUMN {col} {ddl};")

        conn.commit()


def set_approval_required(chat_id: int, required: bool):
    with db() as conn:
        conn.execute(
            """
            INSERT INTO chat_config(chat_id, approval_required, updated_at)
            VALUES(?,?,?)
            ON CONFLICT(chat_id) DO UPDATE
              SET approval_required=excluded.approval_required, updated_at=excluded.updated_at
            """,
            (chat_id, 1 if required else 0, now_utc()),
        )
        conn.commit()


def get_approval_required(chat_id: int) -> bool:
    with db() as conn:
        row = conn.execute("SELECT approval_required FROM chat_config WHERE chat_id=?", (chat_id,)).fetchone()
        if row is None:
            conn.execute(
                "INSERT OR IGNORE INTO chat_config(chat_id, approval_required, updated_at) VALUES(?,?,?)",
                (chat_id, 1, now_utc()),
            )
            conn.commit()
            return True
        return bool(row["approval_required"])


def get_open_case(chat_id: int, user_id: int) -> Optional[sqlite3.Row]:
    with db() as conn:
        return conn.execute(
            "SELECT * FROM cases WHERE chat_id=? AND user_id=? AND status='OPEN' ORDER BY case_id DESC LIMIT 1",
            (chat_id, user_id),
        ).fetchone()


def get_case(case_id: int) -> Optional[sqlite3.Row]:
    with db() as conn:
        return conn.execute("SELECT * FROM cases WHERE case_id=?", (case_id,)).fetchone()


def update_case(case_id: int, **fields):
    if not fields:
        return
    keys = list(fields.keys())
    vals = [fields[k] for k in keys]
    sets = ", ".join([f"{k}=?" for k in keys])
    with db() as conn:
        conn.execute(f"UPDATE cases SET {sets} WHERE case_id=?", (*vals, case_id))
        conn.commit()


def create_or_reset_case(chat_id: int, user_id: int, username: str) -> sqlite3.Row:
    with db() as conn:
        row = conn.execute(
            "SELECT * FROM cases WHERE chat_id=? AND user_id=? AND status='OPEN' ORDER BY case_id DESC LIMIT 1",
            (chat_id, user_id),
        ).fetchone()

        if row:
            conn.execute(
                """
                UPDATE cases
                SET created_at=?,
                    finished_at=NULL,
                    status='OPEN',
                    step_index=0,
                    phase='WAIT_TECHNICIAN',
                    pending_step_no=NULL,
                    technician_name=NULL,
                    service_type=NULL,
                    abonado_code=NULL,
                    location_lat=NULL,
                    location_lon=NULL,
                    location_at=NULL,
                    install_mode=NULL
                WHERE case_id=?
                """,
                (now_utc(), row["case_id"]),
            )
            conn.commit()
            return get_case(int(row["case_id"]))

        conn.execute(
            """
            INSERT INTO cases(chat_id, user_id, username, created_at, finished_at, status, step_index, phase, pending_step_no)
            VALUES(?,?,?,?,NULL,'OPEN',0,'WAIT_TECHNICIAN',NULL)
            """,
            (chat_id, user_id, username, now_utc()),
        )
        conn.commit()
        new_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
        return get_case(int(new_id))

# =========================
# Routing (Sheets cache + fallback)
# =========================
def _safe_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        return int(s)
    except Exception:
        return None


def get_route_for_chat_cached(application: Application, origin_chat_id: int) -> Dict[str, Optional[int]]:
    """
    Ruta principal: cache de ROUTING en Sheets.
    Fallback opcional: ROUTING_JSON.
    """
    try:
        rc = application.bot_data.get("routing_cache") or {}
        row = rc.get(int(origin_chat_id))
        if row and int(row.get("activo", 1)) == 1:
            return {
                "evidence": _safe_int(row.get("evidence_chat_id")),
                "summary": _safe_int(row.get("summary_chat_id")),
            }
    except Exception:
        pass

    # Fallback a variable (migración / emergencia)
    if ROUTING_JSON:
        try:
            mapping = json.loads(ROUTING_JSON)
            cfg = mapping.get(str(origin_chat_id)) or {}
            ev = cfg.get("evidence")
            sm = cfg.get("summary")
            return {"evidence": int(ev) if ev else None, "summary": int(sm) if sm else None}
        except Exception as e:
            log.warning(f"ROUTING_JSON inválido: {e}")

    return {"evidence": None, "summary": None}


async def maybe_copy_to_group(
    context: ContextTypes.DEFAULT_TYPE,
    dest_chat_id: Optional[int],
    file_type: str,
    file_id: str,
    caption: str,
):
    if not dest_chat_id:
        return
    try:
        if file_type == "video":
            await context.bot.send_video(chat_id=dest_chat_id, video=file_id, caption=caption[:1024])
        else:
            await context.bot.send_photo(chat_id=dest_chat_id, photo=file_id, caption=caption[:1024])
    except Exception as e:
        log.warning(f"No pude copiar evidencia a destino {dest_chat_id}: {e}")

# =========================
# step_state helpers
# =========================
def _max_attempt(case_id: int, step_no: int) -> int:
    with db() as conn:
        row = conn.execute(
            "SELECT MAX(attempt) AS mx FROM step_state WHERE case_id=? AND step_no=?",
            (case_id, step_no),
        ).fetchone()
        mx = row["mx"] if row and row["mx"] is not None else 0
        return int(mx) if mx else 0


def ensure_step_state(case_id: int, step_no: int) -> sqlite3.Row:
    with db() as conn:
        row = conn.execute(
            """
            SELECT * FROM step_state
            WHERE case_id=? AND step_no=? AND submitted=0
            ORDER BY attempt DESC LIMIT 1
            """,
            (case_id, step_no),
        ).fetchone()
        if row:
            return row

        attempt = _max_attempt(case_id, step_no) + 1
        conn.execute(
            """
            INSERT INTO step_state(case_id, step_no, attempt, submitted, approved, reviewed_by, reviewed_at, created_at, reject_reason, reject_reason_by, reject_reason_at)
            VALUES(?,?,?,0,NULL,NULL,NULL,?,NULL,NULL,NULL)
            """,
            (case_id, step_no, attempt, now_utc()),
        )
        conn.commit()
        return conn.execute(
            "SELECT * FROM step_state WHERE case_id=? AND step_no=? AND attempt=?",
            (case_id, step_no, attempt),
        ).fetchone()


def get_latest_submitted_state(case_id: int, step_no: int) -> Optional[sqlite3.Row]:
    with db() as conn:
        return conn.execute(
            """
            SELECT * FROM step_state
            WHERE case_id=? AND step_no=? AND submitted=1
            ORDER BY attempt DESC LIMIT 1
            """,
            (case_id, step_no),
        ).fetchone()


def media_count(case_id: int, step_no: int, attempt: int) -> int:
    with db() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM media WHERE case_id=? AND step_no=? AND attempt=?",
            (case_id, step_no, attempt),
        ).fetchone()
        return int(row["c"]) if row else 0


def media_message_ids(case_id: int, step_no: int, attempt: int) -> List[int]:
    with db() as conn:
        rows = conn.execute(
            "SELECT tg_message_id FROM media WHERE case_id=? AND step_no=? AND attempt=? ORDER BY media_id ASC",
            (case_id, step_no, attempt),
        ).fetchall()
        return [int(r["tg_message_id"]) for r in rows] if rows else []


def total_media_for_case(case_id: int) -> int:
    with db() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM media WHERE case_id=? AND step_no > 0",
            (case_id,),
        ).fetchone()
        return int(row["c"] or 0)


def total_rejects_for_case(case_id: int) -> int:
    with db() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM step_state WHERE case_id=? AND step_no > 0 AND approved=0",
            (case_id,),
        ).fetchone()
        return int(row["c"] or 0)


def total_approved_steps_for_case(case_id: int) -> int:
    with db() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM step_state WHERE case_id=? AND step_no > 0 AND approved=1",
            (case_id,),
        ).fetchone()
        return int(row["c"] or 0)


def add_media(
    case_id: int,
    step_no: int,
    attempt: int,
    file_type: str,
    file_id: str,
    file_unique_id: Optional[str],
    tg_message_id: int,
    meta: Dict[str, Any],
):
    with db() as conn:
        conn.execute(
            """
            INSERT INTO media(case_id, step_no, attempt, file_type, file_id, file_unique_id, tg_message_id, meta_json, created_at)
            VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (
                case_id,
                step_no,
                attempt,
                file_type,
                file_id,
                file_unique_id or "",
                tg_message_id,
                json.dumps(meta, ensure_ascii=False),
                now_utc(),
            ),
        )
        conn.commit()


def mark_submitted(case_id: int, step_no: int, attempt: int):
    with db() as conn:
        conn.execute(
            "UPDATE step_state SET submitted=1 WHERE case_id=? AND step_no=? AND attempt=?",
            (case_id, step_no, attempt),
        )
        conn.commit()


def set_review(case_id: int, step_no: int, attempt: int, approved: int, reviewer_id: int):
    with db() as conn:
        conn.execute(
            """
            UPDATE step_state
            SET approved=?, reviewed_by=?, reviewed_at=?
            WHERE case_id=? AND step_no=? AND attempt=?
            """,
            (approved, reviewer_id, now_utc(), case_id, step_no, attempt),
        )
        conn.commit()


def set_reject_reason(case_id: int, step_no: int, attempt: int, reason: str, reviewer_id: int):
    with db() as conn:
        conn.execute(
            """
            UPDATE step_state
            SET reject_reason=?, reject_reason_by=?, reject_reason_at=?
            WHERE case_id=? AND step_no=? AND attempt=?
            """,
            (reason, reviewer_id, now_utc(), case_id, step_no, attempt),
        )
        conn.commit()


def save_auth_text(case_id: int, auth_step_no: int, attempt: int, text: str, tg_message_id: int):
    """
    auth_step_no DEBE ser negativo (ej: -6) para que no se mezcle con el paso real.
    """
    with db() as conn:
        conn.execute(
            """
            INSERT INTO auth_text(case_id, step_no, attempt, text, tg_message_id, created_at)
            VALUES(?,?,?,?,?,?)
            """,
            (case_id, auth_step_no, attempt, text, tg_message_id, now_utc()),
        )
        conn.commit()


def set_pending_input(
    chat_id: int,
    user_id: int,
    kind: str,
    case_id: int,
    step_no: int,
    attempt: int,
    reply_to_message_id: Optional[int] = None,
    tech_user_id: Optional[int] = None,
):
    with db() as conn:
        conn.execute("DELETE FROM pending_inputs WHERE chat_id=? AND user_id=? AND kind=?", (chat_id, user_id, kind))
        conn.execute(
            """
            INSERT INTO pending_inputs(chat_id, user_id, kind, case_id, step_no, attempt, created_at, reply_to_message_id, tech_user_id)
            VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (chat_id, user_id, kind, case_id, step_no, attempt, now_utc(), reply_to_message_id, tech_user_id),
        )
        conn.commit()


def pop_pending_input(chat_id: int, user_id: int, kind: str) -> Optional[sqlite3.Row]:
    with db() as conn:
        row = conn.execute(
            "SELECT * FROM pending_inputs WHERE chat_id=? AND user_id=? AND kind=? ORDER BY pending_id DESC LIMIT 1",
            (chat_id, user_id, kind),
        ).fetchone()
        if row:
            conn.execute("DELETE FROM pending_inputs WHERE pending_id=?", (row["pending_id"],))
            conn.commit()
        return row

# =========================
# Outbox helpers (Google Sheets - historial)
# =========================
def outbox_enqueue(sheet_name: str, op_type: str, dedupe_key: str, row: Dict[str, Any]):
    now = now_utc()
    row_json = json.dumps(row, ensure_ascii=False)
    with db() as conn:
        existing = conn.execute(
            """
            SELECT outbox_id, status FROM sheet_outbox
            WHERE sheet_name=? AND dedupe_key=? AND status IN ('PENDING','FAILED')
            ORDER BY outbox_id DESC LIMIT 1
            """,
            (sheet_name, dedupe_key),
        ).fetchone()

        if existing:
            conn.execute(
                """
                UPDATE sheet_outbox
                SET row_json=?, op_type=?, status='PENDING', last_error=NULL, next_retry_at=NULL, updated_at=?
                WHERE outbox_id=?
                """,
                (row_json, op_type, now, int(existing["outbox_id"])),
            )
        else:
            conn.execute(
                """
                INSERT INTO sheet_outbox(sheet_name, op_type, dedupe_key, row_json, status, attempts, last_error, next_retry_at, created_at, updated_at)
                VALUES(?,?,?,?, 'PENDING', 0, NULL, NULL, ?, NULL)
                """,
                (sheet_name, op_type, dedupe_key, row_json, now),
            )
        conn.commit()


def outbox_fetch_batch(limit: int = 20) -> List[sqlite3.Row]:
    now = now_utc()
    with db() as conn:
        rows = conn.execute(
            """
            SELECT * FROM sheet_outbox
            WHERE status IN ('PENDING','FAILED')
              AND (next_retry_at IS NULL OR next_retry_at <= ?)
            ORDER BY created_at ASC
            LIMIT ?
            """,
            (now, limit),
        ).fetchall()
        return rows


def outbox_mark_sent(outbox_id: int):
    with db() as conn:
        conn.execute(
            "UPDATE sheet_outbox SET status='SENT', updated_at=? WHERE outbox_id=?",
            (now_utc(), outbox_id),
        )
        conn.commit()


def _next_retry_time(attempts: int) -> str:
    minutes = [1, 2, 4, 8, 15, 30, 60, 120]
    idx = min(attempts, len(minutes) - 1)
    dt = datetime.now(timezone.utc) + timedelta(minutes=minutes[idx])
    return dt.isoformat()


def outbox_mark_failed(outbox_id: int, attempts: int, err: str, dead: bool = False):
    status = "DEAD" if dead else "FAILED"
    next_retry_at = None if dead else _next_retry_time(attempts)
    with db() as conn:
        conn.execute(
            """
            UPDATE sheet_outbox
            SET status=?, attempts=?, last_error=?, next_retry_at=?, updated_at=?
            WHERE outbox_id=?
            """,
            (status, attempts, err[:500], next_retry_at, now_utc(), outbox_id),
        )
        conn.commit()

# =========================
# Google Sheets helpers
# =========================
def sheets_client():
    if not SHEET_ID:
        raise RuntimeError("Falta SHEET_ID. Configura la variable SHEET_ID.")

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]

    # Prioridad 1: JSON en texto (Railway)
    if GOOGLE_CREDS_JSON_TEXT:
        creds_info = json.loads(GOOGLE_CREDS_JSON_TEXT)
        creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    # Prioridad 2: archivo local (PC)
    else:
        if not GOOGLE_CREDS_JSON:
            raise RuntimeError("Falta GOOGLE_CREDS_JSON o GOOGLE_CREDS_JSON_TEXT.")
        creds = Credentials.from_service_account_file(GOOGLE_CREDS_JSON, scopes=scopes)

    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)
    return sh


def _ensure_headers(ws, expected_headers: List[str]):
    values = ws.get_all_values()
    if not values:
        ws.append_row(expected_headers, value_input_option="RAW")
        return
    headers = values[0]
    for h in expected_headers:
        if h not in headers:
            raise RuntimeError(f"Falta columna '{h}' en hoja '{ws.title}'. No modifiques headers.")


def build_index(ws, key_cols: List[str]) -> Dict[str, int]:
    values = ws.get_all_values()
    if not values:
        return {}
    headers = values[0]
    col_idx = {h: i for i, h in enumerate(headers)}
    for c in key_cols:
        if c not in col_idx:
            raise RuntimeError(f"Falta columna '{c}' en hoja '{ws.title}'")

    idx: Dict[str, int] = {}
    for r in range(2, len(values) + 1):
        row = values[r - 1]
        parts: List[str] = []
        for c in key_cols:
            i = col_idx[c]
            parts.append(row[i] if i < len(row) else "")
        k = "|".join(parts).strip()
        if k:
            idx[k] = r
    return idx


def row_to_values(row: Dict[str, Any], columns: List[str]) -> List[Any]:
    return [row.get(c, "") for c in columns]


def _col_index_map(ws) -> Dict[str, int]:
    values = ws.get_all_values()
    if not values:
        return {}
    headers = values[0]
    return {h: i + 1 for i, h in enumerate(headers)}  # 1-based


def _a1(col: int, row: int) -> str:
    letters = ""
    n = col
    while n > 0:
        n, rem = divmod(n - 1, 26)
        letters = chr(65 + rem) + letters
    return f"{letters}{row}"


def sheet_upsert(ws, index: Dict[str, int], key: str, row: Dict[str, Any], columns: List[str], key_cols: List[str]):
    _ensure_headers(ws, columns)
    col_map = _col_index_map(ws)

    # valida que key_cols existan
    for kc in key_cols:
        if kc not in col_map:
            raise RuntimeError(f"Falta columna clave '{kc}' en hoja '{ws.title}'")

    values = row_to_values(row, columns)

    if key in index:
        r = index[key]
        start = _a1(1, r)
        end = _a1(len(columns), r)
        ws.update(f"{start}:{end}", [values], value_input_option="RAW")
    else:
        ws.append_row(values, value_input_option="RAW")
        last_row = len(ws.get_all_values())
        index[key] = last_row


def _is_permanent_sheet_error(err: str) -> bool:
    low = err.lower()
    if "not found" in low and "worksheet" in low:
        return True
    if "invalid" in low and "credentials" in low:
        return True
    if "permission" in low or "insufficient" in low:
        return True
    return False


def _safe_str(v: Any) -> str:
    return str(v).strip() if v is not None else ""


def _parse_bool01(v: Any) -> int:
    s = str(v).strip().lower()
    if s in ("1", "true", "si", "sí", "on", "activo", "yes"):
        return 1
    return 0


def _parse_int_or_default(v: Any, default: int) -> int:
    try:
        return int(str(v).strip())
    except Exception:
        return default


def _utc_iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_all_records(ws) -> List[Dict[str, Any]]:
    # get_all_records devuelve dicts con headers como keys
    try:
        return ws.get_all_records()
    except Exception:
        # fallback más resistente
        values = ws.get_all_values()
        if not values or len(values) < 2:
            return []
        headers = values[0]
        out: List[Dict[str, Any]] = []
        for r in values[1:]:
            d = {}
            for i, h in enumerate(headers):
                d[h] = r[i] if i < len(r) else ""
            out.append(d)
        return out


def _find_row_index_by_column(ws, col_name: str, target: str) -> Optional[int]:
    """
    Retorna row_index (1-based) donde col_name == target, buscando sobre toda la hoja.
    """
    values = ws.get_all_values()
    if not values:
        return None
    headers = values[0]
    try:
        ci = headers.index(col_name)
    except ValueError:
        return None
    for idx in range(2, len(values) + 1):
        row = values[idx - 1]
        val = row[ci] if ci < len(row) else ""
        if str(val).strip() == str(target).strip():
            return idx
    return None


def _update_cells_by_headers(ws, row_index: int, updates: Dict[str, Any]) -> None:
    """
    Actualiza celdas de una fila usando headers; updates: {header: value}
    """
    values = ws.get_all_values()
    if not values:
        raise RuntimeError("Hoja vacía, no puedo actualizar.")
    headers = values[0]
    col_map = {h: i + 1 for i, h in enumerate(headers)}  # 1-based col
    for k, v in updates.items():
        if k not in col_map:
            raise RuntimeError(f"Falta columna '{k}' en hoja '{ws.title}'")
        ws.update_cell(row_index, col_map[k], v)


# =========================
# Sheets config cache loaders
# =========================
def load_tecnicos_cache(app: Application) -> None:
    if not app.bot_data.get("sheets_ready"):
        return
    ws = app.bot_data.get("ws_tecnicos")
    if not ws:
        return
    try:
        _ensure_headers(ws, TECNICOS_COLUMNS)
        rows = _read_all_records(ws)
        techs: List[Dict[str, Any]] = []
        for r in rows:
            nombre = _safe_str(r.get("nombre"))
            if not nombre:
                continue
            activo = _parse_bool01(r.get("activo"))
            if activo != 1:
                continue
            alias = _safe_str(r.get("alias"))
            orden = _parse_int_or_default(r.get("orden"), 9999)
            techs.append({"nombre": nombre, "alias": alias, "orden": orden})
        techs.sort(key=lambda x: (x.get("orden", 9999), x.get("nombre", "")))
        app.bot_data["tech_cache"] = techs
        app.bot_data["tech_cache_at"] = time.time()
        log.info(f"TECNICOS cache actualizado: {len(techs)} activos.")
    except Exception as e:
        log.warning(f"TECNICOS cache error: {e}")


def load_routing_cache(app: Application) -> None:
    if not app.bot_data.get("sheets_ready"):
        return
    ws = app.bot_data.get("ws_routing")
    if not ws:
        return
    try:
        _ensure_headers(ws, ROUTING_COLUMNS)
        rows = _read_all_records(ws)
        m: Dict[int, Dict[str, Any]] = {}
        for r in rows:
            origin = _safe_int(r.get("origin_chat_id"))
            if origin is None:
                continue
            activo = _parse_bool01(r.get("activo"))
            if activo != 1:
                # guardamos igual pero marcado, por si se usa en "ver rutas"
                m[int(origin)] = {
                    "origin_chat_id": int(origin),
                    "evidence_chat_id": _safe_str(r.get("evidence_chat_id")),
                    "summary_chat_id": _safe_str(r.get("summary_chat_id")),
                    "alias": _safe_str(r.get("alias")),
                    "activo": 0,
                    "updated_by": _safe_str(r.get("updated_by")),
                    "updated_at": _safe_str(r.get("updated_at")),
                }
                continue

            m[int(origin)] = {
                "origin_chat_id": int(origin),
                "evidence_chat_id": _safe_str(r.get("evidence_chat_id")),
                "summary_chat_id": _safe_str(r.get("summary_chat_id")),
                "alias": _safe_str(r.get("alias")),
                "activo": 1,
                "updated_by": _safe_str(r.get("updated_by")),
                "updated_at": _safe_str(r.get("updated_at")),
            }
        app.bot_data["routing_cache"] = m
        app.bot_data["routing_cache_at"] = time.time()
        log.info(f"ROUTING cache actualizado: {len(m)} rutas.")
    except Exception as e:
        log.warning(f"ROUTING cache error: {e}")


async def refresh_config_jobs(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Job único que refresca TECNICOS + ROUTING según TTL; evita llamadas excesivas.
    """
    app = context.application
    if not app.bot_data.get("sheets_ready"):
        return

    now_ts = time.time()
    tech_at = app.bot_data.get("tech_cache_at", 0)
    routing_at = app.bot_data.get("routing_cache_at", 0)

    if now_ts - tech_at >= TECH_CACHE_TTL_SEC:
        load_tecnicos_cache(app)
    if now_ts - routing_at >= ROUTING_CACHE_TTL_SEC:
        load_routing_cache(app)

# =========================
# Sheets pairing (persistencia en Sheets)
# =========================
def _gen_pair_code() -> str:
    # Corto, fácil de copiar
    raw = uuid.uuid4().hex.upper()
    return f"PAIR-{raw[:6]}"


def pairing_create(app: Application, origin_chat_id: int, purpose: str, created_by: str) -> str:
    """
    Crea fila en PAIRING y retorna code.
    purpose: EVIDENCE | SUMMARY
    """
    if not app.bot_data.get("sheets_ready"):
        raise RuntimeError("Sheets no disponible.")
    ws = app.bot_data.get("ws_pairing")
    if not ws:
        raise RuntimeError("Hoja PAIRING no disponible.")

    _ensure_headers(ws, PAIRING_COLUMNS)

    code = _gen_pair_code()
    # Asegurar unicidad simple (reintenta pocas veces)
    for _ in range(3):
        ri = _find_row_index_by_column(ws, "code", code)
        if ri is None:
            break
        code = _gen_pair_code()

    expires = (datetime.now(timezone.utc) + timedelta(minutes=PAIRING_TTL_MINUTES)).isoformat()
    created_at = _utc_iso_now()

    row = {
        "code": code,
        "origin_chat_id": str(origin_chat_id),
        "purpose": purpose,
        "expires_at": expires,
        "used": "0",
        "created_by": created_by,
        "created_at": created_at,
        "used_by": "",
        "used_at": "",
    }
    ws.append_row([row.get(c, "") for c in PAIRING_COLUMNS], value_input_option="RAW")
    return code


def pairing_consume_and_upsert_routing(
    app: Application,
    code: str,
    dest_chat_id: int,
    used_by: str,
    purpose_expected: str,
    dest_kind: str,
) -> Dict[str, Any]:
    """
    Consume PAIRING (marca used=1) y actualiza ROUTING.
    dest_kind: 'EVIDENCE' o 'SUMMARY' (destino actual)
    Retorna dict con info: origin_chat_id, purpose, alias
    """
    if not app.bot_data.get("sheets_ready"):
        raise RuntimeError("Sheets no disponible.")
    ws_p = app.bot_data.get("ws_pairing")
    ws_r = app.bot_data.get("ws_routing")
    if not ws_p or not ws_r:
        raise RuntimeError("Hojas de configuración no disponibles.")

    _ensure_headers(ws_p, PAIRING_COLUMNS)
    _ensure_headers(ws_r, ROUTING_COLUMNS)

    code = str(code).strip().upper()
    row_idx = _find_row_index_by_column(ws_p, "code", code)
    if row_idx is None:
        raise RuntimeError("Código no encontrado.")

    # Leer fila
    values = ws_p.get_all_values()
    headers = values[0]
    row = values[row_idx - 1] if row_idx - 1 < len(values) else []
    col = {h: i for i, h in enumerate(headers)}

    def get_cell(name: str) -> str:
        i = col.get(name)
        if i is None:
            return ""
        return row[i] if i < len(row) else ""

    used = _parse_bool01(get_cell("used"))
    if used == 1:
        raise RuntimeError("Este código ya fue usado.")

    purpose = _safe_str(get_cell("purpose")).upper()
    if purpose not in ("EVIDENCE", "SUMMARY"):
        raise RuntimeError("Código inválido (purpose).")
    if purpose_expected and purpose != purpose_expected:
        raise RuntimeError(f"Este código es para {purpose}, no para {purpose_expected}.")

    expires_at = _safe_str(get_cell("expires_at"))
    dt_exp = parse_iso(expires_at)
    if not dt_exp:
        raise RuntimeError("Código inválido (expires_at).")
    if datetime.now(timezone.utc) > dt_exp:
        raise RuntimeError("Este código está vencido. Genera uno nuevo en el grupo ORIGEN.")

    origin_chat_id = _safe_int(get_cell("origin_chat_id"))
    if origin_chat_id is None:
        raise RuntimeError("Código inválido (origin_chat_id).")

    # Marcar como usado
    used_at = _utc_iso_now()
    _update_cells_by_headers(ws_p, row_idx, {"used": "1", "used_by": used_by, "used_at": used_at})

    # Upsert ROUTING: buscar fila por origin_chat_id
    origin_str = str(origin_chat_id)
    r_idx = _find_row_index_by_column(ws_r, "origin_chat_id", origin_str)

    alias = ""
    try:
        # alias sugerido: si el origen ya está en cache
        rc = app.bot_data.get("routing_cache") or {}
        if rc.get(int(origin_chat_id)):
            alias = _safe_str(rc[int(origin_chat_id)].get("alias"))
    except Exception:
        pass

    if not alias:
        alias = f"ORIGEN {origin_chat_id}"

    upd_by = used_by
    upd_at = _utc_iso_now()

    if r_idx is None:
        # Crear fila nueva
        new_row = {
            "origin_chat_id": origin_str,
            "evidence_chat_id": str(dest_chat_id) if dest_kind == "EVIDENCE" else "",
            "summary_chat_id": str(dest_chat_id) if dest_kind == "SUMMARY" else "",
            "alias": alias,
            "activo": "1",
            "updated_by": upd_by,
            "updated_at": upd_at,
        }
        ws_r.append_row([new_row.get(c, "") for c in ROUTING_COLUMNS], value_input_option="RAW")
    else:
        # Actualizar fila existente: set dest, activar, updated_*
        updates = {
            "activo": "1",
            "updated_by": upd_by,
            "updated_at": upd_at,
        }
        if dest_kind == "EVIDENCE":
            updates["evidence_chat_id"] = str(dest_chat_id)
        else:
            updates["summary_chat_id"] = str(dest_chat_id)
        # Si alias está vacío en la hoja, setear
        # (leemos de la hoja para decidir)
        vals_r = ws_r.get_all_values()
        hdr_r = vals_r[0]
        try:
            ci_alias = hdr_r.index("alias")
        except ValueError:
            ci_alias = None
        current_alias = ""
        if ci_alias is not None and (r_idx - 1) < len(vals_r):
            rr = vals_r[r_idx - 1]
            current_alias = rr[ci_alias] if ci_alias < len(rr) else ""
        if not str(current_alias).strip():
            updates["alias"] = alias

        _update_cells_by_headers(ws_r, r_idx, updates)

    # refrescar cache routing inmediatamente
    load_routing_cache(app)

    return {"origin_chat_id": int(origin_chat_id), "purpose": purpose, "alias": alias}

# =========================
# Admin helper
# =========================
async def is_admin_of_chat(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int) -> bool:
    try:
        admins = await context.bot.get_chat_administrators(chat_id)
        return any(a.user and a.user.id == user_id for a in admins)
    except Exception:
        return False


def mention_user_html(user_id: int, label: str = "Técnico") -> str:
    return f'<a href="tg://user?id={user_id}">{label}</a>'

# =========================
# Keyboards
# =========================
def kb_technicians_dynamic(app: Application) -> InlineKeyboardMarkup:
    techs = app.bot_data.get("tech_cache") or []
    rows: List[List[InlineKeyboardButton]] = []

    # Si no hay cache, fallback
    if not techs:
        for name in TECHNICIANS_FALLBACK:
            rows.append([InlineKeyboardButton(name, callback_data=f"TECH|{name}")])
        return InlineKeyboardMarkup(rows)

    for t in techs:
        nombre = _safe_str(t.get("nombre"))
        alias = _safe_str(t.get("alias"))
        label = alias if alias else nombre
        rows.append([InlineKeyboardButton(label, callback_data=f"TECH|{nombre}")])

    return InlineKeyboardMarkup(rows)


def kb_services() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(s, callback_data=f"SERV|{s}")] for s in SERVICE_TYPES]
    return InlineKeyboardMarkup(rows)


def kb_install_mode() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("INST EXTERNA", callback_data="MODE|EXTERNA"),
            InlineKeyboardButton("INST INTERNA", callback_data="MODE|INTERNA"),
        ]]
    )


def get_mode_items(mode: str) -> List[Tuple[int, str, int]]:
    return EXTERNA_MENU if mode == "EXTERNA" else INTERNA_MENU


def step_status(case_id: int, step_no: int) -> str:
    with db() as conn:
        in_prog = conn.execute(
            "SELECT 1 FROM step_state WHERE case_id=? AND step_no=? AND submitted=0 ORDER BY attempt DESC LIMIT 1",
            (case_id, step_no),
        ).fetchone()
    if in_prog:
        return "IN_PROGRESS"

    last = get_latest_submitted_state(case_id, step_no)
    if not last:
        return "NOT_STARTED"
    if last["approved"] is None:
        return "IN_REVIEW"
    if int(last["approved"]) == 1:
        return "DONE"
    return "REJECTED"


def compute_next_required_step(case_id: int, mode: str) -> Tuple[int, str, int, str]:
    items = get_mode_items(mode)
    for num, label, step_no in items:
        st = step_status(case_id, step_no)
        if st != "DONE":
            return (num, label, step_no, st)
    last_num, last_label, last_step = items[-1]
    return (last_num, last_label, last_step, "DONE")


def kb_evidence_menu(case_id: int, mode: str) -> InlineKeyboardMarkup:
    items = get_mode_items(mode)
    req_num, req_label, req_step_no, _req_status = compute_next_required_step(case_id, mode)

    rows: List[List[InlineKeyboardButton]] = []
    rows.append([InlineKeyboardButton("↩️ VOLVER AL MENU ANTERIOR", callback_data="BACK|MODE")])

    for num, label, step_no in items:
        st = step_status(case_id, step_no)

        if st == "DONE":
            prefix = "🟢"
        elif st == "IN_REVIEW":
            prefix = "🟡"
        elif st == "REJECTED":
            prefix = "🔴"
        elif step_no == req_step_no:
            prefix = "➡️"
        else:
            prefix = "🔒"

        rows.append([InlineKeyboardButton(f"{prefix} {num}. {label}", callback_data=f"EVID|{mode}|{num}|{step_no}")])

    return InlineKeyboardMarkup(rows)


def kb_action_menu(case_id: int, step_no: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("SOLICITUD DE PERMISO", callback_data=f"ACT|{case_id}|{step_no}|PERMISO"),
            InlineKeyboardButton("CARGAR FOTO", callback_data=f"ACT|{case_id}|{step_no}|FOTO"),
        ]]
    )


def kb_auth_mode(case_id: int, step_no: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("Solo texto", callback_data=f"AUTH_MODE|{case_id}|{step_no}|TEXT"),
            InlineKeyboardButton("Multimedia", callback_data=f"AUTH_MODE|{case_id}|{step_no}|MEDIA"),
        ]]
    )


def kb_auth_media_controls(case_id: int, step_no: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("➕ CARGAR MAS", callback_data=f"AUTH_MORE|{case_id}|{step_no}"),
            InlineKeyboardButton("✅ EVIDENCIAS COMPLETAS", callback_data=f"AUTH_DONE|{case_id}|{step_no}"),
        ]]
    )


def kb_auth_review(case_id: int, step_no: int, attempt: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("✅ AUTORIZADO", callback_data=f"AUT_OK|{case_id}|{step_no}|{attempt}"),
            InlineKeyboardButton("❌ RECHAZO", callback_data=f"AUT_BAD|{case_id}|{step_no}|{attempt}"),
        ]]
    )


def kb_media_controls(case_id: int, step_no: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("➕ CARGAR MAS", callback_data=f"MEDIA_MORE|{case_id}|{step_no}"),
            InlineKeyboardButton("✅ EVIDENCIAS COMPLETAS", callback_data=f"MEDIA_DONE|{case_id}|{step_no}"),
        ]]
    )


def kb_review_step(case_id: int, step_no: int, attempt: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("✅ CONFORME", callback_data=f"REV_OK|{case_id}|{step_no}|{attempt}"),
            InlineKeyboardButton("❌ RECHAZO", callback_data=f"REV_BAD|{case_id}|{step_no}|{attempt}"),
        ]]
    )

# =========================
# /config menu (admin-only)
# =========================
def kb_config_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🔗 Vincular Evidencias", callback_data="CFG|PAIR|EVIDENCE")],
            [InlineKeyboardButton("🧾 Vincular Resumen", callback_data="CFG|PAIR|SUMMARY")],
            [InlineKeyboardButton("📌 Ver rutas de este grupo", callback_data="CFG|ROUTE|STATUS")],
            [InlineKeyboardButton("❌ Cerrar", callback_data="CFG|CLOSE")],
        ]
    )


def kb_back_to_config() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("↩️ Volver a /config", callback_data="CFG|HOME")],
            [InlineKeyboardButton("❌ Cerrar", callback_data="CFG|CLOSE")],
        ]
    )

# =========================
# Prompts
# =========================
def prompt_step3() -> str:
    return (
        "PASO 3 - INGRESA CÓDIGO DE ABONADO\n"
        "✅ Envía el código como texto (puede incluir letras, números o caracteres)."
    )


def prompt_step4() -> str:
    return (
        "PASO 4 - REPORTA TU UBICACIÓN\n"
        "📌 En grupos, Telegram no permite solicitar ubicación con botón.\n"
        "✅ Envía tu ubicación así:\n"
        "1) Pulsa el clip 📎\n"
        "2) Ubicación\n"
        "3) Enviar ubicación actual"
    )


def prompt_media_step(step_no: int) -> str:
    title, desc = STEP_MEDIA_DEFS.get(step_no, (f"PASO {step_no}", "Envía evidencias"))
    return (
        f"{title}\n"
        f"{desc}\n"
        f"📸 Carga entre 1 a {MAX_MEDIA_PER_STEP} fotos (solo se acepta fotos)."
    )


def prompt_auth_media_step(step_no: int) -> str:
    title = STEP_MEDIA_DEFS.get(step_no, (f"PASO {step_no}",))[0]
    return (
        f"Autorización multimedia para {title}\n"
        f"📎 Carga entre 1 a {MAX_MEDIA_PER_STEP} archivos.\n"
        f"✅ En este paso (PERMISO) se acepta FOTO o VIDEO."
    )


async def show_evidence_menu(chat_id: int, context: ContextTypes.DEFAULT_TYPE, case_row: sqlite3.Row):
    mode = (case_row["install_mode"] or "").strip()
    if mode not in ("EXTERNA", "INTERNA"):
        await context.bot.send_message(chat_id=chat_id, text="Selecciona el tipo de instalación:", reply_markup=kb_install_mode())
        return

    await context.bot.send_message(
        chat_id=chat_id,
        text=f"📌 Selecciona la evidencia a cargar ({mode}):",
        reply_markup=kb_evidence_menu(int(case_row["case_id"]), mode),
    )


def is_last_step(mode: str, step_no: int) -> bool:
    items = get_mode_items(mode)
    return step_no == items[-1][2]


def duration_minutes(created_at: str, finished_at: str) -> Optional[int]:
    a = parse_iso(created_at)
    b = parse_iso(finished_at)
    if not a or not b:
        return None
    seconds = int((b - a).total_seconds())
    if seconds < 0:
        return None
    return max(0, seconds // 60)

# =========================
# Commands
# =========================
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg:
        return
    await context.bot.send_message(
        chat_id=msg.chat_id,
        text=(
            "Comandos:\n"
            "• /inicio  → iniciar caso\n"
            "• /estado  → ver estado\n"
            "• /cancelar → cancelar caso\n"
            "• /id → ver chat_id del grupo\n"
            "• /aprobacion on|off → activar/desactivar validaciones (solo admins)\n"
            "• /config → menú de configuración (solo admins)\n"
        ),
    )


async def id_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg:
        return
    title = msg.chat.title if msg.chat else "-"
    await context.bot.send_message(chat_id=msg.chat_id, text=f"Chat ID: {msg.chat_id}\nTitle: {title}")


async def config_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if msg is None or msg.from_user is None:
        return
    if not await is_admin_of_chat(context, msg.chat_id, msg.from_user.id):
        await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Solo Administradores del grupo pueden usar /config.")
        return

    # Forzar refresh suave si la cache está vacía
    app = context.application
    if app.bot_data.get("sheets_ready"):
        if not app.bot_data.get("routing_cache"):
            load_routing_cache(app)

    await context.bot.send_message(
        chat_id=msg.chat_id,
        text="⚙️ CONFIGURACIÓN (Admins)\nSelecciona una opción:",
        reply_markup=kb_config_menu(),
    )


async def inicio_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if msg is None or msg.from_user is None:
        return

    chat_id = msg.chat_id
    user_id = msg.from_user.id
    username = msg.from_user.username or msg.from_user.full_name

    create_or_reset_case(chat_id, user_id, username)

    approval_required = get_approval_required(chat_id)
    extra = "✅ Aprobación: ON (requiere admin)" if approval_required else "⚠️ Aprobación: OFF (auto-aprobación)"

    # Asegurar cache técnicos si posible
    app = context.application
    if app.bot_data.get("sheets_ready") and not app.bot_data.get("tech_cache"):
        load_tecnicos_cache(app)

    # Si aún no hay técnicos activos (ni fallback), avisar
    tech_cache = app.bot_data.get("tech_cache") or []
    if not tech_cache and not TECHNICIANS_FALLBACK:
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                "⚠️ No hay técnicos activos configurados en la hoja TECNICOS.\n"
                "Admin: agrega técnicos en Google Sheets (TECNICOS) y vuelve a intentar."
            ),
        )
        return

    await context.bot.send_message(
        chat_id=chat_id,
        text=f"✅ Caso iniciado.\n{extra}\n\nPASO 1 - NOMBRE DEL TECNICO",
        reply_markup=kb_technicians_dynamic(app),
    )


async def cancelar_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if msg is None or msg.from_user is None:
        return

    case_row = get_open_case(msg.chat_id, msg.from_user.id)
    if not case_row:
        await context.bot.send_message(chat_id=msg.chat_id, text="No tienes un caso abierto.")
        return

    update_case(int(case_row["case_id"]), status="CANCELLED", phase="CANCELLED", finished_at=now_utc())
    await context.bot.send_message(chat_id=msg.chat_id, text="🧾 Caso cancelado. Puedes iniciar otro con /inicio.")


async def estado_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if msg is None or msg.from_user is None:
        return

    case_row = get_open_case(msg.chat_id, msg.from_user.id)
    if not case_row:
        await context.bot.send_message(chat_id=msg.chat_id, text="No tienes un caso abierto. Usa /inicio.")
        return

    approval_required = get_approval_required(msg.chat_id)
    approval_txt = "ON ✅" if approval_required else "OFF ⚠️ (auto)"

    await context.bot.send_message(
        chat_id=msg.chat_id,
        text=(
            f"📌 Caso abierto\n"
            f"• Aprobación: {approval_txt}\n"
            f"• step_index: {int(case_row['step_index'])}\n"
            f"• phase: {case_row['phase']}\n"
            f"• pending_step_no: {case_row['pending_step_no']}\n"
            f"• Modo: {case_row['install_mode'] or '(pendiente)'}\n"
            f"• Técnico: {case_row['technician_name'] or '(pendiente)'}\n"
            f"• Servicio: {case_row['service_type'] or '(pendiente)'}\n"
            f"• Abonado: {case_row['abonado_code'] or '(pendiente)'}\n"
        ),
    )


async def aprobacion_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if msg is None or msg.from_user is None:
        return

    # REGLA: Solo admins pueden cambiar ON/OFF
    if not await is_admin_of_chat(context, msg.chat_id, msg.from_user.id):
        await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Solo Administradores del grupo pueden usar /aprobacion on|off.")
        return

    args = context.args or []
    if not args:
        state = "ON ✅" if get_approval_required(msg.chat_id) else "OFF ⚠️ (auto)"
        await context.bot.send_message(chat_id=msg.chat_id, text=f"Estado de aprobación: {state}")
        return

    val = args[0].strip().lower()
    if val in ("on", "1", "true", "si", "sí", "activar"):
        set_approval_required(msg.chat_id, True)
        await context.bot.send_message(chat_id=msg.chat_id, text="✅ Aprobación ENCENDIDA. Se requiere validación de admins.")
    elif val in ("off", "0", "false", "no", "desactivar"):
        set_approval_required(msg.chat_id, False)
        await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Aprobación APAGADA. Los pasos se auto-aprobarán (APROBACION OFF).")
    else:
        await context.bot.send_message(chat_id=msg.chat_id, text="Uso: /aprobacion on  o  /aprobacion off")

# =========================
# Sheets writers (enqueue) - historial
# =========================
def enqueue_evidencia_row(case_row: sqlite3.Row, step_no: int, attempt: int, file_id: str, file_unique_id: str, tg_message_id: int, grupo_evidencias: Optional[int]):
    created_at = now_utc()
    dt = parse_iso(created_at)
    fecha = dt.astimezone(PERU_TZ).strftime("%Y-%m-%d") if dt else ""
    hora = dt.astimezone(PERU_TZ).strftime("%H:%M") if dt else ""
    row = {
        "case_id": str(case_row["case_id"]),
        "paso_numero": str(step_no),
        "attempt": str(attempt),
        "file_id": file_id,
        "file_unique_id": file_unique_id or "",
        "mensaje_telegram_id": str(tg_message_id),
        "fecha_carga": fecha,
        "hora_carga": hora,
        "grupo_evidencias": str(grupo_evidencias or ""),
    }
    dedupe_key = f"{case_row['case_id']}|{step_no}|{attempt}|{tg_message_id}"
    outbox_enqueue("EVIDENCIAS", "UPSERT", dedupe_key, row)


def enqueue_detalle_paso_row(case_id: int, sheet_step_no: int, attempt: int, estado_paso: str, reviewer_name: str, motivo: str, kind: str = "EVID"):
    """
    kind:
      - "EVID": evidencia normal
      - "PERM": permiso (autorización) asociado al paso real (sheet_step_no positivo)
    NOTA: en Sheets NO usamos step_no negativo. Para permisos, sheet_step_no debe ser el paso real (5..15),
          y el paso_nombre se registrará como "PERMISO - <NOMBRE>".
    """
    case_row = get_case(case_id)
    if not case_row:
        return

    reviewed_at = now_utc()
    dt = parse_iso(reviewed_at)
    fecha = dt.astimezone(PERU_TZ).strftime("%Y-%m-%d") if dt else ""
    hora = dt.astimezone(PERU_TZ).strftime("%H:%M") if dt else ""

    base_name = STEP_MEDIA_DEFS.get(sheet_step_no, (f"PASO {sheet_step_no}",))[0]
    if kind == "PERM":
        paso_nombre = f"PERMISO - {base_name}"
    else:
        paso_nombre = base_name

    db_step_no = -sheet_step_no if kind == "PERM" else sheet_step_no
    fotos = media_count(case_id, db_step_no, attempt)
    ids = ",".join([str(x) for x in media_message_ids(case_id, db_step_no, attempt)])

    row = {
        "case_id": str(case_id),
        "paso_numero": str(sheet_step_no),
        "paso_nombre": paso_nombre,
        "attempt": str(attempt),
        "estado_paso": estado_paso,
        "revisado_por": reviewer_name,
        "fecha_revision": fecha,
        "hora_revision": hora,
        "motivo_rechazo": motivo or "",
        "cantidad_fotos": str(fotos),
        "ids_mensajes": ids,
    }
    dedupe_key = f"{case_id}|{sheet_step_no}|{attempt}|{kind}"
    outbox_enqueue("DETALLE_PASOS", "UPSERT", dedupe_key, row)


def enqueue_caso_row(case_id: int):
    case_row = get_case(case_id)
    if not case_row:
        return

    created_at = case_row["created_at"] or ""
    finished_at = case_row["finished_at"] or ""
    dur = duration_minutes(created_at, finished_at) if finished_at else None
    dur_txt = str(dur) if dur is not None else ""

    lat = case_row["location_lat"]
    lon = case_row["location_lon"]
    link_maps = ""
    if lat is not None and lon is not None:
        link_maps = f"https://maps.google.com/?q={lat},{lon}"

    mode = (case_row["install_mode"] or "").strip()
    total_pasos = len(get_mode_items(mode)) if mode in ("EXTERNA", "INTERNA") else ""

    aprob = total_approved_steps_for_case(case_id)
    rech = total_rejects_for_case(case_id)
    total_evid = total_media_for_case(case_id)
    approval_required = get_approval_required(int(case_row["chat_id"]))

    row = {
        "case_id": str(case_id),
        "estado": case_row["status"],
        "chat_id_origen": str(case_row["chat_id"]),
        "fecha_inicio": fmt_date_pe(created_at) if created_at else "",
        "hora_inicio": fmt_time_pe(created_at) if created_at else "",
        "fecha_cierre": fmt_date_pe(finished_at) if finished_at else "",
        "hora_cierre": fmt_time_pe(finished_at) if finished_at else "",
        "duracion_min": dur_txt,
        "tecnico_nombre": case_row["technician_name"] or "",
        "tecnico_user_id": str(case_row["user_id"]),
        "tipo_servicio": case_row["service_type"] or "",
        "codigo_abonado": case_row["abonado_code"] or "",
        "modo_instalacion": mode or "",
        "latitud": str(lat) if lat is not None else "",
        "longitud": str(lon) if lon is not None else "",
        "link_maps": link_maps,
        "total_pasos": str(total_pasos) if total_pasos != "" else "",
        "pasos_aprobados": str(aprob),
        "pasos_rechazados": str(rech),
        "total_evidencias": str(total_evid),
        "requiere_aprobacion": "1" if approval_required else "0",
        "registrado_en": now_utc(),
        "version_bot": BOT_VERSION,
    }
    dedupe_key = str(case_id)
    outbox_enqueue("CASOS", "UPSERT", dedupe_key, row)

# =========================
# Auto-approval helpers (Aprobacion OFF)
# =========================
def auto_approve_db_step(case_id: int, db_step_no: int, attempt: int):
    """
    Marca submitted=1 y approved=1, con reviewed_by=0 (sistema) y reviewed_at=now.
    """
    with db() as conn:
        conn.execute(
            """
            UPDATE step_state
            SET submitted=1, approved=1, reviewed_by=?, reviewed_at=?
            WHERE case_id=? AND step_no=? AND attempt=?
            """,
            (0, now_utc(), case_id, db_step_no, attempt),
        )
        conn.commit()

# =========================
# Sheets worker (reintentos) - historial
# =========================
async def sheets_worker(context: ContextTypes.DEFAULT_TYPE):
    if "sheets_ready" not in context.application.bot_data:
        return
    if not context.application.bot_data.get("sheets_ready"):
        return

    ws_casos = context.application.bot_data["ws_casos"]
    ws_det = context.application.bot_data["ws_det"]
    ws_evid = context.application.bot_data["ws_evid"]
    idx_casos = context.application.bot_data["idx_casos"]
    idx_det = context.application.bot_data["idx_det"]
    idx_evid = context.application.bot_data["idx_evid"]

    batch = outbox_fetch_batch(limit=20)
    if not batch:
        return

    for item in batch:
        outbox_id = int(item["outbox_id"])
        sheet_name = item["sheet_name"]
        dedupe_key = item["dedupe_key"]
        attempts = int(item["attempts"]) + 1
        row_json = item["row_json"]

        try:
            row = json.loads(row_json)
            if sheet_name == "CASOS":
                sheet_upsert(ws_casos, idx_casos, dedupe_key, row, CASOS_COLUMNS, ["case_id"])
            elif sheet_name == "DETALLE_PASOS":
                sheet_upsert(ws_det, idx_det, dedupe_key, row, DETALLE_PASOS_COLUMNS, ["case_id", "paso_numero", "attempt"])
            elif sheet_name == "EVIDENCIAS":
                sheet_upsert(ws_evid, idx_evid, dedupe_key, row, EVIDENCIAS_COLUMNS, ["case_id", "paso_numero", "attempt", "mensaje_telegram_id"])
            else:
                raise RuntimeError(f"Hoja desconocida: {sheet_name}")

            outbox_mark_sent(outbox_id)

        except Exception as e:
            err = str(e)
            dead = _is_permanent_sheet_error(err) or attempts >= 8
            outbox_mark_failed(outbox_id, attempts, err, dead=dead)
            log.warning(f"Sheets worker error outbox_id={outbox_id} sheet={sheet_name} attempts={attempts}: {err}")
            await context.application.bot.loop.run_in_executor(None, time.sleep, 0.2)

# =========================
# Callbacks
# =========================
async def on_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q is None or q.message is None or q.from_user is None:
        return

    chat_id = q.message.chat_id
    user_id = q.from_user.id
    data = (q.data or "").strip()

    log.info(f"CALLBACK data={data} chat_id={chat_id} user_id={user_id}")

    # -------------------------
    # CONFIG MENU (Admins)
    # -------------------------
    if data.startswith("CFG|"):
        if not await is_admin_of_chat(context, chat_id, user_id):
            await safe_q_answer(q, "⚠️ Solo administradores.", show_alert=True)
            return

        parts = data.split("|")
        # CFG|HOME
        if data == "CFG|HOME":
            await safe_q_answer(q, "Config", show_alert=False)
            await safe_edit_message_text(q, "⚙️ CONFIGURACIÓN (Admins)\nSelecciona una opción:", reply_markup=kb_config_menu())
            return

        # CFG|CLOSE
        if data == "CFG|CLOSE":
            await safe_q_answer(q, "Cerrado", show_alert=False)
            await safe_edit_message_text(q, "✅ Configuración cerrada.")
            return

        # CFG|ROUTE|STATUS
        if len(parts) >= 3 and parts[1] == "ROUTE" and parts[2] == "STATUS":
            app = context.application
            if app.bot_data.get("sheets_ready") and not app.bot_data.get("routing_cache"):
                load_routing_cache(app)

            rc = app.bot_data.get("routing_cache") or {}
            # Si este chat es ORIGEN
            row = rc.get(int(chat_id))
            if row:
                alias = row.get("alias") or f"ORIGEN {chat_id}"
                ev = row.get("evidence_chat_id") or ""
                sm = row.get("summary_chat_id") or ""
                activo = "✅ Activo" if int(row.get("activo", 1)) == 1 else "⛔ Inactivo"
                txt = (
                    f"📌 RUTAS (ORIGEN)\n"
                    f"Alias: {alias}\n"
                    f"Origin chat_id: {chat_id}\n"
                    f"Evidencias chat_id: {ev or '(no vinculado)'}\n"
                    f"Resumen chat_id: {sm or '(no vinculado)'}\n"
                    f"Estado: {activo}\n"
                )
            else:
                # Opcional: indicar si es destino
                found_as = ""
                try:
                    for origin_id, r in rc.items():
                        if str(r.get("evidence_chat_id", "")).strip() == str(chat_id):
                            found_as = f"EVIDENCIAS de ORIGEN {origin_id} ({r.get('alias') or '-'})"
                            break
                        if str(r.get("summary_chat_id", "")).strip() == str(chat_id):
                            found_as = f"RESUMEN de ORIGEN {origin_id} ({r.get('alias') or '-'})"
                            break
                except Exception:
                    found_as = ""
                if found_as:
                    txt = f"ℹ️ Este grupo no es ORIGEN.\nEstá vinculado como: {found_as}"
                else:
                    txt = "ℹ️ Este grupo no es ORIGEN y no aparece como destino en ROUTING."
            await safe_q_answer(q, "Rutas", show_alert=False)
            await safe_edit_message_text(q, txt, reply_markup=kb_back_to_config())
            return

        # CFG|PAIR|EVIDENCE o SUMMARY
        if len(parts) >= 3 and parts[1] == "PAIR":
            purpose = parts[2].strip().upper()
            if purpose not in ("EVIDENCE", "SUMMARY"):
                await safe_q_answer(q, "Opción inválida.", show_alert=True)
                return

            app = context.application
            if not app.bot_data.get("sheets_ready"):
                await safe_q_answer(q, "Sheets no disponible.", show_alert=True)
                await safe_edit_message_text(q, "⚠️ Sheets no está disponible. Revisa credenciales / conexión.", reply_markup=kb_back_to_config())
                return

            # Heurística:
            # - Si el chat actual está como ORIGEN (en ROUTING) o si el admin quiere iniciar desde ORIGEN,
            #   generamos código aquí.
            # - Si el chat NO es ORIGEN, pedimos pegar el código (consumir).
            # Para hacerlo más intuitivo: si el admin está en un chat que NO es ORIGEN, asumimos DESTINO.

            # Asegurar cache routing
            if not app.bot_data.get("routing_cache"):
                load_routing_cache(app)

            rc = app.bot_data.get("routing_cache") or {}
            is_origin = int(chat_id) in rc  # ya registrado como ORIGEN
            # Si no está registrado, igual puede ser ORIGEN "nuevo"; pero tu operación dice cada técnico ya tiene ORIGEN.
            # Para no bloquear, damos opción basada en botón: aquí usamos una lógica simple:
            # - Si el grupo tiene título que parece SGI/ORIGEN o si no está en rc, le damos generar y consumir:
            #   Pero como quieres flujo pro, hacemos:
            #     1) Si NO es ORIGEN: consumir
            #     2) Si es ORIGEN: generar
            if is_origin:
                try:
                    code = pairing_create(app, origin_chat_id=int(chat_id), purpose=purpose, created_by=q.from_user.full_name)
                    expires_dt = datetime.now(PERU_TZ) + timedelta(minutes=PAIRING_TTL_MINUTES)
                    expires_txt = expires_dt.strftime("%H:%M")
                    label = "EVIDENCIAS" if purpose == "EVIDENCE" else "RESUMEN"
                    txt = (
                        f"🔐 Código de vinculación ({label})\n\n"
                        f"Código: {code}\n"
                        f"Vence aprox.: {expires_txt} (Perú)\n\n"
                        f"👉 Ve al grupo DESTINO ({label})\n"
                        f"y usa /config → {'🔗 Vincular Evidencias' if purpose=='EVIDENCE' else '🧾 Vincular Resumen'}\n"
                        f"para pegar el código."
                    )
                    await safe_q_answer(q, "Código generado", show_alert=False)
                    await safe_edit_message_text(q, txt, reply_markup=kb_back_to_config())
                except Exception as e:
                    await safe_q_answer(q, "Error", show_alert=True)
                    await safe_edit_message_text(q, f"⚠️ No pude generar el código: {e}", reply_markup=kb_back_to_config())
                return
            else:
                # Consumir (DESTINO): pedimos código por texto
                kind = "PAIR_CODE_EVID" if purpose == "EVIDENCE" else "PAIR_CODE_SUM"
                set_pending_input(chat_id=chat_id, user_id=user_id, kind=kind, case_id=0, step_no=0, attempt=0, reply_to_message_id=q.message.message_id)
                label = "EVIDENCIAS" if purpose == "EVIDENCE" else "RESUMEN"
                txt = (
                    f"🔗 Vincular {label}\n"
                    f"✅ Pega aquí el código (ej: PAIR-ABC123)\n\n"
                    f"Este grupo será el DESTINO de {label}."
                )
                await safe_q_answer(q, "Pega el código", show_alert=False)
                await safe_edit_message_text(q, txt, reply_markup=kb_back_to_config())
                return

        await safe_q_answer(q, "Opción no válida.", show_alert=True)
        return

    # -------------------------
    # FLUJO ORIGINAL (casos/evidencias)
    # -------------------------
    if data == "BACK|MODE":
        case_row = get_open_case(chat_id, user_id)
        if not case_row:
            await safe_q_answer(q, "No tienes un caso abierto.", show_alert=True)
            return
        update_case(int(case_row["case_id"]), phase="MENU_INST", pending_step_no=None)
        await safe_q_answer(q, "Volviendo…", show_alert=False)
        await context.bot.send_message(
            chat_id=chat_id,
            text="PASO 5 - TIPO DE INSTALACIÓN\nSelecciona una opción:",
            reply_markup=kb_install_mode(),
        )
        return

    if data.startswith("TECH|"):
        case_row = get_open_case(chat_id, user_id)
        if not case_row:
            await safe_q_answer(q, "No tienes un caso abierto. Usa /inicio.", show_alert=True)
            return
        if int(case_row["step_index"]) != 0:
            await safe_q_answer(q, "Este paso ya fue atendido.", show_alert=False)
            return

        name = data.split("|", 1)[1]
        update_case(int(case_row["case_id"]), technician_name=name, step_index=1, phase="WAIT_SERVICE")
        await safe_q_answer(q, "✅ Técnico registrado", show_alert=False)
        await context.bot.send_message(chat_id=chat_id, text="PASO 2 - TIPO DE SERVICIO", reply_markup=kb_services())
        return

    if data.startswith("SERV|"):
        case_row = get_open_case(chat_id, user_id)
        if not case_row:
            await safe_q_answer(q, "No tienes un caso abierto. Usa /inicio.", show_alert=True)
            return
        if int(case_row["step_index"]) != 1:
            await safe_q_answer(q, "Este paso ya fue atendido.", show_alert=False)
            return

        service = data.split("|", 1)[1]
        if service != "ALTA NUEVA":
            await safe_q_answer(q, "PROCESO AUN NO GENERADO", show_alert=True)
            return

        update_case(int(case_row["case_id"]), service_type=service, step_index=2, phase="WAIT_ABONADO")
        await safe_q_answer(q, "✅ Servicio registrado", show_alert=False)
        await context.bot.send_message(chat_id=chat_id, text=prompt_step3())
        return

    if data.startswith("MODE|"):
        case_row = get_open_case(chat_id, user_id)
        if not case_row:
            await safe_q_answer(q, "No tienes un caso abierto. Usa /inicio.", show_alert=True)
            return
        if int(case_row["step_index"]) != 4:
            await safe_q_answer(q, "Aún no llegas a este paso. Completa pasos previos.", show_alert=True)
            return

        mode = data.split("|", 1)[1]
        if mode not in ("EXTERNA", "INTERNA"):
            await safe_q_answer(q, "Modo inválido.", show_alert=True)
            return

        update_case(int(case_row["case_id"]), install_mode=mode, phase="MENU_EVID", pending_step_no=None)
        await safe_q_answer(q, f"✅ {mode}", show_alert=False)
        case_row2 = get_case(int(case_row["case_id"]))
        await show_evidence_menu(chat_id, context, case_row2)
        return

    if data.startswith("EVID|"):
        try:
            _, mode, num_s, step_no_s = data.split("|", 3)
            num = int(num_s)
            step_no = int(step_no_s)
        except Exception:
            await safe_q_answer(q, "Callback inválido", show_alert=True)
            return

        case_row = get_open_case(chat_id, user_id)
        if not case_row:
            await safe_q_answer(q, "No tienes un caso abierto. Usa /inicio.", show_alert=True)
            return
        if (case_row["install_mode"] or "") != mode:
            await safe_q_answer(q, "Modo no coincide con el caso.", show_alert=True)
            return

        case_id = int(case_row["case_id"])

        req_num, req_label, req_step_no, req_status = compute_next_required_step(case_id, mode)

        if req_status == "DONE":
            await safe_q_answer(q, "✅ Caso ya completado.", show_alert=True)
            return

        if step_no != req_step_no:
            latest = get_latest_submitted_state(case_id, step_no)
            if latest and latest["approved"] is not None and int(latest["approved"]) == 1:
                await safe_q_answer(q, "✅ Este paso ya está conforme.", show_alert=True)
                return

            st = step_status(case_id, step_no)
            if st == "IN_REVIEW":
                await safe_q_answer(q, "⏳ Este paso está en revisión de admin.", show_alert=True)
                return

            await safe_q_answer(q, f"⚠️ Debes completar primero: {req_num}. {req_label}", show_alert=True)
            return

        if req_status == "IN_REVIEW":
            await safe_q_answer(q, "⏳ Este paso está en revisión de admin. Espera validación.", show_alert=True)
            return

        update_case(case_id, phase="EVID_ACTION", pending_step_no=step_no)
        await safe_q_answer(q, "Continuar…", show_alert=False)
        label = STEP_MEDIA_DEFS.get(step_no, (f"PASO {step_no}", ""))[0]

        await context.bot.send_message(
            chat_id=chat_id,
            text=f"📌 {req_num}. {label}\nElige una opción:",
            reply_markup=kb_action_menu(case_id, step_no),
        )
        return

    if data.startswith("ACT|"):
        try:
            _, case_id_s, step_no_s, action = data.split("|", 3)
            case_id = int(case_id_s)
            step_no = int(step_no_s)
        except Exception:
            await safe_q_answer(q, "Callback inválido", show_alert=True)
            return

        case_row = get_case(case_id)
        if not case_row or case_row["status"] != "OPEN":
            await safe_q_answer(q, "Caso no válido o cerrado.", show_alert=True)
            return
        if int(case_row["user_id"]) != user_id:
            await safe_q_answer(q, "Solo el técnico del caso puede usar esto.", show_alert=True)
            return

        if action == "PERMISO":
            update_case(case_id, phase="AUTH_MODE", pending_step_no=step_no)
            await safe_q_answer(q, "Permiso…", show_alert=False)
            await context.bot.send_message(
                chat_id=chat_id,
                text="Autorización: elige el tipo",
                reply_markup=kb_auth_mode(case_id, step_no),
            )
            return

        if action == "FOTO":
            update_case(case_id, phase="STEP_MEDIA", pending_step_no=step_no)
            await safe_q_answer(q, "Cargar foto…", show_alert=False)
            await context.bot.send_message(chat_id=chat_id, text=prompt_media_step(step_no))
            return

        await safe_q_answer(q, "Acción inválida.", show_alert=True)
        return

    if data.startswith("AUTH_MODE|"):
        try:
            _, case_id_s, step_no_s, mode = data.split("|", 3)
            case_id = int(case_id_s)
            step_no = int(step_no_s)
        except Exception:
            await safe_q_answer(q, "Callback inválido", show_alert=True)
            return

        case_row = get_case(case_id)
        if not case_row or case_row["status"] != "OPEN":
            await safe_q_answer(q, "Caso no válido o cerrado.", show_alert=True)
            return
        if int(case_row["user_id"]) != user_id:
            await safe_q_answer(q, "Solo el técnico del caso puede elegir.", show_alert=True)
            return

        if mode == "TEXT":
            update_case(case_id, phase="AUTH_TEXT_WAIT", pending_step_no=step_no)
            await safe_q_answer(q, "Envía el texto…", show_alert=False)
            await context.bot.send_message(chat_id=chat_id, text="Envía el texto de la autorización (en un solo mensaje).")
            return

        if mode == "MEDIA":
            update_case(case_id, phase="AUTH_MEDIA", pending_step_no=step_no)
            await safe_q_answer(q, "Carga evidencias…", show_alert=False)
            await context.bot.send_message(
                chat_id=chat_id,
                text=prompt_auth_media_step(step_no),
                reply_markup=kb_auth_media_controls(case_id, step_no),
            )
            return

        await safe_q_answer(q, "Modo inválido", show_alert=True)
        return

    if data.startswith("AUTH_MORE|"):
        await safe_q_answer(q, "Puedes seguir cargando.", show_alert=False)
        return

    if data.startswith("AUTH_DONE|"):
        try:
            _, case_id_s, step_no_s = data.split("|", 2)
            case_id = int(case_id_s)
            step_no = int(step_no_s)
        except Exception:
            await safe_q_answer(q, "Callback inválido", show_alert=True)
            return

        case_row = get_case(case_id)
        if not case_row or case_row["status"] != "OPEN":
            await safe_q_answer(q, "Caso no válido o cerrado.", show_alert=True)
            return
        if int(case_row["user_id"]) != user_id:
            await safe_q_answer(q, "Solo el técnico del caso puede marcar evidencias completas.", show_alert=True)
            return

        auth_step_no = -step_no
        st = ensure_step_state(case_id, auth_step_no)
        attempt = int(st["attempt"])

        if int(st["submitted"]) == 1 and st["approved"] is None:
            await safe_q_answer(q, "Esta autorización ya fue enviada a revisión.", show_alert=True)
            return
        if st["approved"] is not None and int(st["approved"]) == 1:
            await safe_q_answer(q, "✅ Esta autorización ya está aprobada.", show_alert=True)
            return

        count = media_count(case_id, auth_step_no, attempt)
        if count <= 0:
            await safe_q_answer(q, "⚠️ Debes cargar al menos 1 archivo.", show_alert=True)
            return

        approval_required = get_approval_required(int(case_row["chat_id"]))

        if not approval_required:
            auto_approve_db_step(case_id, auth_step_no, attempt)
            enqueue_detalle_paso_row(case_id, step_no, attempt, "APROBADO", "APROBACION OFF", "", kind="PERM")

            await safe_q_answer(q, "✅ Autorización aprobada (OFF)", show_alert=False)
            await safe_edit_message_text(q, "✅ Autorización aprobada automáticamente (APROBACION OFF). Continuando a CARGAR FOTO…")

            update_case(case_id, phase="STEP_MEDIA", pending_step_no=step_no)
            await context.bot.send_message(chat_id=chat_id, text=prompt_media_step(step_no))
            return

        mark_submitted(case_id, auth_step_no, attempt)
        await safe_q_answer(q, "📨 Enviado a revisión", show_alert=False)

        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                f"🔐 **Revisión de AUTORIZACIÓN (multimedia)**\n"
                f"Para: {STEP_MEDIA_DEFS.get(step_no, (f'PASO {step_no}',))[0]}\n"
                f"Intento: {attempt}\n"
                f"Técnico: {case_row['technician_name'] or '-'}\n"
                f"Servicio: {case_row['service_type'] or '-'}\n"
                f"Abonado: {case_row['abonado_code'] or '-'}\n"
                f"Archivos: {count}\n\n"
                "Admins: validar con ✅/❌"
            ),
            parse_mode="Markdown",
            reply_markup=kb_auth_review(case_id, step_no, attempt),
        )
        return

    if data.startswith("AUT_OK|") or data.startswith("AUT_BAD|"):
        try:
            action, case_id_s, step_no_s, attempt_s = data.split("|", 3)
            case_id = int(case_id_s)
            step_no = int(step_no_s)
            attempt = int(attempt_s)
        except Exception:
            await safe_q_answer(q, "Callback inválido", show_alert=True)
            return

        if not await is_admin_of_chat(context, chat_id, user_id):
            await safe_q_answer(q, "Solo Administradores del grupo pueden validar", show_alert=True)
            return

        case_row = get_case(case_id)
        if not case_row or case_row["status"] != "OPEN":
            await safe_q_answer(q, "Caso no válido o cerrado.", show_alert=True)
            return

        auth_step_no = -step_no

        with db() as conn:
            row = conn.execute(
                "SELECT approved FROM step_state WHERE case_id=? AND step_no=? AND attempt=?",
                (case_id, auth_step_no, attempt),
            ).fetchone()
        if not row:
            await safe_q_answer(q, "No encontré la autorización para revisar.", show_alert=True)
            return
        if row["approved"] is not None:
            await safe_q_answer(q, "Esta autorización ya fue revisada.", show_alert=True)
            return

        tech_id = int(case_row["user_id"])
        admin_name = q.from_user.full_name

        if action == "AUT_OK":
            set_review(case_id, auth_step_no, attempt, approved=1, reviewer_id=user_id)
            enqueue_detalle_paso_row(case_id, step_no, attempt, "APROBADO", admin_name, "", kind="PERM")

            await safe_q_answer(q, "✅ Autorizado", show_alert=False)
            await safe_edit_message_text(q, "✅ Autorizado. Continuando a CARGAR FOTO…")

            await context.bot.send_message(
                chat_id=chat_id,
                text=f"🔐 {mention_user_html(tech_id)}: ✅ Autorización aprobada para <b>{STEP_MEDIA_DEFS.get(step_no,(str(step_no),))[0]}</b> (Intento {attempt}) por <b>{admin_name}</b>.",
                parse_mode="HTML",
            )

            update_case(case_id, phase="STEP_MEDIA", pending_step_no=step_no)
            await context.bot.send_message(chat_id=chat_id, text=prompt_media_step(step_no))
            return

        await safe_q_answer(q, "Escribe el motivo del rechazo.", show_alert=False)

        set_pending_input(
            chat_id=chat_id,
            user_id=user_id,
            kind="AUTH_REJECT_REASON",
            case_id=case_id,
            step_no=step_no,
            attempt=attempt,
            reply_to_message_id=q.message.message_id,
            tech_user_id=tech_id,
        )

        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                "❌ Rechazo de autorización.\n"
                "✍️ Admin: escribe el *motivo del rechazo* (un solo mensaje).\n\n"
                f"Paso: {STEP_MEDIA_DEFS.get(step_no, (f'PASO {step_no}',))[0]}\n"
                f"Intento: {attempt}\n"
                f"Técnico: {case_row['technician_name'] or '-'}"
            ),
            parse_mode="Markdown",
        )
        return

    if data.startswith("MEDIA_MORE|"):
        await safe_q_answer(q, "Puedes seguir cargando evidencias.", show_alert=False)
        return

    if data.startswith("MEDIA_DONE|"):
        try:
            _, case_id_s, step_no_s = data.split("|", 2)
            case_id = int(case_id_s)
            step_no = int(step_no_s)
        except Exception:
            await safe_q_answer(q, "Callback inválido", show_alert=True)
            return

        case_row = get_case(case_id)
        if not case_row or case_row["status"] != "OPEN":
            await safe_q_answer(q, "Caso no válido o cerrado.", show_alert=True)
            return
        if int(case_row["user_id"]) != user_id:
            await safe_q_answer(q, "Solo el técnico del caso puede marcar evidencias completas.", show_alert=True)
            return

        st = ensure_step_state(case_id, step_no)
        attempt = int(st["attempt"])

        if int(st["submitted"]) == 1 and st["approved"] is None:
            await safe_q_answer(q, "Este paso ya fue enviado a revisión.", show_alert=True)
            return
        if st["approved"] is not None and int(st["approved"]) == 1:
            await safe_q_answer(q, "✅ Este paso ya está aprobado.", show_alert=True)
            return

        count = media_count(case_id, step_no, attempt)
        if count <= 0:
            await safe_q_answer(q, "⚠️ Debes cargar al menos 1 foto.", show_alert=True)
            return

        title = STEP_MEDIA_DEFS.get(step_no, (f"PASO {step_no}",))[0]
        approval_required = get_approval_required(int(case_row["chat_id"]))
        mode = (case_row["install_mode"] or "EXTERNA").strip()
        tech_id = int(case_row["user_id"])

        if not approval_required:
            auto_approve_db_step(case_id, step_no, attempt)
            enqueue_detalle_paso_row(case_id, step_no, attempt, "APROBADO", "APROBACION OFF", "", kind="EVID")

            await safe_q_answer(q, "✅ Aprobado (OFF)", show_alert=False)
            await safe_edit_message_text(q, "✅ Aprobado automáticamente (APROBACION OFF).")

            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"✅ <b>PASO COMPLETADO</b>\n"
                    f"• Evidencia: <b>{title}</b>\n"
                    f"• Intento: <b>{attempt}</b>\n"
                    f"• Evidencias: <b>{count}</b>\n"
                    f"• Revisado por: <b>APROBACION OFF</b>\n"
                    f"• Técnico: {mention_user_html(tech_id)}"
                ),
                parse_mode="HTML",
            )

            if is_last_step(mode, step_no):
                finished_at = now_utc()
                update_case(case_id, status="CLOSED", phase="CLOSED", finished_at=finished_at, pending_step_no=None)

                enqueue_caso_row(case_id)

                # routing desde Sheets cache
                route = get_route_for_chat_cached(context.application, int(case_row["chat_id"]))
                dest_summary = route.get("summary")
                if dest_summary:
                    created_at = case_row["created_at"] or "-"
                    total_evid = total_media_for_case(case_id)
                    total_rej = total_rejects_for_case(case_id)
                    dur = duration_minutes(created_at, finished_at)
                    dur_txt = f"{dur} min" if dur is not None else "-"

                    await context.bot.send_message(
                        chat_id=dest_summary,
                        text=(
                            "🧾 **RESUMEN DE CASO (CERRADO)**\n"
                            f"Fecha: {fmt_date_pe(created_at)}\n"
                            f"Hora de Inicio: {fmt_time_pe(created_at)}\n"
                            f"Hora de Final: {fmt_time_pe(finished_at)}\n"
                            f"Duración: {dur_txt}\n"
                            f"Técnico: {case_row['technician_name'] or '-'}\n"
                            f"Tipo servicio: {case_row['service_type'] or '-'}\n"
                            f"Código abonado: {case_row['abonado_code'] or '-'}\n"
                            f"Evidencias totales: {total_evid}\n"
                            f"Rechazos: {total_rej}\n"
                            f"Grupo origen: {case_row['chat_id']}\n"
                        ),
                        parse_mode="Markdown",
                    )

                await context.bot.send_message(chat_id=chat_id, text="🧾 Caso COMPLETADO y cerrado.")
                return

            update_case(case_id, phase="MENU_EVID", pending_step_no=None)
            case_row2 = get_case(case_id)
            await context.bot.send_message(chat_id=chat_id, text="➡️ Continúa con el siguiente paso.")
            await show_evidence_menu(chat_id, context, case_row2)
            return

        mark_submitted(case_id, step_no, attempt)
        await safe_q_answer(q, "📨 Enviado a revisión", show_alert=False)

        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                f"🔎 **Revisión requerida - {title}**\n"
                f"Intento: {attempt}\n"
                f"Técnico: {case_row['technician_name'] or '-'}\n"
                f"Servicio: {case_row['service_type'] or '-'}\n"
                f"Abonado: {case_row['abonado_code'] or '-'}\n"
                f"Evidencias: {count}\n\n"
                "Admins: validar con ✅/❌"
            ),
            parse_mode="Markdown",
            reply_markup=kb_review_step(case_id, step_no, attempt),
        )
        return

    if data.startswith("REV_OK|") or data.startswith("REV_BAD|"):
        try:
            action, case_id_s, step_no_s, attempt_s = data.split("|", 3)
            case_id = int(case_id_s)
            step_no = int(step_no_s)
            attempt = int(attempt_s)
        except Exception:
            await safe_q_answer(q, "Callback inválido", show_alert=True)
            return

        if not await is_admin_of_chat(context, chat_id, user_id):
            await safe_q_answer(q, "Solo Administradores del grupo pueden validar", show_alert=True)
            return

        case_row = get_case(case_id)
        if not case_row or case_row["status"] != "OPEN":
            await safe_q_answer(q, "Caso no válido o cerrado.", show_alert=True)
            return

        with db() as conn:
            row = conn.execute(
                "SELECT approved FROM step_state WHERE case_id=? AND step_no=? AND attempt=?",
                (case_id, step_no, attempt),
            ).fetchone()
        if not row:
            await safe_q_answer(q, "No encontré el paso para revisar.", show_alert=True)
            return
        if row["approved"] is not None:
            await safe_q_answer(q, "Este paso ya fue revisado.", show_alert=True)
            return

        mode = (case_row["install_mode"] or "EXTERNA").strip()
        tech_id = int(case_row["user_id"])
        admin_name = q.from_user.full_name
        title = STEP_MEDIA_DEFS.get(step_no, (f"PASO {step_no}",))[0]

        if action == "REV_OK":
            set_review(case_id, step_no, attempt, approved=1, reviewer_id=user_id)
            enqueue_detalle_paso_row(case_id, step_no, attempt, "APROBADO", admin_name, "", kind="EVID")

            await safe_q_answer(q, "✅ Conforme", show_alert=False)
            await safe_edit_message_text(q, "✅ Conforme.")

            evids = media_count(case_id, step_no, attempt)
            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"✅ <b>PASO COMPLETADO</b>\n"
                    f"• Evidencia: <b>{title}</b>\n"
                    f"• Intento: <b>{attempt}</b>\n"
                    f"• Evidencias: <b>{evids}</b>\n"
                    f"• Aprobado por: <b>{admin_name}</b>\n"
                    f"• Técnico: {mention_user_html(tech_id)}"
                ),
                parse_mode="HTML",
            )

            if is_last_step(mode, step_no):
                finished_at = now_utc()
                update_case(case_id, status="CLOSED", phase="CLOSED", finished_at=finished_at, pending_step_no=None)

                enqueue_caso_row(case_id)

                route = get_route_for_chat_cached(context.application, int(case_row["chat_id"]))
                dest_summary = route.get("summary")
                if dest_summary:
                    created_at = case_row["created_at"] or "-"
                    total_evid = total_media_for_case(case_id)
                    total_rej = total_rejects_for_case(case_id)
                    dur = duration_minutes(created_at, finished_at)
                    dur_txt = f"{dur} min" if dur is not None else "-"

                    await context.bot.send_message(
                        chat_id=dest_summary,
                        text=(
                            "🧾 **RESUMEN DE CASO (CERRADO)**\n"
                            f"Fecha: {fmt_date_pe(created_at)}\n"
                            f"Hora de Inicio: {fmt_time_pe(created_at)}\n"
                            f"Hora de Final: {fmt_time_pe(finished_at)}\n"
                            f"Duración: {dur_txt}\n"
                            f"Técnico: {case_row['technician_name'] or '-'}\n"
                            f"Tipo servicio: {case_row['service_type'] or '-'}\n"
                            f"Código abonado: {case_row['abonado_code'] or '-'}\n"
                            f"Evidencias totales: {total_evid}\n"
                            f"Rechazos: {total_rej}\n"
                            f"Grupo origen: {case_row['chat_id']}\n"
                        ),
                        parse_mode="Markdown",
                    )

                await context.bot.send_message(chat_id=chat_id, text="🧾 Caso COMPLETADO y cerrado.")
                return

            update_case(case_id, phase="MENU_EVID", pending_step_no=None)
            case_row2 = get_case(case_id)
            await context.bot.send_message(chat_id=chat_id, text="➡️ Continúa con el siguiente paso.")
            await show_evidence_menu(chat_id, context, case_row2)
            return

        await safe_q_answer(q, "Escribe el motivo del rechazo.", show_alert=False)

        set_pending_input(
            chat_id=chat_id,
            user_id=user_id,
            kind="EVID_REJECT_REASON",
            case_id=case_id,
            step_no=step_no,
            attempt=attempt,
            reply_to_message_id=q.message.message_id,
            tech_user_id=tech_id,
        )

        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                f"❌ Rechazo de evidencia - {title}\n"
                f"Intento: {attempt}\n"
                "✍️ Admin: escribe el *motivo del rechazo* (un solo mensaje)."
            ),
            parse_mode="Markdown",
        )
        return

    await safe_q_answer(q, "Acción no válida.", show_alert=True)

# =========================
# Text handler (PASO 3 + AUTH_TEXT + motivos + Pairing codes)
# =========================
async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg: Message = update.effective_message
    if msg is None or msg.from_user is None:
        return

    # -------------------------
    # Pairing: pegar código (admin-only)
    # -------------------------
    pending_pair_e = pop_pending_input(msg.chat_id, msg.from_user.id, "PAIR_CODE_EVID")
    if pending_pair_e:
        if not await is_admin_of_chat(context, msg.chat_id, msg.from_user.id):
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Solo administradores pueden vincular.")
            return
        code = (msg.text or "").strip().upper()
        if not re.match(r"^PAIR-[A-Z0-9]{6}$", code):
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Código inválido. Ejemplo válido: PAIR-ABC123")
            set_pending_input(msg.chat_id, msg.from_user.id, "PAIR_CODE_EVID", 0, 0, 0)
            return
        try:
            info = pairing_consume_and_upsert_routing(
                context.application,
                code=code,
                dest_chat_id=msg.chat_id,
                used_by=msg.from_user.full_name,
                purpose_expected="EVIDENCE",
                dest_kind="EVIDENCE",
            )
            await context.bot.send_message(
                chat_id=msg.chat_id,
                text=(
                    "✅ Vinculación completada (EVIDENCIAS)\n"
                    f"ORIGEN chat_id: {info.get('origin_chat_id')}\n"
                    f"Alias: {info.get('alias')}\n"
                    f"DESTINO (este grupo): {msg.chat_id}"
                ),
                reply_markup=kb_back_to_config(),
            )
        except Exception as e:
            await context.bot.send_message(chat_id=msg.chat_id, text=f"⚠️ No pude vincular: {e}", reply_markup=kb_back_to_config())
        return

    pending_pair_s = pop_pending_input(msg.chat_id, msg.from_user.id, "PAIR_CODE_SUM")
    if pending_pair_s:
        if not await is_admin_of_chat(context, msg.chat_id, msg.from_user.id):
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Solo administradores pueden vincular.")
            return
        code = (msg.text or "").strip().upper()
        if not re.match(r"^PAIR-[A-Z0-9]{6}$", code):
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Código inválido. Ejemplo válido: PAIR-ABC123")
            set_pending_input(msg.chat_id, msg.from_user.id, "PAIR_CODE_SUM", 0, 0, 0)
            return
        try:
            info = pairing_consume_and_upsert_routing(
                context.application,
                code=code,
                dest_chat_id=msg.chat_id,
                used_by=msg.from_user.full_name,
                purpose_expected="SUMMARY",
                dest_kind="SUMMARY",
            )
            await context.bot.send_message(
                chat_id=msg.chat_id,
                text=(
                    "✅ Vinculación completada (RESUMEN)\n"
                    f"ORIGEN chat_id: {info.get('origin_chat_id')}\n"
                    f"Alias: {info.get('alias')}\n"
                    f"DESTINO (este grupo): {msg.chat_id}"
                ),
                reply_markup=kb_back_to_config(),
            )
        except Exception as e:
            await context.bot.send_message(chat_id=msg.chat_id, text=f"⚠️ No pude vincular: {e}", reply_markup=kb_back_to_config())
        return

    # -------------------------
    # Rechazos autorización/evidencia (admin)
    # -------------------------
    pending_auth = pop_pending_input(msg.chat_id, msg.from_user.id, "AUTH_REJECT_REASON")
    if pending_auth:
        reason = (msg.text or "").strip()
        if not reason:
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Envía un texto válido como motivo.")
            set_pending_input(
                chat_id=msg.chat_id,
                user_id=msg.from_user.id,
                kind="AUTH_REJECT_REASON",
                case_id=int(pending_auth["case_id"]),
                step_no=int(pending_auth["step_no"]),
                attempt=int(pending_auth["attempt"]),
                reply_to_message_id=int(pending_auth["reply_to_message_id"]) if pending_auth["reply_to_message_id"] is not None else None,
                tech_user_id=int(pending_auth["tech_user_id"]) if pending_auth["tech_user_id"] is not None else None,
            )
            return

        case_id = int(pending_auth["case_id"])
        step_no = int(pending_auth["step_no"])
        attempt = int(pending_auth["attempt"])
        auth_step_no = -step_no

        case_db = get_case(case_id)
        if not case_db or case_db["status"] != "OPEN":
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Caso no válido o ya cerrado.")
            return

        set_review(case_id, auth_step_no, attempt, approved=0, reviewer_id=msg.from_user.id)
        set_reject_reason(case_id, auth_step_no, attempt, reason, msg.from_user.id)
        enqueue_detalle_paso_row(case_id, step_no, attempt, "RECHAZADO", msg.from_user.full_name, reason, kind="PERM")

        tech_id = int(pending_auth["tech_user_id"]) if pending_auth["tech_user_id"] is not None else None
        reply_to = int(pending_auth["reply_to_message_id"]) if pending_auth["reply_to_message_id"] is not None else None
        title = STEP_MEDIA_DEFS.get(step_no, (f"PASO {step_no}",))[0]

        mention = mention_user_html(tech_id) if tech_id else "Técnico"

        await context.bot.send_message(
            chat_id=msg.chat_id,
            text=(
                f"❌ Autorización rechazada ({mention}).\n"
                f"📌 Paso: <b>{title}</b> (Intento {attempt})\n"
                f"📝 Motivo: {reason}\n\n"
                "El técnico puede volver a solicitar permiso o cargar foto."
            ),
            parse_mode="HTML",
            reply_to_message_id=reply_to if reply_to else None,
        )

        update_case(case_id, phase="EVID_ACTION", pending_step_no=step_no)
        await context.bot.send_message(chat_id=msg.chat_id, text="Elige una opción:", reply_markup=kb_action_menu(case_id, step_no))
        return

    pending_evid = pop_pending_input(msg.chat_id, msg.from_user.id, "EVID_REJECT_REASON")
    if pending_evid:
        reason = (msg.text or "").strip()
        if not reason:
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Envía un texto válido como motivo.")
            set_pending_input(
                chat_id=msg.chat_id,
                user_id=msg.from_user.id,
                kind="EVID_REJECT_REASON",
                case_id=int(pending_evid["case_id"]),
                step_no=int(pending_evid["step_no"]),
                attempt=int(pending_evid["attempt"]),
                reply_to_message_id=int(pending_evid["reply_to_message_id"]) if pending_evid["reply_to_message_id"] is not None else None,
                tech_user_id=int(pending_evid["tech_user_id"]) if pending_evid["tech_user_id"] is not None else None,
            )
            return

        case_id = int(pending_evid["case_id"])
        step_no = int(pending_evid["step_no"])
        attempt = int(pending_evid["attempt"])

        case_db = get_case(case_id)
        if not case_db or case_db["status"] != "OPEN":
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Caso no válido o ya cerrado.")
            return

        set_review(case_id, step_no, attempt, approved=0, reviewer_id=msg.from_user.id)
        set_reject_reason(case_id, step_no, attempt, reason, msg.from_user.id)

        tech_id = int(pending_evid["tech_user_id"]) if pending_evid["tech_user_id"] is not None else None
        reply_to = int(pending_evid["reply_to_message_id"]) if pending_evid["reply_to_message_id"] is not None else None
        title = STEP_MEDIA_DEFS.get(step_no, (f"PASO {step_no}",))[0]
        mention = mention_user_html(tech_id) if tech_id else "Técnico"

        enqueue_detalle_paso_row(case_id, step_no, attempt, "RECHAZADO", msg.from_user.full_name, reason, kind="EVID")

        await context.bot.send_message(
            chat_id=msg.chat_id,
            text=(
                f"❌ Evidencia rechazada - <b>{title}</b> ({mention}).\n"
                f"Intento: <b>{attempt}</b>\n"
                f"📝 Motivo: {reason}\n\n"
                "El técnico debe reenviar este paso."
            ),
            parse_mode="HTML",
            reply_to_message_id=reply_to if reply_to else None,
        )

        update_case(case_id, phase="EVID_ACTION", pending_step_no=step_no)
        await context.bot.send_message(chat_id=msg.chat_id, text="Elige una opción:", reply_markup=kb_action_menu(case_id, step_no))
        return

    # -------------------------
    # Flujo técnico normal
    # -------------------------
    case_row = get_open_case(msg.chat_id, msg.from_user.id)
    if not case_row:
        return

    if (case_row["phase"] or "") in ("STEP_MEDIA", "AUTH_MEDIA"):
        await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ En este paso no se acepta texto. Envía el archivo según corresponda.")
        return

    if (case_row["phase"] or "") == "AUTH_TEXT_WAIT":
        step_no = int(case_row["pending_step_no"] or 0)
        if step_no < 5 or step_no > 15:
            return

        text = (msg.text or "").strip()
        if not text:
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Envía el texto de autorización.")
            return

        case_id = int(case_row["case_id"])
        auth_step_no = -step_no
        st = ensure_step_state(case_id, auth_step_no)
        attempt = int(st["attempt"])

        save_auth_text(case_id, auth_step_no, attempt, text, msg.message_id)

        approval_required = get_approval_required(int(case_row["chat_id"]))

        if not approval_required:
            auto_approve_db_step(case_id, auth_step_no, attempt)
            enqueue_detalle_paso_row(case_id, step_no, attempt, "APROBADO", "APROBACION OFF", "", kind="PERM")

            update_case(case_id, phase="STEP_MEDIA", pending_step_no=step_no)

            await context.bot.send_message(
                chat_id=msg.chat_id,
                text=(
                    "✅ Autorización aprobada automáticamente (APROBACION OFF).\n"
                    "➡️ Continúa con la carga de foto del paso."
                ),
            )
            await context.bot.send_message(chat_id=msg.chat_id, text=prompt_media_step(step_no))
            return

        mark_submitted(case_id, auth_step_no, attempt)
        update_case(case_id, phase="AUTH_REVIEW", pending_step_no=step_no)

        await context.bot.send_message(
            chat_id=msg.chat_id,
            text=(
                f"🔐 **Revisión de AUTORIZACIÓN (solo texto)**\n"
                f"Para: {STEP_MEDIA_DEFS.get(step_no, (f'PASO {step_no}',))[0]}\n"
                f"Intento: {attempt}\n"
                f"Técnico: {case_row['technician_name'] or '-'}\n"
                f"Servicio: {case_row['service_type'] or '-'}\n"
                f"Abonado: {case_row['abonado_code'] or '-'}\n\n"
                f"Texto:\n{text}\n\n"
                "Admins: validar con ✅/❌"
            ),
            parse_mode="Markdown",
            reply_markup=kb_auth_review(case_id, step_no, attempt),
        )
        return

    if int(case_row["step_index"]) != 2:
        return

    text = (msg.text or "").strip()
    if not text:
        await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Envía el código de abonado como texto.")
        return

    update_case(int(case_row["case_id"]), abonado_code=text, step_index=3, phase="WAIT_LOCATION")
    await context.bot.send_message(chat_id=msg.chat_id, text=f"✅ Código de abonado registrado: {text}\n\n{prompt_step4()}")

# =========================
# PASO 4: Ubicación
# =========================
async def on_location(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg: Message = update.effective_message
    if msg is None or msg.from_user is None:
        return

    case_row = get_open_case(msg.chat_id, msg.from_user.id)
    if not case_row:
        return

    if int(case_row["step_index"]) != 3:
        return

    if not msg.location:
        await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Envía tu ubicación usando 📎 → Ubicación → ubicación actual.")
        return

    update_case(
        int(case_row["case_id"]),
        location_lat=msg.location.latitude,
        location_lon=msg.location.longitude,
        location_at=now_utc(),
        step_index=4,
        phase="MENU_INST",
        pending_step_no=None,
    )

    await context.bot.send_message(
        chat_id=msg.chat_id,
        text="PASO 5 - TIPO DE INSTALACIÓN\nSelecciona una opción:",
        reply_markup=kb_install_mode(),
    )

# =========================
# Carga de media
#   - Evidencias normales: SOLO FOTO
#   - Autorización (permiso) multimedia: FOTO o VIDEO
# =========================
async def on_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg: Message = update.effective_message
    if msg is None or msg.from_user is None:
        return

    case_row = get_open_case(msg.chat_id, msg.from_user.id)
    if not case_row:
        return

    case_id = int(case_row["case_id"])
    pending_step_no = int(case_row["pending_step_no"] or 0)
    phase = (case_row["phase"] or "")

    if phase not in ("AUTH_MEDIA", "STEP_MEDIA"):
        if int(case_row["step_index"]) >= 4:
            await context.bot.send_message(chat_id=msg.chat_id, text="ℹ️ Usa el menú para elegir el paso antes de enviar archivos.")
        return

    if pending_step_no < 5 or pending_step_no > 15:
        return

    if phase == "STEP_MEDIA":
        if not msg.photo:
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ En este paso solo se aceptan FOTOS.")
            return
        file_type = "photo"
    else:
        if msg.photo:
            file_type = "photo"
        elif msg.video:
            file_type = "video"
        else:
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ En PERMISO multimedia se aceptan FOTO o VIDEO.")
            return

    if phase == "AUTH_MEDIA":
        step_no_to_store = -pending_step_no
        controls_kb = kb_auth_media_controls(case_id, pending_step_no)
        label = "AUTORIZACIÓN"
    else:
        step_no_to_store = pending_step_no
        controls_kb = kb_media_controls(case_id, pending_step_no)
        label = "EVIDENCIA"

    st = ensure_step_state(case_id, step_no_to_store)
    attempt = int(st["attempt"])

    if int(st["submitted"]) == 1 and st["approved"] is None:
        await context.bot.send_message(chat_id=msg.chat_id, text="⏳ Ya está en revisión. Espera validación del administrador.")
        return
    if st["approved"] is not None and int(st["approved"]) == 1:
        await context.bot.send_message(chat_id=msg.chat_id, text="✅ Ya está aprobado. Continúa con el menú.")
        return

    current = media_count(case_id, step_no_to_store, attempt)
    if current >= MAX_MEDIA_PER_STEP:
        await context.bot.send_message(
            chat_id=msg.chat_id,
            text=f"⚠️ Ya llegaste al máximo de {MAX_MEDIA_PER_STEP}. Presiona ✅ EVIDENCIAS COMPLETAS.",
        )
        await context.bot.send_message(chat_id=msg.chat_id, text="Controles:", reply_markup=controls_kb)
        return

    if file_type == "photo":
        ph = msg.photo[-1]
        file_id = ph.file_id
        file_unique_id = ph.file_unique_id
    else:
        vd = msg.video
        file_id = vd.file_id if vd else ""
        file_unique_id = vd.file_unique_id if vd else ""

    meta = {
        "from_user_id": msg.from_user.id,
        "from_username": msg.from_user.username,
        "from_name": msg.from_user.full_name,
        "date": msg.date.isoformat() if msg.date else None,
        "caption": msg.caption,
        "phase": phase,
        "step_pending": pending_step_no,
        "attempt": attempt,
        "file_type": file_type,
    }

    add_media(
        case_id=case_id,
        step_no=step_no_to_store,
        attempt=attempt,
        file_type=file_type,
        file_id=file_id,
        file_unique_id=file_unique_id,
        tg_message_id=msg.message_id,
        meta=meta,
    )

    # Routing por Sheets cache
    route = get_route_for_chat_cached(context.application, msg.chat_id)
    caption = (
        f"📌 {label} ({STEP_MEDIA_DEFS.get(pending_step_no, (f'PASO {pending_step_no}',))[0]})\n"
        f"Técnico: {case_row['technician_name'] or '-'}\n"
        f"Servicio: {case_row['service_type'] or '-'}\n"
        f"Abonado: {case_row['abonado_code'] or '-'}\n"
        f"Intento: {attempt}\n"
        f"Tipo: {file_type.upper()}"
    )
    await maybe_copy_to_group(context, route.get("evidence"), file_type, file_id, caption)

    if phase != "AUTH_MEDIA" and file_type == "photo":
        enqueue_evidencia_row(case_row, pending_step_no, attempt, file_id, file_unique_id, msg.message_id, route.get("evidence"))

    new_count = current + 1
    remaining2 = MAX_MEDIA_PER_STEP - new_count

    if remaining2 <= 0:
        await context.bot.send_message(
            chat_id=msg.chat_id,
            text=f"✅ Guardado ({new_count}/{MAX_MEDIA_PER_STEP}). Ya alcanzaste el máximo. Presiona ✅ EVIDENCIAS COMPLETAS.",
            reply_markup=controls_kb,
        )
    else:
        await context.bot.send_message(
            chat_id=msg.chat_id,
            text=f"✅ Guardado ({new_count}/{MAX_MEDIA_PER_STEP}). Te quedan {remaining2}.",
            reply_markup=controls_kb,
        )

# =========================
# Error handler
# =========================
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    log.exception("Error no manejado:", exc_info=context.error)

# =========================
# Main
# =========================
def main():
    if not BOT_TOKEN:
        raise RuntimeError("Falta BOT_TOKEN. Configura la variable BOT_TOKEN con el token de BotFather.")

    init_db()

    request = HTTPXRequest(connect_timeout=10, read_timeout=25, write_timeout=25, pool_timeout=10)
    app = Application.builder().token(BOT_TOKEN).request(request).build()

    # Commands
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("id", id_cmd))
    app.add_handler(CommandHandler("inicio", inicio_cmd))
    app.add_handler(CommandHandler("cancelar", cancelar_cmd))
    app.add_handler(CommandHandler("estado", estado_cmd))
    app.add_handler(CommandHandler("aprobacion", aprobacion_cmd))
    app.add_handler(CommandHandler("config", config_cmd))

    # Callbacks
    app.add_handler(CallbackQueryHandler(on_callbacks))

    # Handlers
    app.add_handler(MessageHandler(filters.LOCATION, on_location))
    app.add_handler(MessageHandler(filters.PHOTO | filters.VIDEO, on_media))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    app.add_error_handler(error_handler)

    # Sheets init + indices + worker (reintentos) + config tabs
    try:
        sh = sheets_client()

        # Historial
        ws_casos = sh.worksheet("CASOS")
        ws_det = sh.worksheet("DETALLE_PASOS")
        ws_evid = sh.worksheet("EVIDENCIAS")

        _ensure_headers(ws_casos, CASOS_COLUMNS)
        _ensure_headers(ws_det, DETALLE_PASOS_COLUMNS)
        _ensure_headers(ws_evid, EVIDENCIAS_COLUMNS)

        idx_casos = build_index(ws_casos, ["case_id"])
        idx_det = build_index(ws_det, ["case_id", "paso_numero", "attempt"])
        idx_evid = build_index(ws_evid, ["case_id", "paso_numero", "attempt", "mensaje_telegram_id"])

        # Config pro
        ws_tecnicos = sh.worksheet(TECNICOS_TAB)
        ws_routing = sh.worksheet(ROUTING_TAB)
        ws_pairing = sh.worksheet(PAIRING_TAB)

        _ensure_headers(ws_tecnicos, TECNICOS_COLUMNS)
        _ensure_headers(ws_routing, ROUTING_COLUMNS)
        _ensure_headers(ws_pairing, PAIRING_COLUMNS)

        app.bot_data["sheets_ready"] = True
        app.bot_data["sh"] = sh

        # Historial refs
        app.bot_data["ws_casos"] = ws_casos
        app.bot_data["ws_det"] = ws_det
        app.bot_data["ws_evid"] = ws_evid
        app.bot_data["idx_casos"] = idx_casos
        app.bot_data["idx_det"] = idx_det
        app.bot_data["idx_evid"] = idx_evid

        # Config refs
        app.bot_data["ws_tecnicos"] = ws_tecnicos
        app.bot_data["ws_routing"] = ws_routing
        app.bot_data["ws_pairing"] = ws_pairing

        # Pre-cargar caches
        load_tecnicos_cache(app)
        load_routing_cache(app)

        # Jobs
        if app.job_queue:
            # Worker de outbox historial
            app.job_queue.run_repeating(sheets_worker, interval=20, first=5)
            # Refresh config (TECNICOS + ROUTING)
            app.job_queue.run_repeating(refresh_config_jobs, interval=30, first=10)

        log.info("Sheets: conectado. Worker iniciado. Config cache (TECNICOS/ROUTING) habilitado.")
    except Exception as e:
        app.bot_data["sheets_ready"] = False
        log.warning(f"Sheets deshabilitado: {e}")

    log.info("Bot corriendo...")
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()


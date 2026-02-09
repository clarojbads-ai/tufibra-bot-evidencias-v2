# ===== INICIO PARTE 1 =====
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
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple

import gspread
from google.oauth2.service_account import Credentials

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Message
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

TECHNICIANS = [
    "FLORO FERNANDEZ VASQUEZ",
    "ANTONY SALVADOR CORONADO",
    "DANIEL EDUARDO LUCENA PIÑANGO",
    "TELMER ROMUALDO RODRIGUEZ",
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
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON", "google_creds.json").strip()
BOT_VERSION = os.getenv("BOT_VERSION", "1.0.0").strip()

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

# =========================
# Logging
# =========================
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("tufibra_bot")


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
# Routing
# =========================
def get_route_for_chat(origin_chat_id: int) -> Dict[str, Optional[int]]:
    if not ROUTING_JSON:
        return {"evidence": None, "summary": None}
    try:
        mapping = json.loads(ROUTING_JSON)
        cfg = mapping.get(str(origin_chat_id)) or {}
        ev = cfg.get("evidence")
        sm = cfg.get("summary")
        return {"evidence": int(ev) if ev else None, "summary": int(sm) if sm else None}
    except Exception as e:
        log.warning(f"ROUTING_JSON inválido: {e}")
        return {"evidence": None, "summary": None}


async def maybe_copy_to_group(context: ContextTypes.DEFAULT_TYPE, dest_chat_id: Optional[int], file_id: str, caption: str):
    if not dest_chat_id:
        return
    try:
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


def add_media(case_id: int, step_no: int, attempt: int, file_id: str, file_unique_id: Optional[str], tg_message_id: int, meta: Dict[str, Any]):
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
                "photo",
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


def save_auth_text(case_id: int, step_no: int, attempt: int, text: str, tg_message_id: int):
    with db() as conn:
        conn.execute(
            """
            INSERT INTO auth_text(case_id, step_no, attempt, text, tg_message_id, created_at)
            VALUES(?,?,?,?,?,?)
            """,
            (case_id, step_no, attempt, text, tg_message_id, now_utc()),
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
# Outbox helpers (Google Sheets)
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
    if not GOOGLE_CREDS_JSON:
        raise RuntimeError("Falta GOOGLE_CREDS_JSON. Configura la variable GOOGLE_CREDS_JSON.")
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
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
        # nueva fila es última
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


# ===== FIN PARTE 1 =====
# ===== INICIO PARTE 2 =====

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
def kb_technicians() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(name, callback_data=f"TECH|{name}")] for name in TECHNICIANS]
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
    req_num, req_label, req_step_no, req_status = compute_next_required_step(case_id, mode)

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


def kb_auth_ask(case_id: int, step_no: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("SI", callback_data=f"AUTH_ASK|{case_id}|{step_no}|YES"),
            InlineKeyboardButton("NO", callback_data=f"AUTH_ASK|{case_id}|{step_no}|NO"),
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


def prompt_auth_question(step_no: int) -> str:
    return (
        f"Antes de iniciar {STEP_MEDIA_DEFS.get(step_no, (f'PASO {step_no}',))[0]}:\n\n"
        "¿Quieres solicitar alguna autorización?"
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
            "• /aprobacion on|off → activar/desactivar validaciones\n"
        ),
    )


async def id_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg:
        return
    title = msg.chat.title if msg.chat else "-"
    await context.bot.send_message(chat_id=msg.chat_id, text=f"Chat ID: {msg.chat_id}\nTitle: {title}")


async def inicio_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if msg is None or msg.from_user is None:
        return

    chat_id = msg.chat_id
    user_id = msg.from_user.id
    username = msg.from_user.username or msg.from_user.full_name

    create_or_reset_case(chat_id, user_id, username)

    approval_required = get_approval_required(chat_id)
    extra = "✅ Aprobación: ON (requiere admin)" if approval_required else "⚠️ Aprobación: OFF (modo libre)"

    await context.bot.send_message(
        chat_id=chat_id,
        text=f"✅ Caso iniciado.\n{extra}\n\nPASO 1 - NOMBRE DEL TECNICO",
        reply_markup=kb_technicians(),
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
    approval_txt = "ON ✅" if approval_required else "OFF ⚠️"

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

    args = context.args or []
    if not args:
        state = "ON ✅" if get_approval_required(msg.chat_id) else "OFF ⚠️"
        await context.bot.send_message(chat_id=msg.chat_id, text=f"Estado de aprobación: {state}")
        return

    val = args[0].strip().lower()
    if val in ("on", "1", "true", "si", "sí", "activar"):
        set_approval_required(msg.chat_id, True)
        await context.bot.send_message(chat_id=msg.chat_id, text="✅ Aprobación ENCENDIDA.")
    elif val in ("off", "0", "false", "no", "desactivar"):
        set_approval_required(msg.chat_id, False)
        await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Aprobación APAGADA.")
    else:
        await context.bot.send_message(chat_id=msg.chat_id, text="Uso: /aprobacion on  o  /aprobacion off")

# ===== FIN PARTE 2 =====
# ===== INICIO PARTE 3 =====

# =========================
# Sheets writers (enqueue)
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


def enqueue_detalle_paso_row(case_id: int, step_no: int, attempt: int, estado_paso: str, reviewer_name: str, motivo: str):
    case_row = get_case(case_id)
    if not case_row:
        return

    reviewed_at = now_utc()
    dt = parse_iso(reviewed_at)
    fecha = dt.astimezone(PERU_TZ).strftime("%Y-%m-%d") if dt else ""
    hora = dt.astimezone(PERU_TZ).strftime("%H:%M") if dt else ""
    paso_nombre = STEP_MEDIA_DEFS.get(step_no, (f"PASO {step_no}",))[0]
    fotos = media_count(case_id, step_no, attempt)
    ids = ",".join([str(x) for x in media_message_ids(case_id, step_no, attempt)])

    row = {
        "case_id": str(case_id),
        "paso_numero": str(step_no),
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
    dedupe_key = f"{case_id}|{step_no}|{attempt}"
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
# Sheets worker
# =========================
async def sheets_worker(context: ContextTypes.DEFAULT_TYPE):
    if "sheets_ready" not in context.application.bot_data:
        return
    if not context.application.bot_data.get("sheets_ready"):
        return

    sh = context.application.bot_data["sh"]
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

    if data == "BACK|MODE":
        case_row = get_open_case(chat_id, user_id)
        if not case_row:
            await q.answer("No tienes un caso abierto.", show_alert=True)
            return
        update_case(int(case_row["case_id"]), phase="MENU_INST", pending_step_no=None)
        await q.answer("Volviendo…", show_alert=False)
        await context.bot.send_message(
            chat_id=chat_id,
            text="PASO 5 - TIPO DE INSTALACIÓN\nSelecciona una opción:",
            reply_markup=kb_install_mode(),
        )
        return

    if data.startswith("TECH|"):
        case_row = get_open_case(chat_id, user_id)
        if not case_row:
            await q.answer("No tienes un caso abierto. Usa /inicio.", show_alert=True)
            return
        if int(case_row["step_index"]) != 0:
            await q.answer("Este paso ya fue atendido.", show_alert=False)
            return

        name = data.split("|", 1)[1]
        update_case(int(case_row["case_id"]), technician_name=name, step_index=1, phase="WAIT_SERVICE")
        await q.answer("✅ Técnico registrado")
        await context.bot.send_message(chat_id=chat_id, text="PASO 2 - TIPO DE SERVICIO", reply_markup=kb_services())
        return

    if data.startswith("SERV|"):
        case_row = get_open_case(chat_id, user_id)
        if not case_row:
            await q.answer("No tienes un caso abierto. Usa /inicio.", show_alert=True)
            return
        if int(case_row["step_index"]) != 1:
            await q.answer("Este paso ya fue atendido.", show_alert=False)
            return

        service = data.split("|", 1)[1]
        if service != "ALTA NUEVA":
            await q.answer("PROCESO AUN NO GENERADO", show_alert=True)
            return

        update_case(int(case_row["case_id"]), service_type=service, step_index=2, phase="WAIT_ABONADO")
        await q.answer("✅ Servicio registrado")
        await context.bot.send_message(chat_id=chat_id, text=prompt_step3())
        return

    if data.startswith("MODE|"):
        case_row = get_open_case(chat_id, user_id)
        if not case_row:
            await q.answer("No tienes un caso abierto. Usa /inicio.", show_alert=True)
            return
        if int(case_row["step_index"]) != 4:
            await q.answer("Aún no llegas a este paso. Completa pasos previos.", show_alert=True)
            return

        mode = data.split("|", 1)[1]
        if mode not in ("EXTERNA", "INTERNA"):
            await q.answer("Modo inválido.", show_alert=True)
            return

        update_case(int(case_row["case_id"]), install_mode=mode, phase="MENU_EVID", pending_step_no=None)
        await q.answer(f"✅ {mode}")
        case_row2 = get_case(int(case_row["case_id"]))
        await show_evidence_menu(chat_id, context, case_row2)
        return

    if data.startswith("EVID|"):
        try:
            _, mode, num_s, step_no_s = data.split("|", 3)
            num = int(num_s)
            step_no = int(step_no_s)
        except Exception:
            await q.answer("Callback inválido", show_alert=True)
            return

        case_row = get_open_case(chat_id, user_id)
        if not case_row:
            await q.answer("No tienes un caso abierto. Usa /inicio.", show_alert=True)
            return
        if (case_row["install_mode"] or "") != mode:
            await q.answer("Modo no coincide con el caso.", show_alert=True)
            return

        case_id = int(case_row["case_id"])

        req_num, req_label, req_step_no, req_status = compute_next_required_step(case_id, mode)

        if req_status == "DONE":
            await q.answer("✅ Caso ya completado.", show_alert=True)
            return

        if step_no != req_step_no:
            latest = get_latest_submitted_state(case_id, step_no)
            if latest and latest["approved"] is not None and int(latest["approved"]) == 1:
                await q.answer("✅ Este paso ya está conforme.", show_alert=True)
                return

            st = step_status(case_id, step_no)
            if st == "IN_REVIEW":
                await q.answer("⏳ Este paso está en revisión de admin.", show_alert=True)
                return

            await q.answer(f"⚠️ Debes completar primero: {req_num}. {req_label}", show_alert=True)
            return

        if req_status == "IN_REVIEW":
            await q.answer("⏳ Este paso está en revisión de admin. Espera validación.", show_alert=True)
            return

        update_case(case_id, phase="EVID_ACTION", pending_step_no=step_no)
        await q.answer("Continuar…")
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
            await q.answer("Callback inválido", show_alert=True)
            return

        case_row = get_case(case_id)
        if not case_row or case_row["status"] != "OPEN":
            await q.answer("Caso no válido o cerrado.", show_alert=True)
            return
        if int(case_row["user_id"]) != user_id:
            await q.answer("Solo el técnico del caso puede usar esto.", show_alert=True)
            return

        if action == "PERMISO":
            update_case(case_id, phase="AUTH_ASK", pending_step_no=step_no)
            await q.answer("Permiso…")
            await context.bot.send_message(
                chat_id=chat_id,
                text=prompt_auth_question(step_no),
                reply_markup=kb_auth_ask(case_id, step_no),
            )
            return

        if action == "FOTO":
            update_case(case_id, phase="STEP_MEDIA", pending_step_no=step_no)
            await q.answer("Cargar foto…")
            await context.bot.send_message(chat_id=chat_id, text=prompt_media_step(step_no))
            return

        await q.answer("Acción inválida.", show_alert=True)
        return

    if data.startswith("AUTH_ASK|"):
        try:
            _, case_id_s, step_no_s, yn = data.split("|", 3)
            case_id = int(case_id_s)
            step_no = int(step_no_s)
        except Exception:
            await q.answer("Callback inválido", show_alert=True)
            return

        case_row = get_case(case_id)
        if not case_row or case_row["status"] != "OPEN":
            await q.answer("Caso no válido o cerrado.", show_alert=True)
            return
        if int(case_row["user_id"]) != user_id:
            await q.answer("Solo el técnico del caso puede responder.", show_alert=True)
            return

        if yn == "NO":
            update_case(case_id, phase="EVID_ACTION", pending_step_no=step_no)
            await q.answer("OK")
            await context.bot.send_message(chat_id=chat_id, text="Elige una opción:", reply_markup=kb_action_menu(case_id, step_no))
            return

        update_case(case_id, phase="AUTH_MODE", pending_step_no=step_no)
        await q.answer("Elige tipo…")
        await context.bot.send_message(chat_id=chat_id, text="Autorización: elige el tipo", reply_markup=kb_auth_mode(case_id, step_no))
        return

    if data.startswith("AUTH_MODE|"):
        try:
            _, case_id_s, step_no_s, mode = data.split("|", 3)
            case_id = int(case_id_s)
            step_no = int(step_no_s)
        except Exception:
            await q.answer("Callback inválido", show_alert=True)
            return

        case_row = get_case(case_id)
        if not case_row or case_row["status"] != "OPEN":
            await q.answer("Caso no válido o cerrado.", show_alert=True)
            return
        if int(case_row["user_id"]) != user_id:
            await q.answer("Solo el técnico del caso puede elegir.", show_alert=True)
            return

        if mode == "TEXT":
            update_case(case_id, phase="AUTH_TEXT_WAIT", pending_step_no=step_no)
            await q.answer("Envía el texto…")
            await context.bot.send_message(chat_id=chat_id, text="Envía el texto de la autorización (en un solo mensaje).")
            return

        if mode == "MEDIA":
            update_case(case_id, phase="AUTH_MEDIA", pending_step_no=step_no)
            await q.answer("Carga evidencias…")
            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"Autorización multimedia para {STEP_MEDIA_DEFS.get(step_no, (f'PASO {step_no}',))[0]}\n"
                    f"📸 Carga entre 1 a {MAX_MEDIA_PER_STEP} fotos (solo se acepta fotos)."
                ),
                reply_markup=kb_auth_media_controls(case_id, step_no),
            )
            return

        await q.answer("Modo inválido", show_alert=True)
        return

    if data.startswith("AUTH_MORE|"):
        await q.answer("Puedes seguir cargando.", show_alert=False)
        return

    if data.startswith("AUTH_DONE|"):
        try:
            _, case_id_s, step_no_s = data.split("|", 2)
            case_id = int(case_id_s)
            step_no = int(step_no_s)
        except Exception:
            await q.answer("Callback inválido", show_alert=True)
            return

        case_row = get_case(case_id)
        if not case_row or case_row["status"] != "OPEN":
            await q.answer("Caso no válido o cerrado.", show_alert=True)
            return
        if int(case_row["user_id"]) != user_id:
            await q.answer("Solo el técnico del caso puede marcar evidencias completas.", show_alert=True)
            return

        auth_step_no = -step_no
        st = ensure_step_state(case_id, auth_step_no)
        attempt = int(st["attempt"])

        if int(st["submitted"]) == 1:
            await q.answer("Esta autorización ya fue enviada a revisión.", show_alert=True)
            return

        count = media_count(case_id, auth_step_no, attempt)
        if count <= 0:
            await q.answer("⚠️ Debes cargar al menos 1 foto.", show_alert=True)
            return

        mark_submitted(case_id, auth_step_no, attempt)
        await q.answer("📨 Enviado a revisión")

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
            await q.answer("Callback inválido", show_alert=True)
            return

        if not await is_admin_of_chat(context, chat_id, user_id):
            await q.answer("Solo Administradores del grupo pueden validar", show_alert=True)
            return

        case_row = get_case(case_id)
        if not case_row or case_row["status"] != "OPEN":
            await q.answer("Caso no válido o cerrado.", show_alert=True)
            return

        auth_step_no = -step_no

        with db() as conn:
            row = conn.execute(
                "SELECT approved FROM step_state WHERE case_id=? AND step_no=? AND attempt=?",
                (case_id, auth_step_no, attempt),
            ).fetchone()
        if not row:
            await q.answer("No encontré la autorización para revisar.", show_alert=True)
            return
        if row["approved"] is not None:
            await q.answer("Esta autorización ya fue revisada.", show_alert=True)
            return

        tech_id = int(case_row["user_id"])
        admin_name = q.from_user.full_name

        if action == "AUT_OK":
            set_review(case_id, auth_step_no, attempt, approved=1, reviewer_id=user_id)
            await q.answer("✅ Autorizado")
            await q.edit_message_text("✅ Autorizado. Continuando a CARGAR FOTO…")

            await context.bot.send_message(
                chat_id=chat_id,
                text=f"🔐 {mention_user_html(tech_id)}: ✅ Autorización aprobada para <b>{STEP_MEDIA_DEFS.get(step_no,(str(step_no),))[0]}</b> (Intento {attempt}) por <b>{admin_name}</b>.",
                parse_mode="HTML",
            )

            update_case(case_id, phase="STEP_MEDIA", pending_step_no=step_no)
            await context.bot.send_message(chat_id=chat_id, text=prompt_media_step(step_no))
            return

        await q.answer("Escribe el motivo del rechazo.", show_alert=False)

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
        await q.answer("Puedes seguir cargando evidencias.", show_alert=False)
        return

    if data.startswith("MEDIA_DONE|"):
        try:
            _, case_id_s, step_no_s = data.split("|", 2)
            case_id = int(case_id_s)
            step_no = int(step_no_s)
        except Exception:
            await q.answer("Callback inválido", show_alert=True)
            return

        case_row = get_case(case_id)
        if not case_row or case_row["status"] != "OPEN":
            await q.answer("Caso no válido o cerrado.", show_alert=True)
            return
        if int(case_row["user_id"]) != user_id:
            await q.answer("Solo el técnico del caso puede marcar evidencias completas.", show_alert=True)
            return

        st = ensure_step_state(case_id, step_no)
        attempt = int(st["attempt"])

        if int(st["submitted"]) == 1:
            await q.answer("Este paso ya fue enviado a revisión.", show_alert=True)
            return

        count = media_count(case_id, step_no, attempt)
        if count <= 0:
            await q.answer("⚠️ Debes cargar al menos 1 foto.", show_alert=True)
            return

        mark_submitted(case_id, step_no, attempt)
        await q.answer("📨 Enviado a revisión")

        title = STEP_MEDIA_DEFS.get(step_no, (f"PASO {step_no}",))[0]

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
            await q.answer("Callback inválido", show_alert=True)
            return

        if not await is_admin_of_chat(context, chat_id, user_id):
            await q.answer("Solo Administradores del grupo pueden validar", show_alert=True)
            return

        case_row = get_case(case_id)
        if not case_row or case_row["status"] != "OPEN":
            await q.answer("Caso no válido o cerrado.", show_alert=True)
            return

        with db() as conn:
            row = conn.execute(
                "SELECT approved FROM step_state WHERE case_id=? AND step_no=? AND attempt=?",
                (case_id, step_no, attempt),
            ).fetchone()
        if not row:
            await q.answer("No encontré el paso para revisar.", show_alert=True)
            return
        if row["approved"] is not None:
            await q.answer("Este paso ya fue revisado.", show_alert=True)
            return

        mode = (case_row["install_mode"] or "EXTERNA").strip()
        tech_id = int(case_row["user_id"])
        admin_name = q.from_user.full_name
        title = STEP_MEDIA_DEFS.get(step_no, (f"PASO {step_no}",))[0]

        if action == "REV_OK":
            set_review(case_id, step_no, attempt, approved=1, reviewer_id=user_id)
            enqueue_detalle_paso_row(case_id, step_no, attempt, "APROBADO", admin_name, "")

            await q.answer("✅ Conforme")
            await q.edit_message_text("✅ Conforme.")

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

                route = get_route_for_chat(int(case_row["chat_id"]))
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

        await q.answer("Escribe el motivo del rechazo.", show_alert=False)

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

    await q.answer("Acción no válida.", show_alert=True)


# =========================
# Text handler (PASO 3 + AUTH_TEXT + motivos)
# =========================
async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg: Message = update.effective_message
    if msg is None or msg.from_user is None:
        return

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

        enqueue_detalle_paso_row(case_id, step_no, attempt, "RECHAZADO", msg.from_user.full_name, reason)

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

    case_row = get_open_case(msg.chat_id, msg.from_user.id)
    if not case_row:
        return

    if (case_row["phase"] or "") in ("STEP_MEDIA", "AUTH_MEDIA"):
        await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ En este paso solo se aceptan fotos.")
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

        save_auth_text(case_id, step_no, attempt, text, msg.message_id)

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
                f"Texto:\n{text}"
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
# Carga de fotos (evidencias + autorización multimedia)
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
            await context.bot.send_message(chat_id=msg.chat_id, text="ℹ️ Usa el menú para elegir el paso antes de enviar fotos.")
        return

    if pending_step_no < 5 or pending_step_no > 15:
        return

    if not msg.photo:
        await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Solo se aceptan fotos en este paso.")
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

    if int(st["submitted"]) == 1:
        await context.bot.send_message(chat_id=msg.chat_id, text="⏳ Ya está en revisión. Espera validación del administrador.")
        return

    current = media_count(case_id, step_no_to_store, attempt)
    remaining = MAX_MEDIA_PER_STEP - current

    if current >= MAX_MEDIA_PER_STEP:
        await context.bot.send_message(
            chat_id=msg.chat_id,
            text=f"⚠️ Ya llegaste al máximo de {MAX_MEDIA_PER_STEP}. Presiona ✅ EVIDENCIAS COMPLETAS.",
        )
        await context.bot.send_message(chat_id=msg.chat_id, text="Controles:", reply_markup=controls_kb)
        return

    ph = msg.photo[-1]
    file_id = ph.file_id
    file_unique_id = ph.file_unique_id

    meta = {
        "from_user_id": msg.from_user.id,
        "from_username": msg.from_user.username,
        "from_name": msg.from_user.full_name,
        "date": msg.date.isoformat() if msg.date else None,
        "caption": msg.caption,
        "phase": phase,
        "step_pending": pending_step_no,
        "attempt": attempt,
    }

    add_media(
        case_id=case_id,
        step_no=step_no_to_store,
        attempt=attempt,
        file_id=file_id,
        file_unique_id=file_unique_id,
        tg_message_id=msg.message_id,
        meta=meta,
    )

    route = get_route_for_chat(msg.chat_id)
    caption = (
        f"📌 {label} ({STEP_MEDIA_DEFS.get(pending_step_no, (f'PASO {pending_step_no}',))[0]})\n"
        f"Técnico: {case_row['technician_name'] or '-'}\n"
        f"Servicio: {case_row['service_type'] or '-'}\n"
        f"Abonado: {case_row['abonado_code'] or '-'}\n"
        f"Intento: {attempt}"
    )
    await maybe_copy_to_group(context, route.get("evidence"), file_id, caption)

    if phase != "AUTH_MEDIA":
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

    app = Application.builder().token(BOT_TOKEN).build()

    # Commands
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("id", id_cmd))
    app.add_handler(CommandHandler("inicio", inicio_cmd))
    app.add_handler(CommandHandler("cancelar", cancelar_cmd))
    app.add_handler(CommandHandler("estado", estado_cmd))
    app.add_handler(CommandHandler("aprobacion", aprobacion_cmd))

    # Callbacks
    app.add_handler(CallbackQueryHandler(on_callbacks))

    # Handlers
    app.add_handler(MessageHandler(filters.LOCATION, on_location))
    app.add_handler(MessageHandler(filters.PHOTO | filters.VIDEO, on_media))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    app.add_error_handler(error_handler)

    # Sheets init + indices + worker
    try:
        sh = sheets_client()
        ws_casos = sh.worksheet("CASOS")
        ws_det = sh.worksheet("DETALLE_PASOS")
        ws_evid = sh.worksheet("EVIDENCIAS")

        _ensure_headers(ws_casos, CASOS_COLUMNS)
        _ensure_headers(ws_det, DETALLE_PASOS_COLUMNS)
        _ensure_headers(ws_evid, EVIDENCIAS_COLUMNS)

        idx_casos = build_index(ws_casos, ["case_id"])
        idx_det = build_index(ws_det, ["case_id", "paso_numero", "attempt"])
        idx_evid = build_index(ws_evid, ["case_id", "paso_numero", "attempt", "mensaje_telegram_id"])

        app.bot_data["sheets_ready"] = True
        app.bot_data["sh"] = sh
        app.bot_data["ws_casos"] = ws_casos
        app.bot_data["ws_det"] = ws_det
        app.bot_data["ws_evid"] = ws_evid
        app.bot_data["idx_casos"] = idx_casos
        app.bot_data["idx_det"] = idx_det
        app.bot_data["idx_evid"] = idx_evid

        if app.job_queue:
            app.job_queue.run_repeating(sheets_worker, interval=20, first=5)

        log.info("Sheets: conectado y worker iniciado.")
    except Exception as e:
        app.bot_data["sheets_ready"] = False
        log.warning(f"Sheets deshabilitado: {e}")

    log.info("Bot corriendo...")
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()

# ===== FIN PARTE 3 =====

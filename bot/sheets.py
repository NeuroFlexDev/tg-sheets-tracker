# bot/sheets.py
import logging
import time
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, date
from dateutil.parser import isoparse
from typing import Optional, List, Dict, Any
from domain import Task
import uuid
import config

log = logging.getLogger("sheets")

# см. инструкцию: либо добавь DRIVE scope, либо используй GSHEET_ID и open_by_key
# scopes
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def _sh():
    gc = _client()
    if getattr(config, "GSHEET_ID", None):
        return gc.open_by_key(config.GSHEET_ID)   # ← открываем по ключу, Drive не нужен
    return gc.open(config.GSHEET_NAME)            # (fallback по имени, требует Drive)


def _client():
    t0 = time.perf_counter()
    creds = Credentials.from_service_account_file(config.GOOGLE_SA_JSON_PATH, scopes=SCOPES)
    gc = gspread.authorize(creds)
    log.debug("client_ready", extra={"ms": round((time.perf_counter()-t0)*1000, 2)})
    return gc

def _sh():
    gc = _client()
    t0 = time.perf_counter()
    if getattr(config, "GSHEET_ID", None):
        sh = gc.open_by_key(config.GSHEET_ID)
        how = "open_by_key"
    else:
        sh = gc.open(config.GSHEET_NAME)
        how = "open_by_name"
    log.info("open_sheet", extra={"how": how, "ms": round((time.perf_counter()-t0)*1000, 2)})
    return sh

def _ws_tasks():
    sh = _sh()
    return sh.worksheet(config.GSHEET_WORKSHEET)

def _ws_threads():
    sh = _sh()
    try:
        return sh.worksheet(config.GSHEET_THREADS_SHEET)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(config.GSHEET_THREADS_SHEET, rows=100, cols=3)
        ws.update("A1:C1", [["Label","ThreadID","CreatedAt"]])
        log.info("threads_sheet_created")
        return ws

TASK_HEADERS = [
    "ID","Title","Description","Status","Assignee","Priority","Due",
    "Labels","CreatedAt","UpdatedAt","Source","TG_ThreadID","TG_MessageLink"
]

def ensure_task_headers():
    ws = _ws_tasks()
    first = ws.row_values(1)
    if first != TASK_HEADERS:
        ws.update("A1:N1", [TASK_HEADERS])
        log.info("tasks_headers_updated", extra={"headers": TASK_HEADERS})

def create_task(title: str, description: str = "", assignee: str = "", priority: str = "P2",
                due: Optional[str] = None, labels: Optional[list[str]] = None,
                source: str = "tg", tg_thread_id: Optional[int] = None,
                tg_message_link: str = "") -> Task:
    ensure_task_headers()
    t = Task(
        id=str(uuid.uuid4())[:8],
        title=title.strip(),
        description=description.strip() if description else "",
        assignee=assignee,
        priority=priority,
        due=isoparse(due).date() if due else None,
        labels=labels or [],
        source=source,
        tg_thread_id=tg_thread_id,
        tg_message_link=tg_message_link,
    )
    row = _to_row(t)
    ws = _ws_tasks()
    t0 = time.perf_counter()
    ws.append_row(row, value_input_option="USER_ENTERED")
    log.info("task_created", extra={"task_id": t.id, "title": t.title, "ms": round((time.perf_counter()-t0)*1000, 2)})
    return t

def _to_row(t: Task):
    return [
        t.id,
        t.title,
        t.description,
        t.status,
        t.assignee,
        t.priority,
        t.due.isoformat() if t.due else "",
        ",".join(t.labels),
        t.created_at.replace(microsecond=0).isoformat()+"Z",
        t.updated_at.replace(microsecond=0).isoformat()+"Z",
        t.source,
        str(t.tg_thread_id) if t.tg_thread_id else "",
        t.tg_message_link,
    ]

def list_tasks(status: Optional[str] = None, assignee: Optional[str] = None, label: Optional[str] = None):
    ws = _ws_tasks()
    t0 = time.perf_counter()
    data = ws.get_all_records()
    ms = round((time.perf_counter()-t0)*1000, 2)
    res = []
    for r in data:
        if status and r.get("Status") != status:
            continue
        if assignee and (assignee.lower() not in str(r.get("Assignee","")).lower()):
            continue
        if label:
            labels = [s.strip() for s in str(r.get("Labels","")).split(",") if s.strip()]
            if label not in labels:
                continue
        res.append(r)
    log.info("list_tasks", extra={
        "status": status, "assignee": assignee, "label": label,
        "total": len(data), "filtered": len(res), "ms": ms
    })
    return res

def _update_field(task_id: str, field: str, value: str):
    ws = _ws_tasks()
    col = TASK_HEADERS.index("ID") + 1
    cell = ws.find(task_id, in_column=col)
    if not cell:
        log.warning("task_not_found", extra={"task_id": task_id, "field": field})
        return False
    row = cell.row
    ws.update_cell(row, TASK_HEADERS.index(field)+1, value)
    ws.update_cell(row, TASK_HEADERS.index("UpdatedAt")+1, datetime.utcnow().replace(microsecond=0).isoformat()+"Z")
    log.info("task_updated", extra={"task_id": task_id, "field": field, "value": value})
    return True

def update_status(task_id: str, status: str):
    return _update_field(task_id, "Status", status)

def assign_task(task_id: str, assignee: str):
    return _update_field(task_id, "Assignee", assignee)

def set_due(task_id: str, due_iso: str):
    due = isoparse(due_iso).date()
    return _update_field(task_id, "Due", due.isoformat())

def update_fields(task_id: str, fields: Dict[str, Any]):
    ws = _ws_tasks()
    col = TASK_HEADERS.index("ID") + 1
    cell = ws.find(task_id, in_column=col)
    if not cell:
        log.warning("task_not_found", extra={"task_id": task_id, "fields": list(fields.keys())})
        return False
    row = cell.row
    for k,v in fields.items():
        if k in TASK_HEADERS:
            ws.update_cell(row, TASK_HEADERS.index(k)+1, v)
    ws.update_cell(row, TASK_HEADERS.index("UpdatedAt")+1, datetime.utcnow().replace(microsecond=0).isoformat()+"Z")
    log.info("task_bulk_updated", extra={"task_id": task_id, "fields": list(fields.keys())})
    return True

# ---------- threads mapping (label -> thread_id) ----------

def bind_thread(label: str, thread_id: int):
    ws = _ws_threads()
    data = ws.get_all_records()
    for idx, r in enumerate(data, start=2):
        if r.get("Label") == label:
            ws.update_cell(idx, 2, str(thread_id))
            log.info("thread_rebound", extra={"label": label, "thread_id": thread_id})
            return True
    ws.append_row([label, str(thread_id), datetime.utcnow().replace(microsecond=0).isoformat()+"Z"])
    log.info("thread_bound", extra={"label": label, "thread_id": thread_id})
    return True

def get_thread_id(label: str) -> Optional[int]:
    if not label:
        return None
    ws = _ws_threads()
    data = ws.get_all_records()
    for r in data:
        if r.get("Label") == label and str(r.get("ThreadID")).strip():
            try:
                tid = int(str(r.get("ThreadID")).strip())
                return tid
            except Exception:
                log.exception("thread_id_parse_fail", extra={"label": label, "record": r})
                return None
    return None

def list_threads() -> list[dict]:
    ws = _ws_threads()
    rows = ws.get_all_records()
    log.info("threads_list", extra={"count": len(rows)})
    return rows

# --- REMINDERS ---------------------------------------------------------------
# Лист: reminders
# Колонки: ID | TaskID | WhenISO | ChatID | ThreadID | CreatedAt | CreatedBy

def _ws_reminders():
    sh = _sh()
    try:
        return sh.worksheet("reminders")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet("reminders", rows=500, cols=7)
        ws.update("A1:G1", [["ID","TaskID","WhenISO","ChatID","ThreadID","CreatedAt","CreatedBy"]])
        log.info("reminders_sheet_created")
        return ws

def add_reminder(task_id: str, when_iso: str, chat_id: int, thread_id: int | None, created_by: str) -> str:
    ws = _ws_reminders()
    rid = str(uuid.uuid4())[:8]
    now = datetime.utcnow().replace(microsecond=0).isoformat()+"Z"
    ws.append_row([rid, task_id, when_iso, str(chat_id), str(thread_id or ""), now, created_by],
                  value_input_option="USER_ENTERED")
    log.info("reminder_added", extra={"rid": rid, "task_id": task_id, "when": when_iso, "thread": thread_id})
    return rid

def list_reminders(task_id: str | None = None):
    ws = _ws_reminders()
    rows = ws.get_all_records()
    if task_id:
        rows = [r for r in rows if r.get("TaskID") == task_id]
    return rows

def due_reminders(now_iso: str):
    # вернуть напоминания, время которых <= now_iso (ISO с зоной)
    ws = _ws_reminders()
    rows = ws.get_all_records()
    res = []
    for r in rows:
        w = r.get("WhenISO")
        if not w:
            continue
        try:
            if w <= now_iso:
                res.append(r)
        except Exception:
            log.exception("reminder_parse_fail", extra={"row": r})
    return res

def remove_reminder(reminder_id: str) -> bool:
    ws = _ws_reminders()
    cell = ws.find(reminder_id, in_column=1)  # col A = ID
    if not cell:
        return False
    ws.delete_rows(cell.row)
    log.info("reminder_removed", extra={"rid": reminder_id})
    return True

def remove_reminders_by_task(task_id: str) -> int:
    ws = _ws_reminders()
    data = ws.get_all_records()
    to_delete_rows = [i+2 for i, r in enumerate(data) if r.get("TaskID")==task_id]  # смещение из-за заголовка
    # удаляем с конца вверх, чтобы индексы не съезжали
    for row in sorted(to_delete_rows, reverse=True):
        ws.delete_rows(row)
    cnt = len(to_delete_rows)
    log.info("reminders_removed_by_task", extra={"task_id": task_id, "count": cnt})
    return cnt

# =================== ENSURE SHEETS ===================

def ensure_threads_sheet():
    """
    Проверяет наличие листа 'threads' и создает его при отсутствии.
    Колонки: Label | ThreadID | CreatedAt
    """
    sh = _sh()
    try:
        sh.worksheet(config.GSHEET_THREADS_SHEET)
        log.info("threads_sheet_exists")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(config.GSHEET_THREADS_SHEET, rows=100, cols=3)
        ws.update("A1:C1", [["Label", "ThreadID", "CreatedAt"]])
        log.info("threads_sheet_created")


def ensure_reminders_sheet():
    """
    Проверяет наличие листа 'reminders' и создает его при отсутствии.
    Колонки: ID | TaskID | WhenISO | ChatID | ThreadID | CreatedAt | CreatedBy
    """
    sh = _sh()
    try:
        sh.worksheet("reminders")
        log.info("reminders_sheet_exists")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet("reminders", rows=500, cols=7)
        ws.update("A1:G1", [["ID", "TaskID", "WhenISO", "ChatID", "ThreadID", "CreatedAt", "CreatedBy"]])
        log.info("reminders_sheet_created")

# bot/app.py
import asyncio
import logging
import time
import uuid
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.client.default import DefaultBotProperties
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime, date, time as dtime, timedelta
from dateutil import parser as dtparser
import pytz

import config
import sheets
from parser import parse_freeform
from logsetup import setup_logging
from functools import wraps

# --------- Настройка логирования ---------
setup_logging()
log = logging.getLogger("app")

# --------- Telegram bot ---------
bot = Bot(
    token=config.TELEGRAM_BOT_TOKEN,
    default=DefaultBotProperties(parse_mode="HTML")
)
dp = Dispatcher()
scheduler = AsyncIOScheduler(timezone=config.TZ)

# =================== Helpers ===================

def _now_tz():
    """Текущее время в заданной TZ"""
    return datetime.now(pytz.timezone(config.TZ))


def _parse_hhmm(s: str):
    """Проверка и парсинг времени HH:MM"""
    try:
        hh, mm = map(int, s.split(":"))
        return hh, mm
    except ValueError:
        raise ValueError(f"Неверный формат времени: {s}. Ожидается HH:MM")


async def _send_chat(text: str, thread_label: str | None = None, fallback_thread_id: int | None = None, **ctx):
    """Отправка сообщения в чат или в тред"""
    thread_id = None
    if thread_label:
        thread_id = sheets.get_thread_id(thread_label)
    if thread_id is None:
        thread_id = fallback_thread_id

    if thread_id is None:
        log.warning("no_thread_id_found", extra={"thread_label": thread_label})
        return await bot.send_message(chat_id=config.TELEGRAM_CHAT_ID, text=text)

    log.info("send_message", extra={"thread_label": thread_label, "thread_id": thread_id, **ctx})
    await bot.send_message(chat_id=config.TELEGRAM_CHAT_ID, text=text, message_thread_id=thread_id)


def _overdue_tasks():
    """Получить список просроченных задач"""
    tasks = sheets.list_tasks(status=None)
    res = []
    for r in tasks:
        if r.get("Status") == "done":
            continue
        due = r.get("Due")
        if due:
            try:
                d = date.fromisoformat(due)
                if d < _now_tz().date():
                    res.append(r)
            except Exception:
                log.exception("parse_due_failed", extra={"due": due, "task": r})
    return res


def with_timing(fn_name: str):
    """Замер времени выполнения хэндлеров"""
    def deco(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            t0 = time.perf_counter()
            try:
                return await func(*args)
            finally:
                dt = (time.perf_counter() - t0) * 1000
                log.info("handler_timing", extra={"handler": fn_name, "ms": round(dt, 2)})
        return wrapper
    return deco


def _to_aware(dt_naive, tzname: str):
    tz = pytz.timezone(tzname)
    return tz.localize(dt_naive) if dt_naive.tzinfo is None else dt_naive.astimezone(tz)


def parse_when(text: str, tzname: str) -> datetime:
    """
    Парсер времени:
    - Абсолютно: "2025-09-05 14:30"
    - Относительно: "+30m", "+2h", "+1d"
    - Русские краткие: "завтра 10:00", "сегодня 18:00"
    """
    text = text.strip().lower()

    # относительные
    if text.startswith("+"):
        num = int("".join(ch for ch in text if ch.isdigit()))
        if text.endswith("m"):
            dt = _now_tz() + timedelta(minutes=num)
        elif text.endswith("h") or text.endswith("ч"):
            dt = _now_tz() + timedelta(hours=num)
        elif text.endswith("d") or text.endswith("д"):
            dt = _now_tz() + timedelta(days=num)
        else:
            raise ValueError("Неподдержанный относительный формат (+30m|+2h|+1d)")
        return dt

    # завтра
    if text.startswith("завтра"):
        parts = text.split()
        base = _now_tz().date() + timedelta(days=1)
        default_time = dtime(hour=9)
        if len(parts) > 1:
            tm = dtparser.parse(parts[1]).time()
            return _to_aware(datetime.combine(base, tm), config.TZ)
        return _to_aware(datetime.combine(base, default_time), config.TZ)

    # сегодня
    if text.startswith("сегодня"):
        parts = text.split()
        base = _now_tz().date()
        default_time = dtime(hour=9)
        if len(parts) > 1:
            tm = dtparser.parse(parts[1]).time()
            return _to_aware(datetime.combine(base, tm), config.TZ)
        return _to_aware(datetime.combine(base, default_time), config.TZ)

    # абсолютные даты
    try:
        dt = dtparser.parse(text, dayfirst=False)
    except Exception:
        raise ValueError(f"Не могу распарсить дату: {text}")

    if dt.tzinfo is None:
        dt = _to_aware(dt, config.TZ)
    else:
        dt = dt.astimezone(pytz.timezone(config.TZ))
    return dt


# =================== Commands ===================

@dp.message(Command("start", "help"))
@with_timing("help")
async def cmd_help(m: Message):
    await m.reply(
        """
<b>Команды</b>:
/add <текст> — создать задачу
/list — список задач
/done <ID> — закрыть задачу
/assign <ID> @user — назначить
/due <ID> YYYY-MM-DD — срок
/who — сводка по людям
/bind #label — привязать тред к лейблу
/summary — сводка за сегодня
/remind <ID> <время> — напоминание (+30m, завтра 10:00)
        """.strip()
    )


@dp.message(Command("bind"))
@with_timing("bind")
async def cmd_bind(m: Message):
    if not m.message_thread_id:
        return await m.reply("Команда должна выполняться внутри треда (форум-топика).")
    text = m.text.partition(" ")[2].strip()
    if not text.startswith("#"):
        return await m.reply("Пример: /bind #frontend")

    label = text.lstrip("#").strip()
    sheets.bind_thread(label, m.message_thread_id)
    await m.reply(f"✅ Тред привязан к лейблу <b>#{label}</b>")


@dp.message(Command("add"))
@with_timing("add")
async def cmd_add(m: Message):
    text = m.text.partition(" ")[2].strip()
    if not text:
        return await m.reply("Пример: /add Починить деплой P1 @vadim до 2025-09-05 #deploy #frontend")

    title, priority, assignee, due, labels, project = parse_freeform(text)
    thread_id = m.message_thread_id or (sheets.get_thread_id(project) if project else None)

    t = sheets.create_task(
        title=title,
        description="",
        assignee=assignee,
        priority=priority,
        due=due,
        labels=labels,
        source="tg",
        tg_thread_id=thread_id,
        tg_message_link=f"https://t.me/c/{str(m.chat.id).replace('-100','')}/{m.message_id}"
    )

    await bot.send_message(
        chat_id=config.TELEGRAM_CHAT_ID,
        message_thread_id=thread_id,
        text=(f"✅ <b>Создано</b>: {t.title}\n"
              f"ID: <code>{t.id}</code> | {t.priority} | {assignee or '—'}"
              f"{f' | до {due}' if due else ''}\n"
              f"Labels: {', '.join(labels) if labels else '—'}")
    )


@dp.message(Command("list"))
@with_timing("list")
async def cmd_list(m: Message):
    arg = m.text.partition(" ")[2].strip()
    status = arg if arg in {"open", "in_progress", "done", "blocked"} else None
    assignee = arg if arg.startswith('@') else None
    label = arg.lstrip("#") if arg.startswith("#") else None

    tasks = sheets.list_tasks(status=status, assignee=assignee, label=label)
    if not tasks:
        return await m.reply("Пусто")

    lines = []
    for r in tasks[:50]:
        lines.append(
            f"<code>{r['ID']}</code> • <b>{r['Title']}</b> • {r['Priority']} • {r.get('Assignee','') or '—'}"
            + (f" • до {r['Due']}" if r.get('Due') else "")
            + (f" • #{(r.get('Labels') or '').split(',')[0]}" if r.get('Labels') else "")
        )
    await m.reply("\n".join(lines))


@dp.message(Command("done"))
@with_timing("done")
async def cmd_done(m: Message):
    tid = m.text.partition(" ")[2].strip()
    if not tid:
        return await m.reply("Укажи ID: /done 1a2b3c4d")
    ok = sheets.update_status(tid, "done")
    await m.reply("✅ Готово" if ok else "❌ Не найдено")


@dp.message(Command("who"))
@with_timing("who")
async def cmd_who(m: Message):
    tasks = sheets.list_tasks(status="open")
    buckets = {}
    for r in tasks:
        buckets.setdefault(r.get("Assignee") or "(не назначено)", 0)
        buckets[r.get("Assignee") or "(не назначено)"] += 1
    text = "\n".join(f"<b>{k}</b>: {v}" for k, v in buckets.items()) or "Пусто"
    await m.reply(text)


@dp.message(Command("summary"))
@with_timing("summary_manual")
async def cmd_summary(m: Message):
    await send_daily_summary()


# =================== Schedules ===================

@with_timing("send_daily_summary")
async def send_daily_summary():
    open_tasks = sheets.list_tasks(status=None)
    by_assignee = {}
    overdue = 0
    for r in open_tasks:
        if r.get("Status") == "done":
            continue
        k = r.get("Assignee") or "(не назначено)"
        by_assignee[k] = by_assignee.get(k, 0) + 1
        d = r.get("Due")
        if d:
            try:
                if date.fromisoformat(d) < _now_tz().date():
                    overdue += 1
            except Exception:
                log.exception("summary_due_parse_fail", extra={"due": d, "task": r})

    lines = ["<b>Ежедневная сводка задач</b>"]
    for k, v in sorted(by_assignee.items(), key=lambda x: (-x[1], x[0].lower())):
        lines.append(f"{k}: {v}")
    lines.append(f"Просрочено: <b>{overdue}</b>")

    await _send_chat("\n".join(lines), thread_label=config.SUMMARY_LABEL)


def schedule_jobs():
    hh, mm = _parse_hhmm(config.DAILY_SUMMARY_HHMM)
    scheduler.add_job(send_daily_summary, "cron", hour=hh, minute=mm, id="daily_summary", replace_existing=True)
    scheduler.start()
